from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool
import math
import os
import socket
import sqlite3
import struct
import threading
import time
from typing import Optional

from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "modbus_test.db")
HTML_PATH = os.path.join(APP_DIR, "index.html")

app = FastAPI(title="Modbus Polling & Write Test")

POLL_INTERVAL_SEC = 0.2
timeseries_next_run: dict[int, float] = {}
offline_data_until: dict[int, float] = {}
offline_data_reason: dict[int, str] = {}
disabled_device_ids: set[int] = set()
state_lock = threading.Lock()
device_retry_lock = threading.Lock()

settings_cache: dict = {"value": None, "ts": 0.0}
SETTINGS_CACHE_TTL_SEC = 2.0

connector_locks: dict[int, threading.Lock] = {}
connector_clients: dict[int, dict] = {}
connector_threads: dict[int, threading.Thread] = {}
supervisor_thread: Optional[threading.Thread] = None
poller_running = True

ALLOWED_TYPES = {
    "string",
    "bytes",
    "bits",
    "8int",
    "8uint",
    "16int",
    "16uint",
    "16float",
    "32int",
    "32uint",
    "32float",
    "64int",
    "64uint",
    "64float",
    "binary",
}

ALLOWED_FUNCTIONS = {1, 2, 3, 4, 5, 6, 16}
WRITABLE_FUNCTIONS = {5, 6, 16}


class ModbusOfflineError(RuntimeError):
    """Raised when a Modbus target fails to connect or times out."""


def ensure_column(conn: sqlite3.Connection, table: str, column: str, col_def: str) -> None:
    columns = [row["name"] for row in conn.execute(f"PRAGMA table_info({table})")]
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def}")


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                modbus_timeout_sec REAL NOT NULL DEFAULT 2.0,
                offline_retry_minutes INTEGER NOT NULL DEFAULT 0,
                offline_retry_seconds INTEGER NOT NULL DEFAULT 30,
                retry_disabled_device_enabled INTEGER NOT NULL DEFAULT 0,
                retry_disabled_device_minutes INTEGER NOT NULL DEFAULT 5
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS modbus_clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                modbus_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                unit_id INTEGER NOT NULL,
                disabled INTEGER NOT NULL DEFAULT 0,
                auto_disabled INTEGER NOT NULL DEFAULT 0,
                disabled_retry_until REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS timeseries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER NOT NULL,
                key_name TEXT NOT NULL,
                data_type TEXT NOT NULL,
                function_code INTEGER NOT NULL,
                object_count INTEGER NOT NULL,
                register_address INTEGER NOT NULL,
                modifier_on INTEGER NOT NULL DEFAULT 0,
                modifier_type TEXT,
                modifier_value REAL,
                pull_on INTEGER NOT NULL DEFAULT 0,
                report_period_ms INTEGER,
                last_value TEXT,
                last_status TEXT,
                last_updated INTEGER,
                bundle_group TEXT,
                bundle_role TEXT
            )
            """
        )
        ensure_column(conn, "settings", "retry_disabled_device_enabled", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "settings", "retry_disabled_device_minutes", "INTEGER NOT NULL DEFAULT 5")
        ensure_column(conn, "modbus_clients", "name", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "devices", "disabled", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "devices", "auto_disabled", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "devices", "disabled_retry_until", "REAL")
        ensure_column(conn, "timeseries", "bundle_group", "TEXT")
        ensure_column(conn, "timeseries", "bundle_role", "TEXT")
        disabled_rows = conn.execute(
            "SELECT id FROM devices WHERE COALESCE(disabled, 0) = 1"
        ).fetchall()
        conn.commit()
    with state_lock:
        disabled_device_ids.clear()
        disabled_device_ids.update(int(r["id"]) for r in disabled_rows)


def load_settings() -> dict:
    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT modbus_timeout_sec, offline_retry_minutes, offline_retry_seconds,
                   retry_disabled_device_enabled, retry_disabled_device_minutes
            FROM settings WHERE id = 1
            """
        ).fetchone()
    if not row:
        timeout_sec = 2.0
        retry_min = 0
        retry_sec = 30
        retry_disabled_enabled = False
        retry_disabled_minutes = 5
    else:
        timeout_sec = max(float(row["modbus_timeout_sec"] or 2.0), 0.2)
        retry_min = max(int(row["offline_retry_minutes"] or 0), 0)
        retry_sec = max(int(row["offline_retry_seconds"] or 0), 0)
        retry_disabled_enabled = bool(row["retry_disabled_device_enabled"])
        retry_disabled_minutes = max(int(row["retry_disabled_device_minutes"] or 5), 1)
    return {
        "modbus_timeout_sec": timeout_sec,
        "offline_retry_minutes": retry_min,
        "offline_retry_seconds": retry_sec,
        "offline_retry_total_sec": (retry_min * 60) + retry_sec,
        "retry_disabled_device_enabled": retry_disabled_enabled,
        "retry_disabled_device_minutes": retry_disabled_minutes,
    }


def get_settings_cached() -> dict:
    now = time.time()
    cached = settings_cache.get("value")
    if cached and (now - float(settings_cache.get("ts") or 0.0) < SETTINGS_CACHE_TTL_SEC):
        return cached
    value = load_settings()
    settings_cache["value"] = value
    settings_cache["ts"] = now
    return value


def save_settings_db(
    modbus_timeout_sec: float,
    offline_retry_minutes: int,
    offline_retry_seconds: int,
    retry_disabled_device_enabled: bool = False,
    retry_disabled_device_minutes: int = 5,
) -> None:
    old_settings = load_settings()
    enabled_int = 1 if retry_disabled_device_enabled else 0
    retry_disabled_minutes = max(int(retry_disabled_device_minutes), 1)
    with device_retry_lock:
        with db_connect() as conn:
            conn.execute(
                """
                INSERT INTO settings (
                    id, modbus_timeout_sec, offline_retry_minutes, offline_retry_seconds,
                    retry_disabled_device_enabled, retry_disabled_device_minutes
                )
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    modbus_timeout_sec = excluded.modbus_timeout_sec,
                    offline_retry_minutes = excluded.offline_retry_minutes,
                    offline_retry_seconds = excluded.offline_retry_seconds,
                    retry_disabled_device_enabled = excluded.retry_disabled_device_enabled,
                    retry_disabled_device_minutes = excluded.retry_disabled_device_minutes
                """,
                (
                    modbus_timeout_sec,
                    offline_retry_minutes,
                    offline_retry_seconds,
                    enabled_int,
                    retry_disabled_minutes,
                ),
            )
            if not retry_disabled_device_enabled:
                conn.execute(
                    """
                    UPDATE devices
                    SET disabled_retry_until = NULL
                    WHERE COALESCE(auto_disabled, 0) = 1
                    """
                )
            elif (
                (not old_settings["retry_disabled_device_enabled"])
                or (retry_disabled_minutes != old_settings["retry_disabled_device_minutes"])
            ):
                new_until = time.time() + (retry_disabled_minutes * 60)
                conn.execute(
                    """
                    UPDATE devices
                    SET disabled_retry_until = ?
                    WHERE COALESCE(disabled, 0) = 1
                      AND COALESCE(auto_disabled, 0) = 1
                    """,
                    (new_until,),
                )
            conn.commit()
    settings_cache["value"] = None
    settings_cache["ts"] = 0.0
    close_all_connector_clients()


def get_connector_lock(modbus_id: int) -> threading.Lock:
    with state_lock:
        lock = connector_locks.get(modbus_id)
        if lock is None:
            lock = threading.Lock()
            connector_locks[modbus_id] = lock
        return lock


def close_connector_client(modbus_id: int) -> None:
    entry = connector_clients.pop(modbus_id, None)
    if entry and entry.get("client"):
        try:
            entry["client"].close()
        except Exception:
            pass


def close_all_connector_clients() -> None:
    for modbus_id in list(connector_clients.keys()):
        lock = get_connector_lock(modbus_id)
        with lock:
            close_connector_client(modbus_id)


def get_connected_client(modbus_id: int, ip: str, port: int, timeout_sec: float) -> ModbusTcpClient:
    entry = connector_clients.get(modbus_id)
    if entry:
        same_target = (
            entry["ip"] == ip
            and entry["port"] == port
            and abs(float(entry["timeout"]) - float(timeout_sec)) < 1e-6
        )
        client = entry["client"]
        if same_target and client.is_socket_open():
            return client
        close_connector_client(modbus_id)

    client = ModbusTcpClient(host=ip, port=port, timeout=timeout_sec, retries=0)
    if not client.connect():
        try:
            client.close()
        except Exception:
            pass
        raise ModbusOfflineError(f"Offline ({ip}:{port})")
    connector_clients[modbus_id] = {
        "ip": ip,
        "port": port,
        "timeout": timeout_sec,
        "client": client,
    }
    return client


def reset_timeseries_polling_state(ts_ids: list[int]) -> None:
    with state_lock:
        for ts_id in ts_ids:
            timeseries_next_run.pop(ts_id, None)
            offline_data_until.pop(ts_id, None)
            offline_data_reason.pop(ts_id, None)


def mark_offline_data(ts_id: int, reason: str, retry_total_sec: int) -> None:
    with state_lock:
        if retry_total_sec > 0:
            offline_data_until[ts_id] = time.time() + retry_total_sec
        else:
            offline_data_until.pop(ts_id, None)
        offline_data_reason[ts_id] = reason


def clear_offline_data(ts_id: int) -> None:
    with state_lock:
        offline_data_until.pop(ts_id, None)
        offline_data_reason.pop(ts_id, None)


def get_offline_info(ts_id: int) -> tuple[bool, int]:
    now = time.time()
    with state_lock:
        until = offline_data_until.get(ts_id)
    if not until or now >= until:
        return False, 0
    return True, max(1, int(math.ceil(until - now)))


def set_device_disabled_state(device_id: int, disabled: bool) -> None:
    with state_lock:
        if disabled:
            disabled_device_ids.add(device_id)
        else:
            disabled_device_ids.discard(device_id)


def is_device_disabled(device_id: int) -> bool:
    with state_lock:
        return device_id in disabled_device_ids


def compute_device_retry_remaining(row: sqlite3.Row, settings: dict) -> tuple[int, int]:
    """Return remaining auto-retry delay in (minutes, seconds) for a disabled device."""
    if not bool(row["disabled"]) or not bool(row["auto_disabled"]):
        return 0, 0
    if not settings.get("retry_disabled_device_enabled"):
        return 0, 0
    until = row["disabled_retry_until"]
    if until is None:
        return 0, 0
    remaining_sec = float(until) - time.time()
    if remaining_sec <= 0:
        return 0, 0
    return max(1, int(math.ceil(remaining_sec / 60.0))), max(1, int(math.ceil(remaining_sec)))


def check_and_disable_device_if_all_offline(device_id: int) -> bool:
    """Auto-disable a device immediately if all of its timeseries variables are offline."""
    with device_retry_lock:
        with db_connect() as conn:
            dev_row = conn.execute(
                "SELECT id, COALESCE(disabled, 0) AS disabled FROM devices WHERE id = ?",
                (device_id,),
            ).fetchone()
            if not dev_row or bool(dev_row["disabled"]):
                return False
            ts_ids = [
                int(r["id"])
                for r in conn.execute(
                    "SELECT id FROM timeseries WHERE device_id = ?",
                    (device_id,),
                ).fetchall()
            ]
            if not ts_ids:
                return False

            with state_lock:
                if device_id in disabled_device_ids:
                    return False
                all_offline = all(ts_id in offline_data_reason for ts_id in ts_ids)

            if not all_offline:
                return False

            settings = get_settings_cached()
            retry_until = None
            if settings["retry_disabled_device_enabled"]:
                retry_until = time.time() + (int(settings["retry_disabled_device_minutes"]) * 60)

            conn.execute(
                """
                UPDATE devices
                SET disabled = 1, auto_disabled = 1, disabled_retry_until = ?
                WHERE id = ?
                """,
                (retry_until, device_id),
            )
            conn.commit()

        set_device_disabled_state(device_id, True)
        reset_timeseries_polling_state(ts_ids)
        return True


def process_due_disabled_device_retries(modbus_id: Optional[int] = None) -> list[int]:
    """Re-enable auto-disabled devices whose retry interval has elapsed."""
    settings = get_settings_cached()
    if not settings["retry_disabled_device_enabled"]:
        return []

    now = time.time()
    re_enabled: list[int] = []
    reset_ts_ids: list[int] = []

    with device_retry_lock:
        with db_connect() as conn:
            if modbus_id is None:
                due_rows = conn.execute(
                    """
                    SELECT id FROM devices
                    WHERE COALESCE(disabled, 0) = 1
                      AND COALESCE(auto_disabled, 0) = 1
                      AND disabled_retry_until IS NOT NULL
                      AND disabled_retry_until <= ?
                    """,
                    (now,),
                ).fetchall()
            else:
                due_rows = conn.execute(
                    """
                    SELECT id FROM devices
                    WHERE modbus_id = ?
                      AND COALESCE(disabled, 0) = 1
                      AND COALESCE(auto_disabled, 0) = 1
                      AND disabled_retry_until IS NOT NULL
                      AND disabled_retry_until <= ?
                    """,
                    (modbus_id, now),
                ).fetchall()

            if not due_rows:
                return []

            for r in due_rows:
                dev_id = int(r["id"])
                ts_ids = [
                    int(ts_r["id"])
                    for ts_r in conn.execute(
                        "SELECT id FROM timeseries WHERE device_id = ?",
                        (dev_id,),
                    ).fetchall()
                ]
                conn.execute(
                    """
                    UPDATE devices
                    SET disabled = 0, auto_disabled = 0, disabled_retry_until = NULL
                    WHERE id = ?
                      AND COALESCE(disabled, 0) = 1
                      AND COALESCE(auto_disabled, 0) = 1
                    """,
                    (dev_id,),
                )
                re_enabled.append(dev_id)
                reset_ts_ids.extend(ts_ids)
            conn.commit()

    for dev_id in re_enabled:
        set_device_disabled_state(dev_id, False)
    if reset_ts_ids:
        reset_timeseries_polling_state(reset_ts_ids)
    return re_enabled


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    start_poller()


@app.on_event("shutdown")
def on_shutdown() -> None:
    global poller_running
    poller_running = False
    close_all_connector_clients()


def list_modbus_clients():
    with db_connect() as conn:
        return conn.execute(
            "SELECT id, name, ip, port FROM modbus_clients ORDER BY id DESC"
        ).fetchall()


def get_modbus_client(modbus_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT id, name, ip, port FROM modbus_clients WHERE id = ?",
            (modbus_id,),
        ).fetchone()


def modbus_name_exists(name: str, exclude_id: Optional[int]) -> bool:
    with db_connect() as conn:
        if exclude_id is None:
            row = conn.execute(
                "SELECT 1 FROM modbus_clients WHERE lower(name) = lower(?)",
                (name,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT 1 FROM modbus_clients WHERE lower(name) = lower(?) AND id != ?",
                (name, exclude_id),
            ).fetchone()
        return row is not None


def save_modbus_client(
    client_id: Optional[int],
    name: str,
    ip: str,
    port: int,
) -> None:
    with db_connect() as conn:
        if client_id is None:
            conn.execute(
                "INSERT INTO modbus_clients (name, ip, port) VALUES (?, ?, ?)",
                (name, ip, port),
            )
        else:
            conn.execute(
                "UPDATE modbus_clients SET name = ?, ip = ?, port = ? WHERE id = ?",
                (name, ip, port, client_id),
            )
        conn.commit()
    if client_id is not None:
        lock = get_connector_lock(client_id)
        with lock:
            close_connector_client(client_id)


def delete_modbus_client(client_id: int) -> None:
    with db_connect() as conn:
        ts_ids = [
            int(r["id"])
            for r in conn.execute(
                "SELECT id FROM timeseries WHERE device_id IN (SELECT id FROM devices WHERE modbus_id = ?)",
                (client_id,),
            ).fetchall()
        ]
        conn.execute(
            "DELETE FROM timeseries WHERE device_id IN (SELECT id FROM devices WHERE modbus_id = ?)",
            (client_id,),
        )
        conn.execute("DELETE FROM devices WHERE modbus_id = ?", (client_id,))
        conn.execute("DELETE FROM modbus_clients WHERE id = ?", (client_id,))
        conn.commit()
    reset_timeseries_polling_state(ts_ids)
    lock = get_connector_lock(client_id)
    with lock:
        close_connector_client(client_id)


def list_devices(modbus_id: int):
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT id, modbus_id, name, unit_id,
                   COALESCE(disabled, 0) AS disabled,
                   COALESCE(auto_disabled, 0) AS auto_disabled,
                   disabled_retry_until
            FROM devices
            WHERE modbus_id = ?
            ORDER BY id DESC
            """,
            (modbus_id,),
        ).fetchall()


def get_device(device_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT id, modbus_id, name, unit_id,
                   COALESCE(disabled, 0) AS disabled,
                   COALESCE(auto_disabled, 0) AS auto_disabled,
                   disabled_retry_until
            FROM devices
            WHERE id = ?
            """,
            (device_id,),
        ).fetchone()


def list_timeseries(device_id: int):
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT id, key_name, data_type, function_code, object_count,
                   register_address, modifier_on, modifier_type, modifier_value,
                   pull_on, report_period_ms, last_value, last_status, last_updated,
                   bundle_group, bundle_role
            FROM timeseries
            WHERE device_id = ?
            ORDER BY id ASC
            """,
            (device_id,),
        ).fetchall()


def replace_timeseries(device_id: int, items: list[dict]) -> list[int]:
    with db_connect() as conn:
        conn.execute("DELETE FROM timeseries WHERE device_id = ?", (device_id,))
        new_ids: list[int] = []
        for item in items:
            cursor = conn.execute(
                """
                INSERT INTO timeseries (
                    device_id, key_name, data_type, function_code, object_count,
                    register_address, modifier_on, modifier_type, modifier_value,
                    pull_on, report_period_ms, bundle_group, bundle_role
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    item["key_name"],
                    item["data_type"],
                    item["function_code"],
                    item["object_count"],
                    item["register_address"],
                    1 if item["modifier_on"] else 0,
                    item.get("modifier_type"),
                    item.get("modifier_value"),
                    1 if item["pull_on"] else 0,
                    item.get("report_period_ms"),
                    item.get("bundle_group"),
                    item.get("bundle_role"),
                ),
            )
            new_ids.append(int(cursor.lastrowid))
        conn.commit()
        return new_ids


def save_device(
    device_id: Optional[int],
    modbus_id: int,
    name: str,
    unit_id: int,
    timeseries_items: list[dict],
    disabled: bool = False,
) -> int:
    old_ts_ids: list[int] = []
    disabled_int = 1 if disabled else 0
    with device_retry_lock:
        with db_connect() as conn:
            if device_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO devices (modbus_id, name, unit_id, disabled, auto_disabled, disabled_retry_until)
                    VALUES (?, ?, ?, ?, 0, NULL)
                    """,
                    (modbus_id, name, unit_id, disabled_int),
                )
                device_id = int(cursor.lastrowid)
            else:
                old_ts_ids = [
                    int(r["id"])
                    for r in conn.execute(
                        "SELECT id FROM timeseries WHERE device_id = ?",
                        (device_id,),
                    ).fetchall()
                ]
                conn.execute(
                    """
                    UPDATE devices
                    SET name = ?, unit_id = ?, disabled = ?, auto_disabled = 0, disabled_retry_until = NULL
                    WHERE id = ?
                    """,
                    (name, unit_id, disabled_int, device_id),
                )
            conn.commit()

        new_ts_ids = replace_timeseries(device_id, timeseries_items)
        set_device_disabled_state(device_id, disabled)
        # Reset offline status and polling schedule whenever a device is edited/saved
        reset_timeseries_polling_state(old_ts_ids + new_ts_ids)
    return device_id


def delete_device(device_id: int) -> None:
    with db_connect() as conn:
        ts_ids = [
            int(r["id"])
            for r in conn.execute(
                "SELECT id FROM timeseries WHERE device_id = ?",
                (device_id,),
            ).fetchall()
        ]
        conn.execute("DELETE FROM timeseries WHERE device_id = ?", (device_id,))
        conn.execute("DELETE FROM devices WHERE id = ?", (device_id,))
        conn.commit()
    set_device_disabled_state(device_id, False)
    reset_timeseries_polling_state(ts_ids)


def parse_port(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def parse_int(value: object, field: str) -> int:
    try:
        return int(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{field} wajib number.") from exc


def parse_float(value: object, field: str) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{field} wajib number.") from exc


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def normalize_timeseries(items: list[dict]) -> list[dict]:
    normalized = []
    bundles: dict[str, list[dict]] = {}

    for item in items:
        key_name = str(item.get("key_name", "")).strip()
        data_type = str(item.get("data_type", "")).strip()
        function_code = parse_int(item.get("function_code"), "Function")
        object_count = parse_int(item.get("object_count"), "Object count")
        register_address = parse_int(item.get("register_address"), "Register address")
        modifier_on = to_bool(item.get("modifier_on"))
        modifier_type = str(item.get("modifier_type", "")).strip() if modifier_on else None
        modifier_value = None
        if modifier_on:
            if modifier_type not in {"divider", "multiplier"}:
                raise HTTPException(status_code=400, detail="Modifier type tidak valid.")
            modifier_value = parse_float(item.get("modifier_value"), "Modifier value")
        pull_on = to_bool(item.get("pull_on"))
        report_period_ms = None
        if pull_on:
            report_period_ms = parse_int(item.get("report_period_ms"), "Report period")
            if report_period_ms <= 0:
                raise HTTPException(status_code=400, detail="Report period wajib > 0.")

        raw_bundle_group = item.get("bundle_group")
        bundle_group = str(raw_bundle_group).strip() if raw_bundle_group not in (None, "") else None
        raw_bundle_role = item.get("bundle_role")
        bundle_role = str(raw_bundle_role).strip() if raw_bundle_role not in (None, "") else None

        if bundle_group:
            if bundle_role not in {"primary", "paired"}:
                raise HTTPException(
                    status_code=400,
                    detail=f"Register '{key_name or '?'}': bundle_role harus 'primary' atau 'paired'.",
                )
        else:
            bundle_role = None

        if not key_name:
            raise HTTPException(status_code=400, detail="Key name wajib diisi.")
        if data_type not in ALLOWED_TYPES:
            raise HTTPException(status_code=400, detail="Type tidak valid.")
        if function_code not in ALLOWED_FUNCTIONS:
            raise HTTPException(status_code=400, detail="Function tidak valid.")
        if object_count <= 0:
            raise HTTPException(status_code=400, detail="Object count wajib > 0.")
        if register_address <= 0:
            raise HTTPException(status_code=400, detail="Register address wajib > 0.")

        entry = {
            "key_name": key_name,
            "data_type": data_type,
            "function_code": function_code,
            "object_count": object_count,
            "register_address": register_address,
            "modifier_on": modifier_on,
            "modifier_type": modifier_type,
            "modifier_value": modifier_value,
            "pull_on": pull_on,
            "report_period_ms": report_period_ms,
            "bundle_group": bundle_group,
            "bundle_role": bundle_role,
        }
        normalized.append(entry)
        if bundle_group:
            bundles.setdefault(bundle_group, []).append(entry)

    # Backend validation for bundle registers: enforce +1 address and inherited fields
    for group_id, group_items in bundles.items():
        if len(group_items) != 2:
            raise HTTPException(
                status_code=400,
                detail=f"Bundle '{group_id}' wajib berisi tepat 2 register (primary dan paired +1).",
            )
        primaries = [g for g in group_items if g["bundle_role"] == "primary"]
        paireds = [g for g in group_items if g["bundle_role"] == "paired"]
        if len(primaries) != 1 or len(paireds) != 1:
            raise HTTPException(
                status_code=400,
                detail=f"Bundle '{group_id}' wajib memiliki tepat 1 primary dan 1 paired.",
            )
        primary = primaries[0]
        paired = paireds[0]

        expected_addr = primary["register_address"] + 1
        if paired["register_address"] != expected_addr:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Bundle '{primary['key_name']}': register pasangan wajib "
                    f"address {expected_addr} (primary {primary['register_address']} + 1), "
                    f"bukan {paired['register_address']}."
                ),
            )

        inherited_fields = (
            "key_name",
            "data_type",
            "object_count",
            "modifier_on",
            "modifier_type",
            "modifier_value",
            "pull_on",
            "report_period_ms",
        )
        for field in inherited_fields:
            if paired[field] != primary[field]:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Bundle '{primary['key_name']}': atribut '{field}' pada register pasangan (+1) "
                        f"wajib mengikuti register utama."
                    ),
                )

    return normalized


def check_tcp_online(ip: str, port: int) -> bool:
    """Check TCP port connectivity to Modbus slave/gateway."""
    try:
        with socket.create_connection((ip, port), timeout=1):
            return True
    except OSError:
        return False


def decode_registers(registers: list[int], data_type: str):
    raw = b"".join(int(reg).to_bytes(2, byteorder="big", signed=False) for reg in registers)
    if data_type == "string":
        return raw.decode("utf-8", errors="replace").strip("\x00")
    if data_type == "bytes":
        return raw.hex(" ")
    if data_type == "bits":
        bits = []
        for reg in registers:
            for bit in range(15, -1, -1):
                bits.append(1 if reg & (1 << bit) else 0)
        return bits
    if data_type == "8int":
        return struct.unpack(">b", raw[:1])[0]
    if data_type == "8uint":
        return struct.unpack(">B", raw[:1])[0]
    if data_type == "16int":
        return struct.unpack(">h", raw[:2])[0]
    if data_type == "16uint":
        return struct.unpack(">H", raw[:2])[0]
    if data_type == "16float":
        return struct.unpack(">e", raw[:2])[0]
    if data_type == "32int":
        return struct.unpack(">i", raw[:4])[0]
    if data_type == "32uint":
        return struct.unpack(">I", raw[:4])[0]
    if data_type == "32float":
        return struct.unpack(">f", raw[:4])[0]
    if data_type == "64int":
        return struct.unpack(">q", raw[:8])[0]
    if data_type == "64uint":
        return struct.unpack(">Q", raw[:8])[0]
    if data_type == "64float":
        return struct.unpack(">d", raw[:8])[0]
    return raw.hex(" ")


def encode_registers(value: object, data_type: str, object_count: int = 1) -> list[int]:
    """Encode a typed value into big-endian 16-bit Modbus registers."""
    if data_type == "raw":
        parts = [p.strip() for p in str(value).split(",") if p.strip() != ""]
        if not parts:
            raise ValueError("Nilai register tidak boleh kosong.")
        regs = []
        for part in parts:
            num = int(part, 0)
            if not 0 <= num <= 65535:
                raise ValueError(f"Register '{part}' harus berada di rentang 0..65535.")
            regs.append(num)
        return regs

    if data_type == "string":
        raw = str(value).encode("utf-8")
        target_len = max(object_count * 2, len(raw) + (len(raw) % 2))
        raw = raw.ljust(target_len, b"\x00")[:target_len]
    elif data_type == "bytes":
        cleaned = str(value).replace(" ", "")
        raw = bytes.fromhex(cleaned)
        if len(raw) % 2 != 0:
            raw += b"\x00"
    elif data_type == "8int":
        raw = b"\x00" + struct.pack(">b", int(str(value).strip()))
    elif data_type == "8uint":
        raw = b"\x00" + struct.pack(">B", int(str(value).strip()))
    elif data_type == "16int":
        raw = struct.pack(">h", int(str(value).strip()))
    elif data_type == "16uint":
        raw = struct.pack(">H", int(str(value).strip()))
    elif data_type == "16float":
        raw = struct.pack(">e", float(str(value).strip()))
    elif data_type == "32int":
        raw = struct.pack(">i", int(str(value).strip()))
    elif data_type == "32uint":
        raw = struct.pack(">I", int(str(value).strip()))
    elif data_type == "32float":
        raw = struct.pack(">f", float(str(value).strip()))
    elif data_type == "64int":
        raw = struct.pack(">q", int(str(value).strip()))
    elif data_type == "64uint":
        raw = struct.pack(">Q", int(str(value).strip()))
    elif data_type == "64float":
        raw = struct.pack(">d", float(str(value).strip()))
    else:
        raise ValueError(f"Tipe data '{data_type}' tidak didukung untuk write register.")

    return [int.from_bytes(raw[i : i + 2], byteorder="big", signed=False) for i in range(0, len(raw), 2)]


def decode_bits(bits: list[bool], data_type: str):
    if data_type == "binary":
        if len(bits) == 1:
            return bool(bits[0])
        return [bool(bit) for bit in bits]
    if len(bits) == 1:
        return 1 if bits[0] else 0
    if data_type == "bits":
        return [1 if bit else 0 for bit in bits]
    if data_type in {"8int", "8uint", "16int", "16uint"}:
        return 1 if bits and bits[0] else 0
    return [1 if bit else 0 for bit in bits]


def apply_modifier(value, modifier_on: bool, modifier_type: Optional[str], modifier_value: Optional[float]):
    if not modifier_on or modifier_value is None:
        return value
    if modifier_type == "divider" and modifier_value != 0:
        return value / modifier_value
    if modifier_type == "multiplier":
        return value * modifier_value
    return value


def raise_if_modbus_error(result) -> None:
    """Translate gateway offline Modbus exceptions (0x0A/0x0B) into ModbusOfflineError."""
    if not result.isError():
        return
    exc_code = getattr(result, "exception_code", None)
    if exc_code in (10, 11):
        raise ModbusOfflineError(f"Gateway target offline (exception {exc_code}): {result}")
    raise RuntimeError(str(result))


def read_timeseries_value(job: sqlite3.Row, timeout_sec: float):
    modbus_id = int(job["modbus_id"])
    lock = get_connector_lock(modbus_id)
    with lock:
        try:
            client = get_connected_client(modbus_id, job["ip"], int(job["port"]), timeout_sec)
            address = max(job["register_address"] - 1, 0)
            count = max(job["object_count"], 1)
            unit_id = job["unit_id"]
            function_code = job["function_code"]

            if function_code in (3, 6, 16):
                result = client.read_holding_registers(
                    address=address,
                    count=count,
                    device_id=unit_id,
                )
                raise_if_modbus_error(result)
                value = decode_registers(result.registers, job["data_type"])
            elif function_code == 4:
                result = client.read_input_registers(
                    address=address,
                    count=count,
                    device_id=unit_id,
                )
                raise_if_modbus_error(result)
                value = decode_registers(result.registers, job["data_type"])
            elif function_code in (1, 5):
                result = client.read_coils(
                    address=address,
                    count=count,
                    device_id=unit_id,
                )
                raise_if_modbus_error(result)
                value = decode_bits(result.bits[:count], job["data_type"])
            elif function_code == 2:
                result = client.read_discrete_inputs(
                    address=address,
                    count=count,
                    device_id=unit_id,
                )
                raise_if_modbus_error(result)
                value = decode_bits(result.bits[:count], job["data_type"])
            else:
                raise RuntimeError("Function tidak dikenal")

            value = apply_modifier(
                value,
                bool(job["modifier_on"]),
                job["modifier_type"],
                job["modifier_value"],
            )
            return value
        except (ModbusException, OSError, TimeoutError) as exc:
            close_connector_client(modbus_id)
            raise ModbusOfflineError(str(exc)) from exc


def write_modbus_value(
    modbus_id: int,
    ip: str,
    port: int,
    unit_id: int,
    function_code: int,
    register_address: int,
    data_type: str,
    value: object,
    object_count: int = 1,
    timeout_sec: float = 2.0,
) -> str:
    address = max(register_address - 1, 0)
    lock = get_connector_lock(modbus_id)
    with lock:
        try:
            client = get_connected_client(modbus_id, ip, port, timeout_sec)

            if function_code == 5:
                coil_val = to_bool(value)
                result = client.write_coil(address=address, value=coil_val, device_id=unit_id)
                if result.isError():
                    raise RuntimeError(str(result))
                return f"FC 5 Write Single Coil addr {register_address} = {coil_val}"

            if function_code == 6:
                regs = encode_registers(value, data_type, 1)
                if len(regs) != 1:
                    raise ValueError(
                        f"FC 6 hanya menulis 1 register (16-bit), tipe '{data_type}' menghasilkan {len(regs)} register. Gunakan FC 16."
                    )
                result = client.write_register(address=address, value=regs[0], device_id=unit_id)
                if result.isError():
                    raise RuntimeError(str(result))
                return f"FC 6 Write Single Register addr {register_address} = {regs[0]} (raw value: {value})"

            if function_code == 16:
                regs = encode_registers(value, data_type, object_count)
                result = client.write_registers(address=address, values=regs, device_id=unit_id)
                if result.isError():
                    raise RuntimeError(str(result))
                return f"FC 16 Write Multiple Registers addr {register_address} ({len(regs)} regs: {regs})"

            raise ValueError("Function code untuk write harus 5, 6, atau 16.")
        except (ModbusException, OSError, TimeoutError) as exc:
            close_connector_client(modbus_id)
            raise RuntimeError(str(exc)) from exc


def update_timeseries_result(ts_id: int, value: Optional[object], status: str) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE timeseries
            SET last_value = ?, last_status = ?, last_updated = ?
            WHERE id = ?
            """,
            (
                None if value is None else str(value),
                status,
                int(time.time() * 1000),
                ts_id,
            ),
        )
        conn.commit()


def get_timeseries_jobs_for_connector(modbus_id: int):
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT ts.id, ts.key_name, ts.data_type, ts.function_code,
                   ts.object_count, ts.register_address, ts.modifier_on,
                   ts.modifier_type, ts.modifier_value, ts.pull_on,
                   ts.report_period_ms, ts.device_id, d.modbus_id, d.unit_id,
                   d.name AS device_name, m.ip, m.port
            FROM timeseries ts
            JOIN devices d ON ts.device_id = d.id
            JOIN modbus_clients m ON d.modbus_id = m.id
            WHERE d.modbus_id = ? AND COALESCE(d.disabled, 0) = 0
            ORDER BY d.unit_id ASC, ts.register_address ASC, ts.id ASC
            """,
            (modbus_id,),
        ).fetchall()


def connector_poller_loop(modbus_id: int) -> None:
    while poller_running:
        # Stop worker cleanly if connector was deleted
        if not get_modbus_client(modbus_id):
            break

        try:
            process_due_disabled_device_retries(modbus_id)
        except Exception:
            pass

        now = time.time()
        settings = get_settings_cached()
        timeout_sec = float(settings["modbus_timeout_sec"])
        retry_total_sec = int(settings["offline_retry_total_sec"])

        try:
            jobs = get_timeseries_jobs_for_connector(modbus_id)
        except Exception:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        for job in jobs:
            if not poller_running:
                break

            dev_id = int(job["device_id"])
            if is_device_disabled(dev_id):
                continue

            ts_id = int(job["id"])
            is_off, _ = get_offline_info(ts_id)
            if is_off:
                continue

            interval_ms = 1000
            if job["pull_on"]:
                interval_ms = int(job["report_period_ms"] or 1000)
            interval_sec = max(interval_ms / 1000.0, 0.2)

            with state_lock:
                next_run = timeseries_next_run.get(ts_id, 0)
            if now < next_run:
                continue

            try:
                value = read_timeseries_value(job, timeout_sec)
                if is_device_disabled(dev_id):
                    continue
                clear_offline_data(ts_id)
                update_timeseries_result(ts_id, value, "ok")
            except ModbusOfflineError as exc:
                if is_device_disabled(dev_id):
                    continue
                mark_offline_data(ts_id, str(exc), retry_total_sec)
                update_timeseries_result(ts_id, None, f"OFFLINE DATA: {exc}")
                check_and_disable_device_if_all_offline(dev_id)
            except Exception as exc:
                if is_device_disabled(dev_id):
                    continue
                clear_offline_data(ts_id)
                update_timeseries_result(ts_id, None, f"error: {exc}")

            with state_lock:
                timeseries_next_run[ts_id] = time.time() + interval_sec

        time.sleep(POLL_INTERVAL_SEC)

    connector_threads.pop(modbus_id, None)


def poller_supervisor_loop() -> None:
    while poller_running:
        try:
            process_due_disabled_device_retries()
            clients = list_modbus_clients()
            for row in clients:
                mid = int(row["id"])
                th = connector_threads.get(mid)
                if th is None or not th.is_alive():
                    new_th = threading.Thread(
                        target=connector_poller_loop,
                        args=(mid,),
                        daemon=True,
                    )
                    connector_threads[mid] = new_th
                    new_th.start()
        except Exception:
            pass
        time.sleep(1.0)


def start_poller() -> None:
    global supervisor_thread
    if supervisor_thread and supervisor_thread.is_alive():
        return
    supervisor_thread = threading.Thread(target=poller_supervisor_loop, daemon=True)
    supervisor_thread.start()


def load_html() -> str:
    with open(HTML_PATH, "r", encoding="utf-8") as handle:
        return handle.read()


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse(load_html())


@app.get("/api/settings")
def settings_read() -> JSONResponse:
    return JSONResponse(load_settings())


@app.post("/api/settings/save")
async def settings_save(request: Request) -> JSONResponse:
    payload = await request.json()
    timeout_sec = parse_float(payload.get("modbus_timeout_sec"), "Modbus Timeout")
    retry_min = parse_int(payload.get("offline_retry_minutes", 0), "Offline Retry Minutes")
    retry_sec = parse_int(payload.get("offline_retry_seconds", 0), "Offline Retry Seconds")
    retry_disabled_enabled = to_bool(payload.get("retry_disabled_device_enabled", False))
    raw_retry_disabled_min = payload.get("retry_disabled_device_minutes")

    if retry_disabled_enabled:
        if raw_retry_disabled_min in (None, ""):
            raise HTTPException(
                status_code=400,
                detail="Retry Disabled Device (minutes) wajib diisi jika diaktifkan.",
            )
        retry_disabled_minutes = parse_int(raw_retry_disabled_min, "Retry Disabled Device Minutes")
        if retry_disabled_minutes <= 0:
            raise HTTPException(
                status_code=400,
                detail="Retry Disabled Device (minutes) minimal 1 menit.",
            )
    else:
        if raw_retry_disabled_min in (None, ""):
            retry_disabled_minutes = load_settings()["retry_disabled_device_minutes"]
        else:
            retry_disabled_minutes = max(
                parse_int(raw_retry_disabled_min, "Retry Disabled Device Minutes"),
                1,
            )

    if timeout_sec < 0.2:
        raise HTTPException(status_code=400, detail="Modbus Timeout minimal 0.2 detik.")
    if retry_min < 0 or retry_sec < 0:
        raise HTTPException(status_code=400, detail="Offline Device Retry tidak boleh negatif.")
    if retry_sec >= 60:
        retry_min += retry_sec // 60
        retry_sec = retry_sec % 60
    if (retry_min * 60 + retry_sec) <= 0:
        raise HTTPException(status_code=400, detail="Offline Retry minimal 1 detik.")

    save_settings_db(
        timeout_sec,
        retry_min,
        retry_sec,
        retry_disabled_device_enabled=retry_disabled_enabled,
        retry_disabled_device_minutes=retry_disabled_minutes,
    )
    return JSONResponse(
        {
            "ok": True,
            "message": "Settings tersimpan.",
            "settings": load_settings(),
        }
    )


@app.get("/api/modbus")
def modbus_list() -> JSONResponse:
    items = []
    for row in list_modbus_clients():
        items.append(
            {
                "id": row["id"],
                "name": row["name"],
                "ip": row["ip"],
                "port": row["port"],
                "online": check_tcp_online(row["ip"], row["port"]),
            }
        )
    return JSONResponse({"items": items})


@app.get("/api/modbus/{modbus_id}")
def modbus_detail(modbus_id: int) -> JSONResponse:
    row = get_modbus_client(modbus_id)
    if not row:
        raise HTTPException(status_code=404, detail="Connector tidak ditemukan.")
    return JSONResponse(
        {
            "id": row["id"],
            "name": row["name"],
            "ip": row["ip"],
            "port": row["port"],
        }
    )


@app.post("/api/modbus/save")
async def modbus_save(request: Request) -> JSONResponse:
    payload = await request.json()
    raw_id = payload.get("id")
    client_id = None
    if raw_id not in (None, ""):
        client_id = parse_port(str(raw_id))
        if client_id is None:
            raise HTTPException(status_code=400, detail="ID tidak valid.")

    name = str(payload.get("name", "")).strip()
    ip = str(payload.get("ip", "")).strip()
    port = parse_port(str(payload.get("port", "")))

    if not name:
        raise HTTPException(status_code=400, detail="Nama connector wajib diisi.")
    if not ip or port is None:
        raise HTTPException(status_code=400, detail="IP dan port wajib diisi.")
    if modbus_name_exists(name, client_id):
        raise HTTPException(status_code=400, detail="Nama connector sudah dipakai.")

    save_modbus_client(client_id, name, ip, port)
    return JSONResponse({"ok": True, "message": "Data tersimpan."})


@app.post("/api/modbus/delete")
async def modbus_delete(request: Request) -> JSONResponse:
    payload = await request.json()
    raw_id = payload.get("id")
    client_id = None
    if raw_id not in (None, ""):
        client_id = parse_port(str(raw_id))

    if client_id is None:
        raise HTTPException(status_code=400, detail="ID tidak valid.")

    delete_modbus_client(client_id)
    return JSONResponse({"ok": True, "message": "Data terhapus."})


@app.post("/api/modbus/write")
async def modbus_write(request: Request) -> JSONResponse:
    payload = await request.json()
    modbus_id = parse_int(payload.get("modbus_id"), "Modbus ID")
    unit_id = parse_int(payload.get("unit_id"), "Unit ID")
    function_code = parse_int(payload.get("function_code"), "Function code")
    register_address = parse_int(payload.get("register_address"), "Register address")
    data_type = str(payload.get("data_type", "16uint")).strip()
    object_count = parse_int(payload.get("object_count", 1) or 1, "Object count")
    value = payload.get("value")
    ts_id_raw = payload.get("ts_id")

    if unit_id <= 0:
        raise HTTPException(status_code=400, detail="Unit ID wajib > 0.")
    if function_code not in WRITABLE_FUNCTIONS:
        raise HTTPException(status_code=400, detail="Function code untuk write harus 5, 6, atau 16.")
    if register_address <= 0:
        raise HTTPException(status_code=400, detail="Register address wajib > 0.")
    if value is None or str(value).strip() == "":
        raise HTTPException(status_code=400, detail="Value wajib diisi.")

    connector = get_modbus_client(modbus_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector tidak ditemukan.")

    ts_id = None
    if ts_id_raw not in (None, ""):
        ts_id = parse_port(str(ts_id_raw))
        if ts_id is None:
            raise HTTPException(status_code=400, detail="Timeseries ID tidak valid.")
        with db_connect() as conn:
            ts_row = conn.execute(
                "SELECT function_code FROM timeseries WHERE id = ?",
                (ts_id,),
            ).fetchone()
        if not ts_row:
            raise HTTPException(status_code=404, detail="Timeseries tidak ditemukan.")
        if ts_row["function_code"] not in WRITABLE_FUNCTIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Timeseries dengan FC {ts_row['function_code']} bertipe Read dan tidak dapat di-write.",
            )

    timeout_sec = float(get_settings_cached()["modbus_timeout_sec"])
    try:
        msg = write_modbus_value(
            modbus_id=modbus_id,
            ip=connector["ip"],
            port=int(connector["port"]),
            unit_id=unit_id,
            function_code=function_code,
            register_address=register_address,
            data_type=data_type,
            value=value,
            object_count=max(object_count, 1),
            timeout_sec=timeout_sec,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Write gagal: {exc}") from exc

    if ts_id is not None:
        reset_timeseries_polling_state([ts_id])

    return JSONResponse({"ok": True, "message": msg})


@app.post("/api/modbus/test-read")
async def modbus_test_read(request: Request) -> JSONResponse:
    """One-shot read for the Add/Edit device form. Does not touch offline/polling state or DB."""
    payload = await request.json()
    modbus_id = parse_int(payload.get("modbus_id"), "Modbus ID")
    connector = get_modbus_client(modbus_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector tidak ditemukan.")

    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Tidak ada register untuk di-test.")
    unit_id = parse_int(payload.get("unit_id"), "Unit ID")
    if unit_id <= 0:
        raise HTTPException(status_code=400, detail="Unit ID wajib > 0.")

    timeout_sec = float(get_settings_cached()["modbus_timeout_sec"])
    results = []
    for item in items:
        try:
            data_type = str(item.get("data_type", "")).strip()
            function_code = parse_int(item.get("function_code"), "Function")
            object_count = parse_int(item.get("object_count"), "Object count")
            register_address = parse_int(item.get("register_address"), "Register address")
            if data_type not in ALLOWED_TYPES:
                raise HTTPException(status_code=400, detail="Type tidak valid.")
            if function_code not in ALLOWED_FUNCTIONS:
                raise HTTPException(status_code=400, detail="Function tidak valid.")
            if object_count <= 0 or register_address <= 0:
                raise HTTPException(status_code=400, detail="Object count & address wajib > 0.")
            modifier_on = to_bool(item.get("modifier_on"))
            modifier_value = None
            modifier_type = None
            if modifier_on:
                modifier_type = str(item.get("modifier_type", "")).strip()
                modifier_value = parse_float(item.get("modifier_value"), "Modifier value")
        except HTTPException as exc:
            results.append({"address": item.get("register_address"), "ok": False, "error": str(exc.detail)})
            continue

        job = {
            "modbus_id": modbus_id,
            "ip": connector["ip"],
            "port": connector["port"],
            "unit_id": unit_id,
            "function_code": function_code,
            "register_address": register_address,
            "object_count": object_count,
            "data_type": data_type,
            "modifier_on": modifier_on,
            "modifier_type": modifier_type,
            "modifier_value": modifier_value,
        }
        try:
            value = await run_in_threadpool(read_timeseries_value, job, timeout_sec)
            results.append({"address": register_address, "function_code": function_code, "ok": True, "value": value})
        except ModbusOfflineError as exc:
            results.append({"address": register_address, "function_code": function_code, "ok": False, "error": f"timeout ({exc})"})
        except Exception as exc:
            results.append({"address": register_address, "function_code": function_code, "ok": False, "error": str(exc)})
    return JSONResponse({"ok": True, "results": results})


@app.get("/api/modbus/{modbus_id}/devices")
def device_list(modbus_id: int) -> JSONResponse:
    settings = get_settings_cached()
    devices = []
    for row in list_devices(modbus_id):
        retry_in_min, retry_in_sec = compute_device_retry_remaining(row, settings)
        ts_rows = []
        for ts in list_timeseries(row["id"]):
            ts_id = int(ts["id"])
            is_off, retry_in = get_offline_info(ts_id)
            ts_rows.append(
                {
                    "id": ts_id,
                    "key_name": ts["key_name"],
                    "data_type": ts["data_type"],
                    "object_count": ts["object_count"],
                    "register_address": ts["register_address"],
                    "last_value": ts["last_value"],
                    "last_status": ts["last_status"],
                    "last_updated": ts["last_updated"],
                    "function_code": ts["function_code"],
                    "bundle_group": ts["bundle_group"],
                    "bundle_role": ts["bundle_role"],
                    "offline": is_off,
                    "offline_retry_in": retry_in,
                }
            )
        devices.append(
            {
                "id": row["id"],
                "modbus_id": row["modbus_id"],
                "name": row["name"],
                "unit_id": row["unit_id"],
                "disabled": bool(row["disabled"]),
                "auto_disabled": bool(row["auto_disabled"]),
                "disabled_retry_in_minutes": retry_in_min,
                "disabled_retry_in_seconds": retry_in_sec,
                "timeseries": ts_rows,
            }
        )
    return JSONResponse({"items": devices})


@app.get("/api/devices/{device_id}")
def device_detail(device_id: int) -> JSONResponse:
    row = get_device(device_id)
    if not row:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan.")
    settings = get_settings_cached()
    retry_in_min, retry_in_sec = compute_device_retry_remaining(row, settings)
    timeseries_rows = []
    for ts in list_timeseries(device_id):
        timeseries_rows.append(
            {
                "id": ts["id"],
                "key_name": ts["key_name"],
                "data_type": ts["data_type"],
                "function_code": ts["function_code"],
                "object_count": ts["object_count"],
                "register_address": ts["register_address"],
                "modifier_on": bool(ts["modifier_on"]),
                "modifier_type": ts["modifier_type"],
                "modifier_value": ts["modifier_value"],
                "pull_on": bool(ts["pull_on"]),
                "report_period_ms": ts["report_period_ms"],
                "bundle_group": ts["bundle_group"],
                "bundle_role": ts["bundle_role"],
            }
        )
    return JSONResponse(
        {
            "id": row["id"],
            "modbus_id": row["modbus_id"],
            "name": row["name"],
            "unit_id": row["unit_id"],
            "disabled": bool(row["disabled"]),
            "auto_disabled": bool(row["auto_disabled"]),
            "disabled_retry_in_minutes": retry_in_min,
            "disabled_retry_in_seconds": retry_in_sec,
            "timeseries": timeseries_rows,
        }
    )


@app.post("/api/devices/save")
async def device_save(request: Request) -> JSONResponse:
    payload = await request.json()
    raw_id = payload.get("id")
    device_id = None
    if raw_id not in (None, ""):
        device_id = parse_port(str(raw_id))
        if device_id is None:
            raise HTTPException(status_code=400, detail="ID tidak valid.")

    raw_dup_from = payload.get("duplicate_from_id")
    duplicate_from_id = None
    if raw_dup_from not in (None, ""):
        duplicate_from_id = parse_port(str(raw_dup_from))
        if duplicate_from_id is None:
            raise HTTPException(status_code=400, detail="duplicate_from_id tidak valid.")

    modbus_id = parse_int(payload.get("modbus_id"), "Modbus ID")
    name = str(payload.get("name", "")).strip()
    unit_id = parse_int(payload.get("unit_id"), "Unit ID")
    disabled = to_bool(payload.get("disabled", False))
    timeseries_items = payload.get("timeseries", [])

    if not name:
        raise HTTPException(status_code=400, detail="Device name wajib diisi.")
    if unit_id <= 0:
        raise HTTPException(status_code=400, detail="Unit ID wajib > 0.")
    if not get_modbus_client(modbus_id):
        raise HTTPException(status_code=404, detail="Connector tidak ditemukan.")

    # Verify unit_id is unique on this modbus connector (required for duplicate & normal save)
    with db_connect() as conn:
        if duplicate_from_id is not None:
            src_dev = conn.execute(
                "SELECT id, unit_id FROM devices WHERE id = ?",
                (duplicate_from_id,),
            ).fetchone()
            if src_dev and int(src_dev["unit_id"]) == unit_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"Duplicate device wajib menggunakan Unit ID berbeda dari {unit_id}.",
                )

        existing_unit = conn.execute(
            """
            SELECT id, name FROM devices
            WHERE modbus_id = ? AND unit_id = ? AND (? IS NULL OR id != ?)
            """,
            (modbus_id, unit_id, device_id, device_id),
        ).fetchone()
        if existing_unit:
            raise HTTPException(
                status_code=400,
                detail=f"Unit ID {unit_id} sudah dipakai oleh device '{existing_unit['name']}' pada connector ini.",
            )

    normalized = normalize_timeseries(timeseries_items)
    device_id = save_device(device_id, modbus_id, name, unit_id, normalized, disabled=disabled)
    return JSONResponse({"ok": True, "id": device_id, "message": "Device tersimpan (status polling & OFFLINE DATA direset)."})


@app.post("/api/devices/toggle-disable")
async def device_toggle_disable(request: Request) -> JSONResponse:
    payload = await request.json()
    raw_id = payload.get("id")
    device_id = parse_port(str(raw_id)) if raw_id not in (None, "") else None
    if device_id is None:
        raise HTTPException(status_code=400, detail="ID tidak valid.")

    row = get_device(device_id)
    if not row:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan.")

    if "disabled" in payload and payload["disabled"] is not None:
        new_disabled = to_bool(payload["disabled"])
    else:
        new_disabled = not bool(row["disabled"])

    with device_retry_lock:
        with db_connect() as conn:
            ts_ids = [
                int(r["id"])
                for r in conn.execute(
                    "SELECT id FROM timeseries WHERE device_id = ?",
                    (device_id,),
                ).fetchall()
            ]
            conn.execute(
                "UPDATE devices SET disabled = ?, auto_disabled = 0, disabled_retry_until = NULL WHERE id = ?",
                (1 if new_disabled else 0, device_id),
            )
            conn.commit()

        set_device_disabled_state(device_id, new_disabled)
        reset_timeseries_polling_state(ts_ids)
    status_label = "dinonaktifkan (polling dihentikan)" if new_disabled else "diaktifkan kembali (polling berjalan)"
    return JSONResponse(
        {
            "ok": True,
            "disabled": new_disabled,
            "message": f"Device '{row['name']}' {status_label}.",
        }
    )


@app.post("/api/devices/delete")
async def device_delete(request: Request) -> JSONResponse:
    payload = await request.json()
    raw_id = payload.get("id")
    device_id = None
    if raw_id not in (None, ""):
        device_id = parse_port(str(raw_id))

    if device_id is None:
        raise HTTPException(status_code=400, detail="ID tidak valid.")

    delete_device(device_id)
    return JSONResponse({"ok": True, "message": "Device terhapus."})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8010,
        reload=True,
        app_dir=APP_DIR,
        reload_dirs=[APP_DIR],
    )
