from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
import json
import os
import socket
import sqlite3
import struct
import threading
import time
from typing import Optional

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusTcpClient

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "app_rpc.db")
HTML_PATH = os.path.join(APP_DIR, "app_rpc.html")

app = FastAPI(title="ThingsBoard Gateway RPC")

POLL_INTERVAL_SEC = 0.2
timeseries_next_run: dict[int, float] = {}
poller_thread: Optional[threading.Thread] = None

settings_cache: dict = {"value": None, "ts": 0.0}
SETTINGS_CACHE_TTL_SEC = 2.0

rpc_logs: list[dict] = []
MAX_RPC_LOGS = 100


def add_rpc_log(topic: str, raw_payload: str) -> None:
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_entry = {
        "id": int(time.time() * 1000),
        "timestamp": now_str,
        "topic": topic,
        "payload": raw_payload,
    }
    rpc_logs.insert(0, log_entry)
    if len(rpc_logs) > MAX_RPC_LOGS:
        rpc_logs.pop()


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
                tb_ip TEXT NOT NULL DEFAULT '',
                tb_port INTEGER NOT NULL DEFAULT 8080,
                tb_user TEXT NOT NULL DEFAULT '',
                tb_pass TEXT NOT NULL DEFAULT '',
                rpc_ip TEXT NOT NULL DEFAULT '',
                rpc_port INTEGER NOT NULL DEFAULT 1883,
                rpc_token TEXT NOT NULL DEFAULT ''
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
                tb_token TEXT
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
                last_updated INTEGER
            )
            """
        )
        ensure_column(conn, "modbus_clients", "name", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "settings", "tb_ip", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "settings", "tb_port", "INTEGER NOT NULL DEFAULT 8080")
        ensure_column(conn, "settings", "tb_user", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "settings", "tb_pass", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "settings", "rpc_ip", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "settings", "rpc_port", "INTEGER NOT NULL DEFAULT 1883")
        ensure_column(conn, "settings", "rpc_token", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "timeseries", "rpc_method", "TEXT")
        conn.commit()


def load_settings() -> dict:
    with db_connect() as conn:
        row = conn.execute(
            """SELECT tb_ip, tb_port, tb_user, tb_pass,
                      rpc_ip, rpc_port, rpc_token
               FROM settings WHERE id = 1"""
        ).fetchone()

    if not row:
        return {
            "tb_ip": "",
            "tb_port": "8080",
            "tb_user": "",
            "tb_pass": "",
            "rpc_ip": "",
            "rpc_port": "1883",
            "rpc_token": "",
        }

    keys = row.keys()
    return {
        "tb_ip": row["tb_ip"] if "tb_ip" in keys else "",
        "tb_port": str(row["tb_port"]) if "tb_port" in keys else "8080",
        "tb_user": row["tb_user"] if "tb_user" in keys else "",
        "tb_pass": row["tb_pass"] if "tb_pass" in keys else "",
        "rpc_ip": row["rpc_ip"] if "rpc_ip" in keys else "",
        "rpc_port": str(row["rpc_port"]) if "rpc_port" in keys else "1883",
        "rpc_token": row["rpc_token"] if "rpc_token" in keys else "",
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


def save_web_settings(settings: dict) -> None:
    with db_connect() as conn:
        row = conn.execute("SELECT 1 FROM settings WHERE id = 1").fetchone()
        if not row:
            conn.execute(
                """INSERT INTO settings (id, tb_ip, tb_port, tb_user, tb_pass, rpc_ip, rpc_port, rpc_token)
                   VALUES (1, ?, ?, ?, ?, '', 1883, '')""",
                (settings["tb_ip"], settings["tb_port"], settings["tb_user"], settings["tb_pass"]),
            )
        else:
            conn.execute(
                """UPDATE settings
                   SET tb_ip = ?, tb_port = ?, tb_user = ?, tb_pass = ?
                   WHERE id = 1""",
                (settings["tb_ip"], settings["tb_port"], settings["tb_user"], settings["tb_pass"]),
            )
        conn.commit()


def save_rpc_settings(settings: dict) -> None:
    with db_connect() as conn:
        row = conn.execute("SELECT 1 FROM settings WHERE id = 1").fetchone()
        if not row:
            conn.execute(
                """INSERT INTO settings (id, tb_ip, tb_port, tb_user, tb_pass, rpc_ip, rpc_port, rpc_token)
                   VALUES (1, '', 8080, '', '', ?, ?, ?)""",
                (settings["rpc_ip"], settings["rpc_port"], settings["rpc_token"]),
            )
        else:
            conn.execute(
                """UPDATE settings
                   SET rpc_ip = ?, rpc_port = ?, rpc_token = ?
                   WHERE id = 1""",
                (settings["rpc_ip"], settings["rpc_port"], settings["rpc_token"]),
            )
        conn.commit()


def tb_get_bearer_token(settings: dict) -> str:
    import http.client

    conn = http.client.HTTPConnection(
        settings["tb_ip"],
        int(settings["tb_port"]),
        timeout=10,
    )
    payload = json.dumps(
        {"username": settings["tb_user"], "password": settings["tb_pass"]}
    )
    headers = {"Content-Type": "application/json"}
    try:
        conn.request("POST", "/api/auth/login", body=payload, headers=headers)
        resp = conn.getresponse()
        data = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise RuntimeError(f"Login failed: {resp.status} {resp.reason}: {data}")
        token = json.loads(data).get("token")
        if not token:
            raise RuntimeError(f"Login response missing token: {data}")
        return token
    finally:
        conn.close()


def parse_stored_value(val_str: Optional[object]):
    if val_str is None:
        return None
    s = str(val_str).strip()
    if s.lower() in ("true", "on"):
        return 1
    if s.lower() in ("false", "off"):
        return 0
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return s


# -----------------------------------------------------------------------------
# Background MQTT Manager for RPC & Telemetry
# -----------------------------------------------------------------------------

class MqttManager(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.client = None
        self.connected = False
        self.active_settings = None
        self.running = True

    def run(self):
        while self.running:
            try:
                settings = None
                with db_connect() as conn:
                    row = conn.execute("SELECT rpc_ip, rpc_port, rpc_token FROM settings WHERE id = 1").fetchone()
                    if row and row["rpc_ip"] and row["rpc_token"]:
                        settings = (row["rpc_ip"], int(row["rpc_port"]), row["rpc_token"])

                if settings != self.active_settings:
                    if self.client:
                        print("[MQTT] Settings updated. Reconnecting to new broker/token...")
                        try:
                            self.client.loop_stop()
                            self.client.disconnect()
                        except Exception as e:
                            print(f"[MQTT] Error disconnecting old client: {e}")
                        self.client = None
                        self.connected = False
                    self.active_settings = settings

                if self.active_settings and not self.client:
                    ip, port, token = self.active_settings
                    if ip and port and token:
                        try:
                            try:
                                client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
                            except AttributeError:
                                client = mqtt.Client()

                            client.username_pw_set(username=token)

                            def on_connect(*args, **kwargs):
                                client_obj = args[0]
                                rc = args[3]
                                rc_val = getattr(rc, "value", rc)
                                if rc_val == 0:
                                    print("[MQTT] Successfully connected to ThingsBoard broker.")
                                    self.connected = True
                                    client_obj.subscribe("v1/gateway/rpc")
                                    client_obj.subscribe("v1/devices/me/rpc/request/+")
                                else:
                                    print(f"[MQTT] Connection rejected, rc = {rc_val}")
                                    self.connected = False

                            def on_disconnect(*args, **kwargs):
                                print("[MQTT] Connection disconnected.")
                                self.connected = False

                            def on_message(*args, **kwargs):
                                client_obj = args[0]
                                msg = args[2]
                                topic = getattr(msg, "topic", "v1/gateway/rpc")
                                raw_str = msg.payload.decode("utf-8", errors="replace")
                                print(f"[MQTT INCOMING] Topic: {topic} | Payload: {raw_str}")
                                add_rpc_log(topic, raw_str)
                                if topic == "v1/gateway/rpc":
                                    try:
                                        payload = json.loads(raw_str)
                                        handle_rpc_command(client_obj, payload)
                                    except Exception as ex:
                                        print(f"[MQTT] Error in RPC message handler: {ex}")

                            client.on_connect = on_connect
                            client.on_disconnect = on_disconnect
                            client.on_message = on_message

                            print(f"[MQTT] Connecting to {ip}:{port} using Gateway Token {token[:6]}...")
                            client.connect(ip, port, keepalive=60)
                            client.loop_start()
                            self.client = client
                        except Exception as ex:
                            print(f"[MQTT] Initial connection failure: {ex}")
                            self.client = None
                            self.connected = False

                if self.connected and self.client:
                    self.publish_all_telemetry()

            except Exception as ex:
                print(f"[MQTT] Error in background manager thread: {ex}")

            time.sleep(1.0)

    def publish_all_telemetry(self):
        if not self.connected or not self.client:
            return
        try:
            payload = {}
            with db_connect() as conn:
                devices = conn.execute("SELECT id, name FROM devices").fetchall()
                for d in devices:
                    ts_rows = conn.execute(
                        "SELECT key_name, last_value FROM timeseries WHERE device_id = ? AND last_value IS NOT NULL",
                        (d["id"],),
                    ).fetchall()
                    if ts_rows:
                        device_data = {}
                        for ts in ts_rows:
                            parsed_val = parse_stored_value(ts["last_value"])
                            if parsed_val is not None:
                                device_data[ts["key_name"]] = sanitize_telemetry_value(parsed_val)
                        if device_data:
                            payload[d["name"]] = [device_data]

            if payload:
                self.client.publish("v1/gateway/telemetry", json.dumps(payload), qos=1)
        except Exception as ex:
            print(f"[MQTT] Telemetry push failed: {ex}")


def handle_rpc_command(client, payload: dict) -> None:
    device_name = payload.get("device")
    data = payload.get("data", {})
    rpc_id = payload.get("id") if payload.get("id") is not None else data.get("id")
    method = data.get("method")
    params = data.get("params")

    if not device_name or not method or rpc_id is None:
        print(f"[RPC] Error: Invalid RPC request envelope. Request dump: {json.dumps(payload)}")
        return

    print(f"[RPC] Command received for '{device_name}' (ID={rpc_id}): method='{method}', params={params}")

    write_success = False
    write_msg = ""
    try:
        with db_connect() as conn:
            job = conn.execute(
                """
                SELECT ts.id, ts.key_name, ts.data_type, ts.function_code,
                       ts.register_address, d.unit_id, m.ip, m.port
                FROM timeseries ts
                JOIN devices d ON ts.device_id = d.id
                JOIN modbus_clients m ON d.modbus_id = m.id
                WHERE lower(d.name) = lower(?) AND ts.function_code = 5 AND lower(ts.rpc_method) = lower(?)
                """,
                (device_name, method),
            ).fetchone()
            
        if job:
            modbus_client = ModbusTcpClient(host=job["ip"], port=job["port"], timeout=1)
            try:
                if not modbus_client.connect():
                    raise RuntimeError("Modbus client connection failed")
                
                address = max(job["register_address"] - 1, 0)
                unit_id = job["unit_id"]
                
                val_to_write = False
                if isinstance(params, bool):
                    val_to_write = params
                elif str(params).strip().lower() in ("true", "1", "on"):
                    val_to_write = True
                
                print(f"[RPC] Writing Single Coil to address {address} on slave {unit_id} with value {val_to_write}")
                result = modbus_client.write_coil(
                    address=address,
                    value=val_to_write,
                    device_id=unit_id,
                )
                if result.isError():
                    raise RuntimeError(str(result))
                
                write_success = True
                write_msg = f"Successfully wrote {val_to_write} to coil address {job['register_address']}."
                
                # Update timeseries in database
                update_timeseries_result(job["id"], val_to_write, "ok")
                
                # Push telemetry to ThingsBoard Gateway
                if client:
                    telemetry_payload = {device_name: [{job["key_name"]: val_to_write}]}
                    client.publish("v1/gateway/telemetry", json.dumps(telemetry_payload), qos=1)
            except Exception as ex:
                write_success = False
                write_msg = f"Modbus write failed: {ex}"
                print(f"[RPC] Modbus write error: {ex}")
            finally:
                try:
                    modbus_client.close()
                except Exception:
                    pass
        else:
            write_success = False
            write_msg = f"No matching Modbus device/method found for '{device_name}' with method '{method}'"
            print(f"[RPC] {write_msg}")
    except Exception as ex:
        write_success = False
        write_msg = f"Database query failed: {ex}"
        print(f"[RPC] Database error: {ex}")

    response_payload = {
        "device": device_name,
        "id": rpc_id,
        "data": {
            "success": write_success,
            "message": write_msg
        }
    }
    try:
        client.publish("v1/gateway/rpc", json.dumps(response_payload), qos=1)
        print(f"[RPC] Sent reply to ThingsBoard for Request ID {rpc_id}.")
    except Exception as ex:
        print(f"[RPC] Failed to send reply: {ex}")


mqtt_manager = MqttManager()


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    mqtt_manager.start()
    start_poller()


@app.on_event("shutdown")
def on_shutdown() -> None:
    mqtt_manager.running = False
    if mqtt_manager.client:
        try:
            mqtt_manager.client.loop_stop()
            mqtt_manager.client.disconnect()
        except Exception:
            pass


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


def delete_modbus_client(client_id: int) -> None:
    with db_connect() as conn:
        conn.execute(
            "DELETE FROM timeseries WHERE device_id IN (SELECT id FROM devices WHERE modbus_id = ?)",
            (client_id,),
        )
        conn.execute("DELETE FROM devices WHERE modbus_id = ?", (client_id,))
        conn.execute("DELETE FROM modbus_clients WHERE id = ?", (client_id,))
        conn.commit()


def list_devices(modbus_id: int):
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT id, modbus_id, name, unit_id, tb_token
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
            SELECT id, modbus_id, name, unit_id, tb_token
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
                   rpc_method
            FROM timeseries
            WHERE device_id = ?
            ORDER BY id ASC
            """,
            (device_id,),
        ).fetchall()


def replace_timeseries(device_id: int, items: list[dict]) -> None:
    with db_connect() as conn:
        conn.execute("DELETE FROM timeseries WHERE device_id = ?", (device_id,))
        for item in items:
            conn.execute(
                """
                INSERT INTO timeseries (
                    device_id, key_name, data_type, function_code, object_count,
                    register_address, modifier_on, modifier_type, modifier_value,
                    pull_on, report_period_ms, rpc_method
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    item.get("rpc_method"),
                ),
            )
        conn.commit()


def save_device(
    device_id: Optional[int],
    modbus_id: int,
    name: str,
    unit_id: int,
    tb_token: Optional[str],
    timeseries_items: list[dict],
) -> int:
    with db_connect() as conn:
        if device_id is None:
            cursor = conn.execute(
                """
                INSERT INTO devices (modbus_id, name, unit_id, tb_token)
                VALUES (?, ?, ?, ?)
                """,
                (modbus_id, name, unit_id, tb_token),
            )
            device_id = int(cursor.lastrowid)
        else:
            conn.execute(
                """
                UPDATE devices
                SET name = ?, unit_id = ?, tb_token = ?
                WHERE id = ?
                """,
                (name, unit_id, tb_token, device_id),
            )
        conn.commit()

    replace_timeseries(device_id, timeseries_items)
    return device_id


def delete_device(device_id: int) -> None:
    with db_connect() as conn:
        conn.execute("DELETE FROM timeseries WHERE device_id = ?", (device_id,))
        conn.execute("DELETE FROM devices WHERE id = ?", (device_id,))
        conn.commit()


def parse_port(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


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

ALLOWED_FUNCTIONS = {1, 2, 3, 4, 5}


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


def normalize_timeseries(items: list[dict]) -> list[dict]:
    normalized = []
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

        rpc_method = str(item.get("rpc_method", "")).strip() if item.get("rpc_method") else None
        if function_code == 5:
            if not rpc_method:
                raise HTTPException(status_code=400, detail="Method wajib diisi untuk Function 5.")

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

        normalized.append(
            {
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
                "rpc_method": rpc_method,
            }
        )
    return normalized


def sanitize_telemetry_value(value: object):
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        return value
    if isinstance(value, (list, tuple)):
        return [sanitize_telemetry_value(item) for item in value]
    if isinstance(value, bytes):
        return value.hex(" ")
    return str(value)


def check_tcp_online(ip: str, port: int) -> bool:
    """check TCP port to gateway"""
    try:
        with socket.create_connection((ip, port), timeout=1):
            return True
    except OSError:
        return False


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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
    if not modifier_on:
        return value
    if modifier_value is None:
        return value
    if modifier_type == "divider" and modifier_value != 0:
        return value / modifier_value
    if modifier_type == "multiplier":
        return value * modifier_value
    return value


def read_timeseries_value(job: sqlite3.Row):
    client = ModbusTcpClient(host=job["ip"], port=job["port"], timeout=1)
    try:
        if not client.connect():
            raise RuntimeError("Offline")

        address = max(job["register_address"] - 1, 0)
        count = max(job["object_count"], 1)
        unit_id = job["unit_id"]
        function_code = job["function_code"]

        if function_code == 3:
            result = client.read_holding_registers(
                address=address,
                count=count,
                device_id=unit_id,
            )
            if result.isError():
                raise RuntimeError(str(result))
            value = decode_registers(result.registers, job["data_type"])
        elif function_code == 4:
            result = client.read_input_registers(
                address=address,
                count=count,
                device_id=unit_id,
            )
            if result.isError():
                raise RuntimeError(str(result))
            value = decode_registers(result.registers, job["data_type"])
        elif function_code == 1:
            result = client.read_coils(
                address=address,
                count=count,
                device_id=unit_id,
            )
            if result.isError():
                raise RuntimeError(str(result))
            value = decode_bits(result.bits[:count], job["data_type"])
        elif function_code == 2:
            result = client.read_discrete_inputs(
                address=address,
                count=count,
                device_id=unit_id,
            )
            if result.isError():
                raise RuntimeError(str(result))
            value = decode_bits(result.bits[:count], job["data_type"])
        elif function_code == 5:
            # Polling write single coil: read using read_coils (FC 1)
            result = client.read_coils(
                address=address,
                count=count,
                device_id=unit_id,
            )
            if result.isError():
                raise RuntimeError(str(result))
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
    finally:
        try:
            client.close()
        except Exception:
            pass


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


def get_timeseries_jobs():
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT ts.id, ts.key_name, ts.data_type, ts.function_code,
                   ts.object_count, ts.register_address, ts.modifier_on,
                   ts.modifier_type, ts.modifier_value, ts.pull_on,
                   ts.report_period_ms, ts.device_id, d.unit_id, d.name AS device_name,
                   m.ip, m.port
            FROM timeseries ts
            JOIN devices d ON ts.device_id = d.id
            JOIN modbus_clients m ON d.modbus_id = m.id
            """
        ).fetchall()


def poller_loop() -> None:
    while True:
        now = time.time()
        try:
            jobs = get_timeseries_jobs()
        except Exception:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        for job in jobs:
            ts_id = int(job["id"])
            interval_ms = 1000
            if job["pull_on"]:
                interval_ms = int(job["report_period_ms"] or 1000)
            interval_sec = max(interval_ms / 1000.0, 0.2)

            next_run = timeseries_next_run.get(ts_id, 0)
            if now < next_run:
                continue

            try:
                value = read_timeseries_value(job)
                update_timeseries_result(ts_id, value, "ok")

                if mqtt_manager.connected and mqtt_manager.client:
                    device_name = job["device_name"]
                    key_name = str(job["key_name"])
                    parsed_val = parse_stored_value(value)
                    payload = {device_name: [{key_name: sanitize_telemetry_value(parsed_val)}]}
                    try:
                        mqtt_manager.client.publish("v1/gateway/telemetry", json.dumps(payload), qos=1)
                    except Exception:
                        pass
            except Exception as exc:
                update_timeseries_result(ts_id, None, f"error: {exc}")

            timeseries_next_run[ts_id] = now + interval_sec

        time.sleep(POLL_INTERVAL_SEC)


def start_poller() -> None:
    global poller_thread
    if poller_thread and poller_thread.is_alive():
        return
    poller_thread = threading.Thread(target=poller_loop, daemon=True)
    poller_thread.start()


def load_html() -> str:
    path = HTML_PATH if os.path.exists(HTML_PATH) else os.path.join(APP_DIR, "index.html")
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse(load_html())


@app.get("/settings", response_class=HTMLResponse)
def settings_view() -> HTMLResponse:
    return HTMLResponse(load_html())


@app.get("/api/settings")
def settings_read() -> JSONResponse:
    return JSONResponse(load_settings())


# --- Web Settings Endpoints ---

@app.post("/api/settings/save")
async def web_settings_save(request: Request) -> JSONResponse:
    payload = await request.json()
    tb_ip = str(payload.get("tb_ip", "")).strip()
    tb_port_raw = payload.get("tb_port", 8080)
    tb_user = str(payload.get("tb_user", "")).strip()
    tb_pass = str(payload.get("tb_pass", ""))

    if not tb_ip or not tb_port_raw:
        raise HTTPException(status_code=400, detail="IP dan Port wajib diisi.")

    try:
        tb_port = int(tb_port_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Port harus berupa angka valid.")

    save_web_settings({"tb_ip": tb_ip, "tb_port": tb_port, "tb_user": tb_user, "tb_pass": tb_pass})
    return JSONResponse({"ok": True, "message": "Web Settings tersimpan."})


@app.post("/api/settings/test")
async def web_settings_test(request: Request) -> JSONResponse:
    payload = await request.json()
    tb_ip = str(payload.get("tb_ip", "")).strip()
    tb_port_raw = payload.get("tb_port", 8080)
    tb_user = str(payload.get("tb_user", "")).strip()
    tb_pass = str(payload.get("tb_pass", ""))

    if not tb_ip or not tb_port_raw:
        raise HTTPException(status_code=400, detail="IP dan Port wajib diisi.")

    try:
        tb_port = int(tb_port_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Port harus berupa angka valid.")

    try:
        token = tb_get_bearer_token({"tb_ip": tb_ip, "tb_port": tb_port, "tb_user": tb_user, "tb_pass": tb_pass})
        if not token:
            raise RuntimeError("Token tidak diterima.")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Login Web HTTP gagal: {exc}") from exc

    return JSONResponse({"ok": True, "message": "Web HTTP login berhasil, token diterima!"})


# --- RPC Settings Endpoints ---

@app.post("/api/settings/rpc/save")
async def rpc_settings_save(request: Request) -> JSONResponse:
    payload = await request.json()
    rpc_ip = str(payload.get("rpc_ip", "")).strip()
    rpc_port_raw = payload.get("rpc_port", 1883)
    rpc_token = str(payload.get("rpc_token", "")).strip()

    if not rpc_ip or not rpc_token:
        raise HTTPException(status_code=400, detail="RPC IP dan Access Token wajib diisi.")

    try:
        rpc_port = int(rpc_port_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Port harus berupa angka valid.")

    save_rpc_settings({"rpc_ip": rpc_ip, "rpc_port": rpc_port, "rpc_token": rpc_token})
    return JSONResponse({"ok": True, "message": "RPC Settings tersimpan."})


@app.post("/api/settings/rpc/test")
async def rpc_settings_test(request: Request) -> JSONResponse:
    payload = await request.json()
    rpc_ip = str(payload.get("rpc_ip", "")).strip()
    rpc_port_raw = payload.get("rpc_port", 1883)
    rpc_token = str(payload.get("rpc_token", "")).strip()

    if not rpc_ip or not rpc_token:
        raise HTTPException(status_code=400, detail="RPC IP dan Access Token wajib diisi.")

    try:
        rpc_port = int(rpc_port_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Port harus berupa angka valid.")

    connect_event = threading.Event()
    result_code = [None]

    try:
        try:
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            client = mqtt.Client()

        client.username_pw_set(username=rpc_token)

        def test_on_connect(*args, **kwargs):
            rc = args[3]
            result_code[0] = getattr(rc, "value", rc)
            connect_event.set()

        client.on_connect = test_on_connect
        client.connect_async(rpc_ip, rpc_port, keepalive=10)
        client.loop_start()

        connected_ok = connect_event.wait(timeout=3.0)
        client.loop_stop()
        client.disconnect()

        if not connected_ok:
            raise HTTPException(status_code=400, detail="Koneksi timeout setelah 3 detik.")

        rc_val = result_code[0]
        if rc_val == 0:
            return JSONResponse({"ok": True, "message": "Koneksi MQTT RPC berhasil!"})
        else:
            raise HTTPException(status_code=400, detail=f"Koneksi MQTT ditolak (code {rc_val}). Periksa Access Token.")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=f"Gagal terhubung: {e}")


# --- RPC Logs Endpoints ---

@app.get("/api/rpc/logs")
def get_rpc_logs() -> JSONResponse:
    return JSONResponse({"logs": rpc_logs})


@app.post("/api/rpc/logs/clear")
def clear_rpc_logs() -> JSONResponse:
    global rpc_logs
    rpc_logs = []
    return JSONResponse({"ok": True, "message": "Log RPC terhapus."})


@app.post("/api/telemetry/push-all")
def push_all_telemetry_endpoint() -> JSONResponse:
    if not mqtt_manager.connected or not mqtt_manager.client:
        raise HTTPException(status_code=400, detail="MQTT Gateway belum terhubung. Periksa RPC Settings.")
    mqtt_manager.publish_all_telemetry()
    return JSONResponse({"ok": True, "message": "Telemetri semua device berhasil dikirim ke ThingsBoard!"})


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


@app.get("/api/modbus/{modbus_id}/devices")
def device_list(modbus_id: int) -> JSONResponse:
    devices = []
    for row in list_devices(modbus_id):
        ts_rows = []
        for ts in list_timeseries(row["id"]):
            ts_rows.append(
                {
                    "id": ts["id"],
                    "key_name": ts["key_name"],
                    "register_address": ts["register_address"],
                    "last_value": ts["last_value"],
                    "last_status": ts["last_status"],
                    "last_updated": ts["last_updated"],
                    "function_code": ts["function_code"],
                    "rpc_method": ts["rpc_method"],
                }
            )
        devices.append(
            {
                "id": row["id"],
                "modbus_id": row["modbus_id"],
                "name": row["name"],
                "unit_id": row["unit_id"],
                "timeseries": ts_rows,
            }
        )
    return JSONResponse({"items": devices})


@app.get("/api/devices/{device_id}")
def device_detail(device_id: int) -> JSONResponse:
    row = get_device(device_id)
    if not row:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan.")
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
                "rpc_method": ts["rpc_method"],
            }
        )
    return JSONResponse(
        {
            "id": row["id"],
            "modbus_id": row["modbus_id"],
            "name": row["name"],
            "unit_id": row["unit_id"],
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

    modbus_id = parse_int(payload.get("modbus_id"), "Modbus ID")
    name = str(payload.get("name", "")).strip()
    unit_id = parse_int(payload.get("unit_id"), "Unit ID")
    tb_token = str(payload.get("tb_token", "")).strip() or None
    timeseries_items = payload.get("timeseries", [])

    if not name:
        raise HTTPException(status_code=400, detail="Device name wajib diisi.")
    if unit_id <= 0:
        raise HTTPException(status_code=400, detail="Unit ID wajib > 0.")
    if not get_modbus_client(modbus_id):
        raise HTTPException(status_code=404, detail="Connector tidak ditemukan.")

    normalized = normalize_timeseries(timeseries_items)
    device_id = save_device(device_id, modbus_id, name, unit_id, tb_token, normalized)
    return JSONResponse({"ok": True, "id": device_id, "message": "Device tersimpan."})


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

    uvicorn.run("app_rpc:app", host="0.0.0.0", port=8000, reload=True)
