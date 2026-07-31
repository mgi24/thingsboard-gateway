import json
import os
import sqlite3
import threading
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "rpc.db")
HTML_PATH = os.path.join(APP_DIR, "rpcindex.html")

app = FastAPI(title="ThingsBoard RPC Gateway")

# -----------------------------------------------------------------------------
# Database Utility Functions
# -----------------------------------------------------------------------------

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
                tb_ip TEXT NOT NULL,
                tb_port INTEGER NOT NULL,
                tb_token TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS variables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 1,
                FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE,
                UNIQUE(device_id, name)
            )
            """
        )
        # Ensure rpc_method column exists
        try:
            conn.execute("ALTER TABLE variables ADD COLUMN rpc_method TEXT")
        except sqlite3.OperationalError:
            pass # Already exists
        conn.commit()


def load_settings() -> dict:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT tb_ip, tb_port, tb_token FROM settings WHERE id = 1"
        ).fetchone()
    if not row:
        return {"tb_ip": "", "tb_port": 1883, "tb_token": ""}
    return {
        "tb_ip": row["tb_ip"],
        "tb_port": row["tb_port"],
        "tb_token": row["tb_token"],
    }


def save_settings_db(tb_ip: str, tb_port: int, tb_token: str) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO settings (id, tb_ip, tb_port, tb_token)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                tb_ip = excluded.tb_ip,
                tb_port = excluded.tb_port,
                tb_token = excluded.tb_token
            """,
            (tb_ip, tb_port, tb_token),
        )
        conn.commit()


def list_devices_with_variables() -> list:
    devices_list = []
    with db_connect() as conn:
        devices = conn.execute("SELECT id, name FROM devices ORDER BY name ASC").fetchall()
        for d in devices:
            variables = conn.execute(
                "SELECT id, name, value, rpc_method FROM variables WHERE device_id = ? ORDER BY name ASC",
                (d["id"],),
            ).fetchall()
            
            devices_list.append({
                "id": d["id"],
                "name": d["name"],
                "variables": [{"id": v["id"], "name": v["name"], "value": v["value"], "rpc_method": v["rpc_method"]} for v in variables]
            })
    return devices_list


# -----------------------------------------------------------------------------
# Telemetry and RPC Logic Helpers
# -----------------------------------------------------------------------------

def clean_name(name: str) -> str:
    n = name.lower()
    if n.startswith("set"):
        n = n[3:]
    return "".join(c for c in n if c.isalnum())


def extract_param_value(params, var_name: str):
    if isinstance(params, dict):
        cleaned_var = clean_name(var_name)
        for k, v in params.items():
            if clean_name(k) == cleaned_var:
                return v
        if "value" in params:
            return params["value"]
        if "params" in params:
            return params["params"]
        if len(params) == 1:
            return list(params.values())[0]
        return None
    return params


def parse_int_value(val) -> int:
    if val is None:
        return 1
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val_lower = val.lower().strip()
        if val_lower in ("true", "on", "yes", "active"):
            return 1
        if val_lower in ("false", "off", "no", "inactive"):
            return 0
        try:
            return int(float(val))
        except ValueError:
            pass
    return 1


# -----------------------------------------------------------------------------
# Background MQTT Thread Manager
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
                # 1. Fetch current settings from DB
                settings = None
                with db_connect() as conn:
                    row = conn.execute("SELECT tb_ip, tb_port, tb_token FROM settings WHERE id = 1").fetchone()
                    if row:
                        settings = (row["tb_ip"], int(row["tb_port"]), row["tb_token"])

                # If settings have changed, tear down old client
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

                # If settings are valid and we are not connected, attempt connection
                if self.active_settings and not self.client:
                    ip, port, token = self.active_settings
                    if ip and port and token:
                        try:
                            import paho.mqtt.client as mqtt
                            # API compatibility for newer paho-mqtt versions
                            try:
                                client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
                            except AttributeError:
                                client = mqtt.Client()

                            client.username_pw_set(username=token)

                            def on_connect(*args, **kwargs):
                                # args: client, userdata, flags, rc, [properties]
                                client = args[0]
                                rc = args[3]
                                rc_val = getattr(rc, "value", rc)
                                if rc_val == 0:
                                    print("[MQTT] Successfully connected to ThingsBoard broker.")
                                    self.connected = True
                                    client.subscribe("v1/gateway/rpc")
                                else:
                                    print(f"[MQTT] Connection rejected, rc = {rc_val}")
                                    self.connected = False

                            def on_disconnect(*args, **kwargs):
                                print("[MQTT] Connection disconnected.")
                                self.connected = False

                            def on_message(*args, **kwargs):
                                # args: client, userdata, msg
                                client = args[0]
                                msg = args[2]
                                if msg.topic == "v1/gateway/rpc":
                                    try:
                                        payload = json.loads(msg.payload.decode("utf-8"))
                                        handle_rpc_command(client, payload)
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

                # 2. Publish telemetry combined payload if connected
                if self.connected and self.client:
                    self.publish_all_telemetry()

            except Exception as ex:
                print(f"[MQTT] Error in background manager thread: {ex}")

            time.sleep(1.0)

    def publish_all_telemetry(self):
        try:
            payload = {}
            with db_connect() as conn:
                devices = conn.execute("SELECT id, name FROM devices").fetchall()
                for d in devices:
                    variables = conn.execute(
                        "SELECT name, value FROM variables WHERE device_id = ?",
                        (d["id"],),
                    ).fetchall()
                    if variables:
                        device_payload = {v["name"]: v["value"] for v in variables}
                        payload[d["name"]] = [device_payload]

            if payload:
                self.client.publish("v1/gateway/telemetry", json.dumps(payload), qos=1)
        except Exception as ex:
            print(f"[MQTT] Telemetry send failed: {ex}")


def handle_rpc_command(client, payload: dict) -> None:
    device_name = payload.get("device")
    data = payload.get("data", {})
    rpc_id = payload.get("id") if payload.get("id") is not None else data.get("id")
    method = data.get("method")
    params = data.get("params")

    if not device_name or not method or rpc_id is None:
        print(f"[RPC] Error: Invalid RPC request envelope. Request dump: {json.dumps(payload, indent=2)}")
        return

    print(f"[RPC] Command received for '{device_name}' (ID={rpc_id}): method='{method}', params={params}")

    with db_connect() as conn:
        # Find device record
        device_row = conn.execute(
            "SELECT id FROM devices WHERE lower(name) = lower(?)", (device_name,)
        ).fetchone()
        if not device_row:
            print(f"[RPC FAILURE] Device '{device_name}' not defined in local gateway database. Request dump: {json.dumps(payload, indent=2)}")
            return

        device_id = device_row["id"]
        
        # Get variable rows
        variables = conn.execute(
            "SELECT id, name, value, rpc_method FROM variables WHERE device_id = ?", (device_id,)
        ).fetchall()

        matched_var = None
        for v in variables:
            v_rpc = v["rpc_method"]
            if v_rpc and v_rpc.strip().lower() == method.strip().lower():
                matched_var = v
                break

        if not matched_var:
            print(f"[RPC FAILURE] No matching variable with RPC method '{method}' on device '{device_name}'. Request dump: {json.dumps(payload, indent=2)}")
            return

        # Extract value and convert
        raw_val = extract_param_value(params, matched_var["name"])
        new_val = parse_int_value(raw_val)

        # Update variable in database
        conn.execute(
            "UPDATE variables SET value = ? WHERE id = ?", (new_val, matched_var["id"])
        )
        conn.commit()
        print(f"[RPC] Variable '{matched_var['name']}' updated to {new_val} via ThingsBoard command.")

        # Reply with status success acknowledgment
        response_payload = {
            "device": device_name,
            "id": rpc_id,
            "data": {
                "success": True,
                "message": f"Variable '{matched_var['name']}' updated to {new_val}."
            }
        }
        client.publish("v1/gateway/rpc", json.dumps(response_payload), qos=1)
        print(f"[RPC] Sent reply to ThingsBoard for Request ID {rpc_id}.")


# Initialize global background thread manager
mqtt_manager = MqttManager()

# -----------------------------------------------------------------------------
# FastAPI Startup / Shutdown Events
# -----------------------------------------------------------------------------

@app.on_event("startup")
def on_startup() -> None:
    init_db()
    mqtt_manager.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    mqtt_manager.running = False
    if mqtt_manager.client:
        try:
            mqtt_manager.client.loop_stop()
            mqtt_manager.client.disconnect()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# HTTP Endpoints (FastAPI)
# -----------------------------------------------------------------------------

def load_html() -> str:
    with open(HTML_PATH, "r", encoding="utf-8") as handle:
        return handle.read()


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse(load_html())


@app.get("/api/state")
def get_state() -> JSONResponse:
    state = {
      "settings": load_settings(),
      "mqtt_connected": mqtt_manager.connected,
      "devices": list_devices_with_variables()
    }
    return JSONResponse(state)


@app.post("/api/settings/save")
async def save_settings_endpoint(request: Request) -> JSONResponse:
    payload = await request.json()
    tb_ip = str(payload.get("tb_ip", "")).strip()
    tb_port_raw = payload.get("tb_port", 1883)
    tb_token = str(payload.get("tb_token", "")).strip()

    if not tb_ip or not tb_token:
        raise HTTPException(status_code=400, detail="Thingsboard host IP and access token are required.")
    
    try:
        tb_port = int(tb_port_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Port must be a valid number.")

    save_settings_db(tb_ip, tb_port, tb_token)
    return JSONResponse({"ok": True, "message": "Settings saved successfully."})


@app.post("/api/settings/test")
async def settings_test_endpoint(request: Request) -> JSONResponse:
    payload = await request.json()
    tb_ip = str(payload.get("tb_ip", "")).strip()
    tb_port_raw = payload.get("tb_port", 1883)
    tb_token = str(payload.get("tb_token", "")).strip()

    if not tb_ip or not tb_token:
        raise HTTPException(status_code=400, detail="Thingsboard host IP and access token are required.")

    try:
        tb_port = int(tb_port_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Port must be a valid number.")

    import paho.mqtt.client as mqtt
    
    connect_event = threading.Event()
    result_code = [None]
    
    try:
        try:
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            client = mqtt.Client()
        
        client.username_pw_set(username=tb_token)
        
        def test_on_connect(*args, **kwargs):
            rc = args[3]
            result_code[0] = getattr(rc, "value", rc)
            connect_event.set()

        client.on_connect = test_on_connect
        
        # Connect asynchronously
        client.connect_async(tb_ip, tb_port, keepalive=10)
        client.loop_start()
        
        # Wait up to 3 seconds for the connect event to fire
        connected_ok = connect_event.wait(timeout=3.0)
        client.loop_stop()
        client.disconnect()
        
        if not connected_ok:
            raise HTTPException(status_code=400, detail="Connection timed out after 3 seconds.")
            
        rc_val = result_code[0]
        if rc_val == 0:
            return JSONResponse({"ok": True, "message": "MQTT connection successful!"})
        else:
            raise HTTPException(status_code=400, detail=f"MQTT connection rejected (code {rc_val}). Check your token.")
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=f"Failed to connect: {e}")


@app.post("/api/devices/add")
async def add_device_endpoint(request: Request) -> JSONResponse:
    payload = await request.json()
    name = str(payload.get("name", "")).strip()
    if not name:
        raise HTTPException(status_code=400, detail="Device name cannot be blank.")

    try:
        with db_connect() as conn:
            conn.execute("INSERT INTO devices (name) VALUES (?)", (name,))
            conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Device with this name already exists.")

    return JSONResponse({"ok": True})


@app.post("/api/devices/{device_id}/delete")
def delete_device_endpoint(device_id: int) -> JSONResponse:
    with db_connect() as conn:
        conn.execute("DELETE FROM devices WHERE id = ?", (device_id,))
        conn.commit()
    return JSONResponse({"ok": True})


@app.post("/api/variables/add")
async def add_variable_endpoint(request: Request) -> JSONResponse:
    payload = await request.json()
    device_id_raw = payload.get("device_id")
    name = str(payload.get("name", "")).strip()
    rpc_method = str(payload.get("rpc_method", "")).strip()

    if not device_id_raw or not name:
        raise HTTPException(status_code=400, detail="Target device and variable name are required.")

    try:
        device_id = int(device_id_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Device ID must be integer.")

    try:
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO variables (device_id, name, value, rpc_method) VALUES (?, ?, 1, ?)",
                (device_id, name, rpc_method),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Variable name already registered for this device.")

    return JSONResponse({"ok": True})


@app.post("/api/variables/{var_id}/update")
async def update_variable_endpoint(var_id: int, request: Request) -> JSONResponse:
    payload = await request.json()
    name = str(payload.get("name", "")).strip()
    rpc_method = str(payload.get("rpc_method", "")).strip()

    if not name:
        raise HTTPException(status_code=400, detail="Variable name cannot be blank.")

    try:
        with db_connect() as conn:
            conn.execute(
                "UPDATE variables SET name = ?, rpc_method = ? WHERE id = ?",
                (name, rpc_method, var_id),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Variable name already exists for this device.")

    return JSONResponse({"ok": True})


@app.post("/api/variables/{var_id}/increment")
def increment_variable_endpoint(var_id: int) -> JSONResponse:
    with db_connect() as conn:
        row = conn.execute("SELECT value FROM variables WHERE id = ?", (var_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Variable not found.")
        new_value = row["value"] + 1
        conn.execute("UPDATE variables SET value = ? WHERE id = ?", (new_value, var_id))
        conn.commit()
    return JSONResponse({"ok": True})


@app.post("/api/variables/{var_id}/decrement")
def decrement_variable_endpoint(var_id: int) -> JSONResponse:
    with db_connect() as conn:
        row = conn.execute("SELECT value FROM variables WHERE id = ?", (var_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Variable not found.")
        new_value = row["value"] - 1
        conn.execute("UPDATE variables SET value = ? WHERE id = ?", (new_value, var_id))
        conn.commit()
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    uvicorn.run("rpcgateway:app", host="0.0.0.0", port=8001, reload=True)
