# tbgateway2 - Service & Port Mapping

## Systemd Services

| Service | Port | Description |
|---------|------|-------------|
| `tbgateway.service` | 8000 | FastAPI app - Modbus to ThingsBoard gateway |
| `thingsboard.service` | 8080 | ThingsBoard Web UI & REST API |
| `thingsboard.service` | 1883 | ThingsBoard MQTT |
| `thingsboard.service` | 7070 | ThingsBoard gRPC/Edge |
| `ssh.service` | 22 | SSH |
| `postgresql@18-main.service` | 5432 | PostgreSQL database |

## Docker Containers

| Container | Port | Image |
|-----------|------|-------|
| `tb-gateway` | 5000 | `thingsboard/tb-gateway:3.8-stable` |

## Service Files

| Service | Config Path |
|---------|-------------|
| tbgateway | `/etc/systemd/system/tbgateway.service` |
| thingsboard | `/usr/lib/systemd/system/thingsboard.service` |
