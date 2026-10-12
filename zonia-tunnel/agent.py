"""Agente Zonia para Home Assistant.

Con un código de activación (lo da el dashboard de Zonia) deja la casa
conectada sin pasos manuales:

  1. Canjea el código → recibe sus credenciales (una sola vez; quedan en /data).
  2. Túnel de acceso remoto (frpc) → https://<tenant>.<dominio>.
  3. Publica los sensores de energía al broker de Zonia por MQTT/TLS con el
     MISMO formato de topics que `mqtt_statestream` (el backend no cambia).
  4. Agrega el bloque `http:` a configuration.yaml si no existe (con backup,
     validación y rollback automático) y reinicia HA.
  5. Reporta su estado al dashboard (heartbeat).

Modo legado: sin código pero con tenant_id + token, solo hace el túnel (2).
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

import paho.mqtt.client as mqtt
import websocket

VERSION = "2.0.0"
DATA_DIR = os.environ.get("ZONIA_DATA_DIR", "/data")
OPTIONS_PATH = f"{DATA_DIR}/options.json"
CREDS_PATH = f"{DATA_DIR}/credentials.json"
STATE_PATH = f"{DATA_DIR}/state.json"
CA_PATH = "/etc/zonia/ca.crt"
FRPC_CONF = "/tmp/frpc.toml"
HA_CONFIG = os.environ.get("ZONIA_HA_CONFIG", "/homeassistant/configuration.yaml")

FUNCTIONS_BASE = os.environ.get(
    "ZONIA_FUNCTIONS_BASE", "https://us-central1-zonia-2026-experimental.cloudfunctions.net"
)
SUPERVISOR = os.environ.get("ZONIA_SUPERVISOR_URL", "http://supervisor")
DEFAULT_SERVER = "35.253.76.219"
DEFAULT_SERVER_PORT = 7000
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

# Mismos filtros que el bloque mqtt_statestream estándar de Zonia.
ENTITY_GLOBS = [
    "sensor.*power*", "sensor.*potencia*", "sensor.*energy*", "sensor.*energia*",
    "sensor.*kwh*", "sensor.*voltage*", "sensor.*voltaje*", "sensor.*tension*",
    "sensor.*current*", "sensor.*corriente*",
]
ATTRIBUTES = ("friendly_name", "unit_of_measurement", "device_class")

HTTP_BLOCK = """
# >>> zonia: agregado por el add-on Zonia ({date}). Backup: {backup}
http:
  use_x_forwarded_for: true
  trusted_proxies:
    - 172.30.32.0/23   # add-ons del Supervisor (túnel Zonia)
    - 10.250.0.0/24    # proxies del servidor Zonia
  ip_ban_enabled: true
  login_attempts_threshold: 5
# <<< zonia
"""

status = {"tunnel": "starting", "mqtt": "disabled", "sensors": 0, "http_config": "pending"}


def log(msg: str) -> None:
    print(f"[zonia] {msg}", flush=True)


# ── utilidades ───────────────────────────────────────────────────
def read_json(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_json_private(path: str, data: dict) -> None:
    tmp = f"{path}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def http_json(method: str, url: str, body: dict | None = None, token: str | None = None,
              timeout: int = 30) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except json.JSONDecodeError:
            return e.code, {}


def supervisor(method: str, path: str, body: dict | None = None, timeout: int = 120):
    return http_json(method, f"{SUPERVISOR}{path}", body, SUPERVISOR_TOKEN, timeout)


# ── 1. credenciales ──────────────────────────────────────────────
def load_credentials(opts: dict) -> dict:
    creds = read_json(CREDS_PATH)
    code = (opts.get("activation_code") or "").strip()
    code_sha = hashlib.sha256(code.upper().encode()).hexdigest() if code else ""

    if code and code_sha != creds.get("code_sha"):
        log("Canjeando código de activación…")
        st, resp = http_json("POST", f"{FUNCTIONS_BASE}/activate", {"code": code})
        if st != 200:
            raise SystemExit(f"Activación rechazada ({st}): {resp.get('error', 'error desconocido')}")
        creds = {**resp, "code_sha": code_sha, "source": "activation"}
        write_json_private(CREDS_PATH, creds)
        log(f"Activado como '{creds['tenant_id']}'.")
        clear_activation_code(opts)
        return creds

    if creds.get("tenant_id"):
        return creds

    # Modo legado (alta manual): solo túnel.
    if opts.get("tenant_id") and opts.get("token"):
        return {
            "tenant_id": opts["tenant_id"],
            "tunnel": {"server": opts.get("server") or DEFAULT_SERVER,
                       "port": opts.get("server_port") or DEFAULT_SERVER_PORT,
                       "token": opts["token"]},
            "source": "manual",
        }
    raise SystemExit("Falta el código de activación: pegalo en la pestaña Configuración y reiniciá el add-on.")


def clear_activation_code(opts: dict) -> None:
    """Borra el código de las opciones (ya se usó; evita confusiones)."""
    new_opts = {k: v for k, v in opts.items() if k != "activation_code"}
    new_opts["activation_code"] = ""
    st, _ = supervisor("POST", "/addons/self/options", {"options": new_opts})
    if st != 200:
        log("Aviso: no se pudo limpiar el código de las opciones (no afecta el funcionamiento).")


# ── 2. túnel ─────────────────────────────────────────────────────
def write_frpc_conf(creds: dict, opts: dict) -> None:
    t = creds["tunnel"]
    tenant = creds["tenant_id"]
    server = t.get("server") or DEFAULT_SERVER
    port = int(t.get("port") or DEFAULT_SERVER_PORT)
    conf = f'''serverAddr = "{server}"
serverPort = {port}
user = "{tenant}"
loginFailExit = false
transport.tls.enable = true
transport.tls.trustedCaFile = "{CA_PATH}"
transport.tls.serverName = "{server}"
metadatas.tenant = "{tenant}"
metadatas.token = "{t["token"]}"
log.to = "console"
log.level = "info"

[[proxies]]
name = "ha"
type = "http"
localIP = "homeassistant"
localPort = 8123
subdomain = "{tenant}"
'''
    fd = os.open(FRPC_CONF, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(conf)


def run_tunnel() -> None:
    while True:
        proc = subprocess.Popen(["frpc", "-c", FRPC_CONF], stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:  # type: ignore[union-attr]
            line = re.sub(r"\x1b\[[0-9;]*m", "", line).rstrip()
            print(f"[túnel] {line}", flush=True)
            if "start proxy success" in line:
                status["tunnel"] = "ok"
            elif "error" in line.lower() and "proxy" in line.lower() or "login to the server failed" in line:
                status["tunnel"] = "error: " + line[-150:]
        proc.wait()
        status["tunnel"] = "restarting"
        log("El túnel se cerró; reintentando en 10 s…")
        time.sleep(10)


# ── 3. sensores → MQTT ───────────────────────────────────────────
def entity_matches(entity_id: str) -> bool:
    return any(fnmatch.fnmatchcase(entity_id, g) for g in ENTITY_GLOBS)


class Publisher:
    def __init__(self, creds: dict) -> None:
        m = creds["mqtt"]
        self.tenant = creds["tenant_id"]
        self.attr_cache: dict[tuple[str, str], object] = {}
        self.entities: set[str] = set()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=f"zonia-agent-{self.tenant}")
        self.client.username_pw_set(m["username"], m["password"])
        self.client.tls_set(ca_certs=CA_PATH)  # valida el cert del broker (SAN = IP)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=5, max_delay=120)
        self.client.connect_async(m["host"], int(m["port"]), keepalive=60)
        self.client.loop_start()

    def _on_connect(self, _c, _u, _f, reason, _p) -> None:
        if reason == 0:
            status["mqtt"] = "connected"
            log("MQTT conectado al broker de Zonia.")
            self.attr_cache.clear()  # re-publicar metadatos tras reconectar
        else:
            # Recién activado: el provisioner tarda unos segundos en dar de alta el usuario.
            status["mqtt"] = f"error: {reason}"
            log(f"MQTT rechazado ({reason}); reintentando…")

    def _on_disconnect(self, _c, _u, _f, reason, _p) -> None:
        status["mqtt"] = "disconnected"

    def publish_state(self, st: dict | None) -> None:
        if not st or not entity_matches(st.get("entity_id", "")):
            return
        eid = st["entity_id"]
        self.entities.add(eid)
        status["sensors"] = len(self.entities)
        domain, obj = eid.split(".", 1)
        base = f"tenant/{self.tenant}/homeassistant/{domain}/{obj}"
        self.client.publish(f"{base}/state", str(st.get("state")), qos=1, retain=True)
        attrs = st.get("attributes") or {}
        for k in ATTRIBUTES:
            v = attrs.get(k)
            if v is not None and self.attr_cache.get((eid, k)) != v:
                self.client.publish(f"{base}/{k}", json.dumps(v), qos=1, retain=True)
                self.attr_cache[(eid, k)] = v

    def run_forever(self) -> None:
        while True:
            try:
                self._session()
            except Exception as exc:  # noqa: BLE001 - reconectar ante cualquier corte
                log(f"Conexión con Home Assistant perdida ({exc}); reintentando en 10 s…")
            time.sleep(10)

    def _session(self) -> None:
        ws = websocket.create_connection(f"ws://supervisor/core/websocket", timeout=30)
        try:
            json.loads(ws.recv())  # auth_required
            ws.send(json.dumps({"type": "auth", "access_token": SUPERVISOR_TOKEN}))
            if json.loads(ws.recv()).get("type") != "auth_ok":
                raise RuntimeError("auth con Home Assistant rechazada")
            ws.send(json.dumps({"id": 1, "type": "get_states"}))
            ws.send(json.dumps({"id": 2, "type": "subscribe_events", "event_type": "state_changed"}))
            ws.settimeout(60)
            msg_id, idle = 100, 0
            while True:
                try:
                    msg = json.loads(ws.recv())
                    idle = 0
                except websocket.WebSocketTimeoutException:
                    idle += 1
                    if idle > 2:
                        raise RuntimeError("sin respuesta de Home Assistant")
                    msg_id += 1
                    ws.send(json.dumps({"id": msg_id, "type": "ping"}))
                    continue
                if msg.get("id") == 1 and msg.get("type") == "result":
                    for st in msg.get("result") or []:
                        self.publish_state(st)
                    log(f"Publicando {status['sensors']} sensores de energía.")
                elif msg.get("type") == "event":
                    self.publish_state(((msg.get("event") or {}).get("data") or {}).get("new_state"))
        finally:
            ws.close()


# ── 4. configuration.yaml ────────────────────────────────────────
def core_api_up() -> bool:
    try:
        st, _ = supervisor("GET", "/core/api/", timeout=10)
        return st == 200
    except OSError:
        return False


def atomic_write_like(path: str, text: str, like: str) -> None:
    tmp = f"{path}.zonia-tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    shutil.copymode(like, tmp)
    os.replace(tmp, path)


def ensure_http_config() -> str:
    """Agrega el bloque http: SOLO si no existe ninguno. Medidas de seguridad:
    backup con fecha, escritura atómica, validación con `ha core check`,
    rollback si la validación falla o si HA no vuelve a levantar."""
    state = read_json(STATE_PATH)
    if state.get("http_config") in ("patched", "already"):
        return state["http_config"]
    if not os.path.exists(HA_CONFIG):
        return "manual: no se encontró configuration.yaml"

    with open(HA_CONFIG, encoding="utf-8") as f:
        original = f.read()

    # Ya hay un bloque http: (top-level). Nunca lo tocamos: solo verificamos.
    if re.search(r"^http\s*:", original, re.M):
        if "10.250.0.0/24" in original and "172.30.32.0/23" in original:
            result = "already"
        else:
            log("configuration.yaml ya tiene un bloque http: propio. NO se modifica.")
            log("Agregá a mano en http: use_x_forwarded_for: true y en trusted_proxies: "
                "172.30.32.0/23 y 10.250.0.0/24, y reiniciá HA.")
            result = "manual: ya existe un bloque http: (completar trusted_proxies a mano)"
        state["http_config"] = result
        write_json_private(STATE_PATH, state)
        return result

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = f"{HA_CONFIG}.zonia-backup-{stamp}"
    shutil.copy2(HA_CONFIG, backup)
    patched = original.rstrip("\n") + "\n" + HTTP_BLOCK.format(
        date=stamp, backup=os.path.basename(backup))

    def rollback(reason: str) -> str:
        atomic_write_like(HA_CONFIG, original, backup)
        log(f"Cambio revertido ({reason}). configuration.yaml quedó como estaba.")
        return f"error: {reason}"

    atomic_write_like(HA_CONFIG, patched, backup)
    log(f"Bloque http: agregado (backup en {os.path.basename(backup)}). Validando configuración…")

    st, resp = supervisor("POST", "/core/check", timeout=300)
    if st != 200 or resp.get("result") != "ok":
        result = rollback(f"la validación falló: {resp.get('message', st)}")
        state["http_config"] = result
        write_json_private(STATE_PATH, state)
        return result

    log("Configuración válida. Reiniciando Home Assistant…")
    supervisor("POST", "/core/restart", timeout=600)
    deadline = time.time() + 600
    time.sleep(20)
    while time.time() < deadline and not core_api_up():
        time.sleep(15)
    if not core_api_up():
        result = rollback("Home Assistant no volvió a levantar en 10 min")
        supervisor("POST", "/core/restart", timeout=600)
    else:
        result = "patched"
        log("Home Assistant reiniciado con el acceso remoto configurado.")
    state["http_config"] = result
    write_json_private(STATE_PATH, state)
    return result


def ensure_external_url(url: str | None) -> str:
    """Pone la URL de Internet de HA = la del túnel, solo si está vacía."""
    if not url:
        return ""
    try:
        ws = websocket.create_connection("ws://supervisor/core/websocket", timeout=30)
        try:
            ws.recv()
            ws.send(json.dumps({"type": "auth", "access_token": SUPERVISOR_TOKEN}))
            ws.recv()
            ws.send(json.dumps({"id": 1, "type": "get_config"}))
            current = (json.loads(ws.recv()).get("result") or {}).get("external_url")
            if current:
                return current
            ws.send(json.dumps({"id": 2, "type": "config/core/update", "external_url": url}))
            if json.loads(ws.recv()).get("success"):
                log(f"URL de Internet configurada: {url}")
                return url
        finally:
            ws.close()
    except Exception as exc:  # noqa: BLE001 - es opcional
        log(f"Aviso: no se pudo configurar la URL de Internet ({exc}).")
    return ""


# ── 5. heartbeat ─────────────────────────────────────────────────
def heartbeat_loop(creds: dict, extra: dict) -> None:
    while True:
        body = {"tenant_id": creds["tenant_id"], "token": creds["tunnel"]["token"],
                "version": VERSION, **status, **extra}
        try:
            http_json("POST", f"{FUNCTIONS_BASE}/agentStatus", body, timeout=20)
        except OSError:
            pass
        time.sleep(300)


def main() -> None:
    opts = read_json(OPTIONS_PATH)
    creds = load_credentials(opts)
    log(f"Zonia {VERSION} — cliente '{creds['tenant_id']}'")

    write_frpc_conf(creds, opts)
    threading.Thread(target=run_tunnel, daemon=True).start()

    extra: dict = {}
    if creds.get("source") == "activation":
        publisher = Publisher(creds)
        threading.Thread(target=publisher.run_forever, daemon=True).start()
        threading.Thread(target=heartbeat_loop, args=(creds, extra), daemon=True).start()

    status["http_config"] = ensure_http_config()
    extra["http_detail"] = status["http_config"]
    extra["external_url"] = ensure_external_url(creds.get("url"))

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
