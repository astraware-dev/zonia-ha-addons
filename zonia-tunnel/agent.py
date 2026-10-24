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
  6. Envía el inventario de equipos eléctricos instalados (registros de
     dispositivos/entidades/áreas de HA) → "Equipos instalados" en el dashboard.
  7. Envía el consumo por hora de cada sensor de energía según las estadísticas
     de HA (recorder). Es lo que usa Zonia para calcular: no depende de que
     llegue cada evento por MQTT y recupera los huecos.

Modo legado: sin código pero con tenant_id + token, solo hace el túnel (2).
"""
from __future__ import annotations

import fnmatch
import hashlib
import itertools
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import websocket

VERSION = "2.2.0"
DATA_DIR = os.environ.get("ZONIA_DATA_DIR", "/data")
OPTIONS_PATH = f"{DATA_DIR}/options.json"
CREDS_PATH = f"{DATA_DIR}/credentials.json"
STATE_PATH = f"{DATA_DIR}/state.json"
STATS_PATH = f"{DATA_DIR}/stats.json"
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

# Inventario: una entidad es "eléctrica" por su device_class o, si no tiene,
# por su unidad. No depende del nombre (a diferencia de ENTITY_GLOBS).
ELECTRIC_CLASSES = ("power", "energy", "voltage", "current")
ELECTRIC_UNITS = {"W": "power", "kW": "power", "Wh": "energy", "kWh": "energy",
                  "MWh": "energy", "V": "voltage", "A": "current", "mA": "current"}
REGISTRIES = ("device", "entity", "area")
INVENTORY_QUIET_S = 10        # esperar a que HA deje de cambiar los registros
INVENTORY_RESEND_S = 24 * 3600  # reenvío diario aunque no cambie (frescura)
INVENTORY_RETRY_S = 300

# Estadísticas de energía: HA solo las guarda para sensores con state_class.
STATS_STATE_CLASSES = ("total", "total_increasing")
STATS_EVERY_S = 300
STATS_BACKFILL_H = 31 * 24  # primera vez: el último mes (cubre el periodo de facturación)
STATS_CHUNK_H = 24          # horas por envío al backfillear
HOUR_FMT = "%Y-%m-%dT%H:00"  # UTC, misma clave que history/ en el backend

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

status = {"tunnel": "starting", "mqtt": "disabled", "sensors": 0, "http_config": "pending",
          "stats": "pending"}


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


def electric_metric(attrs: dict) -> str | None:
    dc = attrs.get("device_class")
    if dc in ELECTRIC_CLASSES:
        return dc
    return ELECTRIC_UNITS.get(attrs.get("unit_of_measurement") or "")


class Publisher:
    def __init__(self, creds: dict) -> None:
        m = creds["mqtt"]
        self.tenant = creds["tenant_id"]
        self.token = creds["tunnel"]["token"]
        self.attr_cache: dict[tuple[str, str], object] = {}
        self.entities: set[str] = set()
        # Inventario: entidades eléctricas vivas + registros de HA.
        self.lock = threading.Lock()
        self.electric: dict[str, dict] = {}
        self.registry: dict[str, list] = {}
        self.states_loaded = False
        self.inv_changed_at = 0.0
        self.inv_dirty = False
        self.inv_hash = ""
        self.inv_sent_at = 0.0
        self.inv_next_try = 0.0
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

    def track(self, eid: str, st: dict | None) -> None:
        """Mantiene el set de entidades eléctricas para el inventario."""
        info = None
        if st and eid.startswith("sensor."):
            attrs = st.get("attributes") or {}
            metric = electric_metric(attrs)
            if metric:
                info = {"metric": metric, "unit": attrs.get("unit_of_measurement"),
                        "name": attrs.get("friendly_name"),
                        "state_class": attrs.get("state_class")}
        with self.lock:
            if self.electric.get(eid) == info:
                return
            if info is None:
                self.electric.pop(eid, None)
            else:
                self.electric[eid] = info
            self._mark_dirty()

    def _mark_dirty(self) -> None:
        self.inv_dirty = True
        self.inv_changed_at = time.time()

    def _session(self) -> None:
        ws = websocket.create_connection(f"ws://supervisor/core/websocket", timeout=30)
        ids = itertools.count(100)
        pending: dict[int, str] = {}  # id de request → registro pedido

        def ask_registry(kind: str) -> None:
            if kind in pending.values():
                return  # ya hay uno en vuelo: alcanza con esa respuesta
            mid = next(ids)
            pending[mid] = kind
            ws.send(json.dumps({"id": mid, "type": f"config/{kind}_registry/list"}))

        try:
            json.loads(ws.recv())  # auth_required
            ws.send(json.dumps({"type": "auth", "access_token": SUPERVISOR_TOKEN}))
            if json.loads(ws.recv()).get("type") != "auth_ok":
                raise RuntimeError("auth con Home Assistant rechazada")
            ws.send(json.dumps({"id": 1, "type": "get_states"}))
            ws.send(json.dumps({"id": 2, "type": "subscribe_events", "event_type": "state_changed"}))
            for i, kind in enumerate(REGISTRIES, start=3):
                ws.send(json.dumps({"id": i, "type": "subscribe_events",
                                    "event_type": f"{kind}_registry_updated"}))
                ask_registry(kind)
            ws.settimeout(60)
            idle = 0
            while True:
                try:
                    msg = json.loads(ws.recv())
                    idle = 0
                except websocket.WebSocketTimeoutException:
                    idle += 1
                    if idle > 2:
                        raise RuntimeError("sin respuesta de Home Assistant")
                    ws.send(json.dumps({"id": next(ids), "type": "ping"}))
                    continue
                if msg.get("id") == 1 and msg.get("type") == "result":
                    with self.lock:
                        self.electric.clear()  # lo borrado mientras no estábamos, afuera
                    for st in msg.get("result") or []:
                        self.publish_state(st)
                        self.track(st.get("entity_id", ""), st)
                    with self.lock:
                        self.states_loaded = True
                        self._mark_dirty()
                    log(f"Publicando {status['sensors']} sensores de energía.")
                elif msg.get("type") == "result" and msg.get("id") in pending:
                    kind = pending.pop(msg["id"])
                    if msg.get("success"):
                        with self.lock:
                            self.registry[kind] = msg.get("result") or []
                            self._mark_dirty()
                    else:
                        log(f"Aviso: no se pudo leer el registro de {kind} ({msg.get('error')}).")
                elif msg.get("type") == "event":
                    ev = msg.get("event") or {}
                    etype = ev.get("event_type", "")
                    if etype.endswith("_registry_updated"):
                        ask_registry(etype.removesuffix("_registry_updated"))
                        continue
                    data = ev.get("data") or {}
                    self.publish_state(data.get("new_state"))
                    self.track(data.get("entity_id", ""), data.get("new_state"))
        finally:
            ws.close()

    # ── 6. inventario ────────────────────────────────────────────
    def build_inventory(self) -> dict:
        """Equipos (dispositivos de HA) con al menos una entidad eléctrica.

        Llamar con self.lock tomado.
        """
        areas = {a.get("area_id"): a.get("name") for a in self.registry["area"]}
        devices = {d.get("id"): d for d in self.registry["device"]}
        entities = {e.get("entity_id"): e for e in self.registry["entity"]}
        equipos: dict[str, dict] = {}
        sin_equipo: list[dict] = []
        for eid, info in sorted(self.electric.items()):
            reg = entities.get(eid) or {}
            ent = {
                "entity_id": eid,
                "key": eid.replace(".", "_", 1),  # misma clave que live/<key>
                "metrica": info["metric"],
                "unidad": info["unit"],
                "nombre": info["name"],
                "state_class": info["state_class"],
                "publicada": entity_matches(eid),
            }
            dev = devices.get(reg.get("device_id"))
            if not dev:
                sin_equipo.append(ent)
                continue
            rec = equipos.setdefault(dev["id"], {
                "nombre": dev.get("name_by_user") or dev.get("name") or dev["id"],
                "fabricante": dev.get("manufacturer"),
                "modelo": dev.get("model"),
                "area": areas.get(dev.get("area_id")),
                "entidades": [],
            })
            rec["entidades"].append(ent)
        return {"equipos": equipos, "sin_equipo": sin_equipo}

    def inventory_loop(self) -> None:
        while True:
            time.sleep(15)
            try:
                self._maybe_send_inventory()
            except Exception as exc:  # noqa: BLE001 - nunca matar el hilo
                log(f"Aviso: error armando el inventario ({exc}).")

    def _maybe_send_inventory(self) -> None:
        now = time.time()
        with self.lock:
            if not self.states_loaded or any(k not in self.registry for k in REGISTRIES):
                return
            if now < self.inv_next_try or now - self.inv_changed_at < INVENTORY_QUIET_S:
                return
            stale = now - self.inv_sent_at >= INVENTORY_RESEND_S
            if not self.inv_dirty and not stale:
                return
            inv = self.build_inventory()
            self.inv_dirty = False
        digest = hashlib.sha256(json.dumps(inv, sort_keys=True).encode()).hexdigest()
        if digest == self.inv_hash and not stale:
            return
        body = {"tenant_id": self.tenant, "token": self.token, "version": VERSION, "inventory": inv}
        try:
            st, resp = http_json("POST", f"{FUNCTIONS_BASE}/agentInventory", body, timeout=30)
        except OSError as exc:
            st, resp = 0, {"error": str(exc)}
        if st == 200:
            self.inv_hash, self.inv_sent_at = digest, now
            log(f"Inventario enviado: {len(inv['equipos'])} equipos, "
                f"{len(inv['sin_equipo'])} sensores sueltos.")
        else:
            with self.lock:
                self.inv_dirty = True
            self.inv_next_try = now + INVENTORY_RETRY_S
            log(f"Aviso: no se pudo enviar el inventario ({st}: {resp.get('error', '')}).")

    # ── 7. estadísticas de energía ───────────────────────────────
    def stats_ids(self) -> list[str]:
        with self.lock:
            if not self.states_loaded:
                return []
            return sorted(eid for eid, i in self.electric.items()
                          if i["metric"] == "energy" and i["state_class"] in STATS_STATE_CLASSES)

    def stats_loop(self) -> None:
        while True:
            time.sleep(STATS_EVERY_S)
            try:
                self._send_stats()
            except Exception as exc:  # noqa: BLE001 - nunca matar el hilo
                status["stats"] = f"error: {exc}"[:300]
                log(f"Aviso: error enviando las estadísticas de energía ({exc}).")

    def _send_stats(self) -> None:
        """Horas cerradas desde el cursor con las estadísticas horarias de HA, y
        la hora anterior + la actual con las de 5 min (la horaria de la hora
        recién cerrada puede no estar compilada todavía). Se reenvían en cada
        vuelta hasta que salen de esa ventana; el backend pisa lo anterior."""
        ids = self.stats_ids()
        if not ids:
            status["stats"] = "sin sensores de energía con estadísticas"
            return
        now = datetime.now(timezone.utc)
        current = now.replace(minute=0, second=0, microsecond=0)
        fresh_from = current - timedelta(hours=1)
        start = current - timedelta(hours=STATS_BACKFILL_H)
        cursor = read_json(STATS_PATH).get("cursor")
        if cursor:
            start = max(start, datetime.fromisoformat(cursor))

        ha = HAWebSocket()
        try:
            while start < fresh_from:
                end = min(start + timedelta(hours=STATS_CHUNK_H), fresh_from)
                if not self._post_stats(ha.statistics(ids, start, end, "hour"), current):
                    return
                write_json_private(STATS_PATH, {"cursor": end.isoformat()})
                start = end
            if self._post_stats(ha.statistics(ids, fresh_from, now, "5minute"), current):
                status["stats"] = f"ok: {len(ids)} sensores"
        finally:
            ha.close()

    def _post_stats(self, result: dict, current: datetime) -> bool:
        hours = stats_by_hour(result)
        if not hours:
            return True
        cur = current.strftime(HOUR_FMT)
        body = {"tenant_id": self.tenant, "token": self.token, "version": VERSION,
                "hours": {h: {"kwh": kwh, "parcial": h == cur} for h, kwh in hours.items()}}
        try:
            st, resp = http_json("POST", f"{FUNCTIONS_BASE}/agentStats", body, timeout=60)
        except OSError as exc:
            st, resp = 0, {"error": str(exc)}
        if st != 200:
            status["stats"] = f"error al enviar ({st})"
            log(f"Aviso: no se pudieron enviar las estadísticas ({st}: {resp.get('error', '')}).")
            return False
        return True


class HAWebSocket:
    """Conexión corta a la API WebSocket de HA para pedidos puntuales."""

    def __init__(self) -> None:
        self.ws = websocket.create_connection("ws://supervisor/core/websocket", timeout=60)
        self.ids = itertools.count(1)
        try:
            self.ws.recv()  # auth_required
            self.ws.send(json.dumps({"type": "auth", "access_token": SUPERVISOR_TOKEN}))
            if json.loads(self.ws.recv()).get("type") != "auth_ok":
                raise RuntimeError("auth con Home Assistant rechazada")
        except Exception:
            self.ws.close()
            raise

    def call(self, msg: dict):
        mid = next(self.ids)
        self.ws.send(json.dumps({**msg, "id": mid}))
        while True:
            r = json.loads(self.ws.recv())
            if r.get("id") != mid:
                continue
            if not r.get("success"):
                raise RuntimeError((r.get("error") or {}).get("message") or "error de HA")
            return r.get("result")

    def statistics(self, ids: list[str], start: datetime, end: datetime, period: str) -> dict:
        return self.call({
            "type": "recorder/statistics_during_period",
            "start_time": start.isoformat(), "end_time": end.isoformat(),
            "statistic_ids": ids, "period": period, "types": ["change"],
            "units": {"energy": "kWh"},  # HA convierte Wh/MWh → kWh
        }) or {}

    def close(self) -> None:
        self.ws.close()


def stats_by_hour(result: dict) -> dict[str, dict[str, float]]:
    """{entity_id: [{start, change}]} → {hora UTC: {key: kWh}} (suma los 5 min)."""
    out: dict[str, dict[str, float]] = {}
    for eid, rows in result.items():
        key = eid.replace(".", "_", 1)
        for r in rows or []:
            ch, ts = r.get("change"), r.get("start")
            if ch is None or ts is None:
                continue
            # HA ≥ 2023.3 da ms epoch; versiones viejas, ISO.
            t = (datetime.fromtimestamp(ts / 1000, timezone.utc) if isinstance(ts, (int, float))
                 else datetime.fromisoformat(ts).astimezone(timezone.utc))
            h = out.setdefault(t.strftime(HOUR_FMT), {})
            h[key] = round(h.get(key, 0.0) + float(ch), 5)
    return out


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
        threading.Thread(target=publisher.inventory_loop, daemon=True).start()
        threading.Thread(target=publisher.stats_loop, daemon=True).start()
        threading.Thread(target=heartbeat_loop, args=(creds, extra), daemon=True).start()

    status["http_config"] = ensure_http_config()
    extra["http_detail"] = status["http_config"]
    extra["external_url"] = ensure_external_url(creds.get("url"))

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
