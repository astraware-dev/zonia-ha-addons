#!/usr/bin/with-contenv bashio
# Abre un túnel SALIENTE al servidor de Zonia y publica este Home Assistant en
# https://<tenant_id>.<dominio de Zonia>. No requiere abrir puertos en el router.
set -e

TENANT="$(bashio::config 'tenant_id')"
TOKEN="$(bashio::config 'token')"
SERVER="$(bashio::config 'server')"
PORT="$(bashio::config 'server_port')"

if [ -z "$TENANT" ] || [ -z "$TOKEN" ]; then
  bashio::exit.nok "Configurá tenant_id y token (te los da Zonia) y reiniciá el add-on."
fi

cat > /tmp/frpc.toml <<EOF
serverAddr = "${SERVER}"
serverPort = ${PORT}
user = "${TENANT}"
loginFailExit = false

# TLS verificado contra la CA de Zonia (el cert del server lleva su IP).
transport.tls.enable = true
transport.tls.trustedCaFile = "/etc/zonia/ca.crt"
transport.tls.serverName = "${SERVER}"

metadatas.tenant = "${TENANT}"
metadatas.token = "${TOKEN}"

log.to = "console"
log.level = "info"

[[proxies]]
name = "ha"
type = "http"
localIP = "homeassistant"
localPort = 8123
subdomain = "${TENANT}"
EOF

bashio::log.info "Conectando túnel de '${TENANT}' a ${SERVER}:${PORT}…"
exec frpc -c /tmp/frpc.toml
