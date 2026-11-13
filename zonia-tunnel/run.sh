#!/usr/bin/with-contenv bashio
# El agente hace todo (activación, túnel, sensores, configuración de HA).
# Ver agent.py.
#
# El túnel ES el acceso remoto: si el agente termina por cualquier motivo
# (error, sin internet al arrancar, credenciales nuevas), se vuelve a levantar
# solo, con espera creciente (5 s … 5 min). Nunca queda apagado esperando a que
# alguien lo reinicie en la casa.

child=""
stop() {
  [ -n "$child" ] && kill -TERM "$child" 2>/dev/null && wait "$child"
  exit 0
}
trap stop TERM INT

delay=5
while true; do
  started=$(date +%s)
  python3 /app/agent.py &
  child=$!
  wait "$child"
  code=$?
  child=""

  # Si corrió más de 10 min, fue un corte puntual: reintentar rápido.
  if [ $(( $(date +%s) - started )) -gt 600 ]; then
    delay=5
  fi
  bashio::log.warning "El agente terminó (código ${code}); se reinicia en ${delay} s."
  sleep "$delay" &
  wait $!
  delay=$(( delay * 2 ))
  [ "$delay" -gt 300 ] && delay=300
done
