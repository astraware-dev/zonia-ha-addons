#!/usr/bin/with-contenv bashio
# El agente hace todo (activación, túnel, sensores, configuración de HA).
# Ver agent.py.
exec python3 /app/agent.py
