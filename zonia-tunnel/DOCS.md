# Zonia

Conecta esta casa a Zonia con un **código de activación**:

- **Acceso remoto seguro** en `https://<tu-id>.ha.zonia.co` (túnel cifrado
  saliente: no hace falta abrir puertos ni Nabu Casa).
- **Monitoreo de energía**: publica solo los equipos que miden consumo (los que
  reportan energía y además potencia, voltaje o corriente): su contador de
  energía, la potencia y el voltaje. Potencia y voltaje se envían cuando cambian
  de forma relevante (a lo sumo cada 5 s, y un refresco cada 5 min). Si en tu
  HA no hay ningún equipo así, publica los sensores eléctricos por su nombre.
- **Inventario de equipos**: envía la lista de dispositivos de Home Assistant
  que tienen sensores eléctricos (nombre, fabricante, modelo y área). Solo eso:
  no envía otros dispositivos ni datos personales.
- **Consumo por hora**: envía cuántos kWh registró cada sensor de energía por
  hora, según las estadísticas de Home Assistant. Si se corta internet, al
  volver completa las horas que faltaron. Solo funciona con sensores que tienen
  `state_class` (los mismos que acepta el panel de Energía de HA).

## Configuración

1. Pegá en `activation_code` el código que te dio Zonia (ej. `4P13-95K8-TV5P`).
2. Guardá, activá **Start on boot** y dale **Start**. (**Watchdog** es opcional:
   el agente ya se reinicia solo si se detiene.)

El código se usa una sola vez. Las credenciales quedan guardadas en el
almacenamiento privado del add-on.

## Qué cambia en tu Home Assistant

Si `configuration.yaml` **no** tiene un bloque `http:`, el add-on agrega este
bloque (necesario para el acceso remoto):

```yaml
http:
  use_x_forwarded_for: true
  trusted_proxies:
    - 172.30.32.0/23
    - 10.250.0.0/24
  ip_ban_enabled: true
  login_attempts_threshold: 5
```

Medidas de seguridad:

- Antes de tocar nada guarda una copia: `configuration.yaml.zonia-backup-<fecha>`.
- Valida la configuración (como "Comprobar configuración"). Si falla, deja el
  archivo como estaba y **no** reinicia.
- Reinicia Home Assistant una vez. Si no vuelve a levantar en 10 minutos,
  restaura el archivo original y reinicia de nuevo.
- Si ya existe un bloque `http:`, **no lo modifica**. En ese caso agregá a mano
  las líneas de arriba que falten.

También completa la "URL de Internet" de Home Assistant si estaba vacía.

Recomendado: activá la verificación en dos pasos en el perfil de cada usuario.

## Actualizaciones y cortes

- Las actualizaciones se descargan ya compiladas: no se compila nada en tu
  Home Assistant.
- Si el agente se detiene, se vuelve a iniciar solo (en segundos; si sigue
  fallando, espera cada vez más, hasta 5 minutos).
- Si al activar no hay internet, sigue funcionando con la conexión anterior y
  reintenta el código en segundo plano.

## Modo manual (instalaciones anteriores)

Con `tenant_id` + `token` (sin código), el add-on solo abre el túnel de acceso
remoto.
