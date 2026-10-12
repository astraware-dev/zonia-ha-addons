# Zonia

Conecta esta casa a Zonia con un **código de activación**:

- **Acceso remoto seguro** en `https://<tu-id>.ha.zonia.co` (túnel cifrado
  saliente: no hace falta abrir puertos ni Nabu Casa).
- **Monitoreo de energía**: publica los sensores de potencia, energía, voltaje
  y corriente en la plataforma de Zonia.

## Configuración

1. Pegá en `activation_code` el código que te dio Zonia (ej. `4P13-95K8-TV5P`).
2. Guardá, activá **Start on boot** y **Watchdog**, y dale **Start**.

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

## Modo manual (instalaciones anteriores)

Con `tenant_id` + `token` (sin código), el add-on solo abre el túnel de acceso
remoto.
