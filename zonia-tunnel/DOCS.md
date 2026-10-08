# Zonia Tunnel

Publica este Home Assistant en `https://<tenant_id>.<dominio de Zonia>`
mediante un túnel saliente cifrado hacia el servidor de Zonia. No hace falta
abrir puertos en el router y funciona detrás de CGNAT.

## Configuración

| Opción        | Valor                                   |
|---------------|-----------------------------------------|
| `tenant_id`   | el identificador de la casa (lo da Zonia) |
| `token`       | el token del túnel (lo da Zonia)        |
| `server`      | dejar por defecto                       |
| `server_port` | dejar por defecto                       |

Además, en `configuration.yaml`:

```yaml
http:
  use_x_forwarded_for: true
  trusted_proxies:
    - 172.30.32.0/23
    - 10.250.0.0/24
  ip_ban_enabled: true
  login_attempts_threshold: 5
```

y reiniciar Home Assistant. Recomendado: activar la verificación en dos pasos
en el perfil de cada usuario de HA.
