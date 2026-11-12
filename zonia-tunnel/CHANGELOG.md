# Registro de cambios

## 2.3.0 — 2026-11-11

- Solo se publican los equipos que miden consumo (los que reportan energía y
  además potencia, voltaje o corriente): su contador de energía, la potencia y
  el voltaje. Se deja de enviar corriente, factor de potencia, energía
  exportada, ventanas diarias/semanales/anuales, helpers y sensores sueltos.
- Potencia y voltaje se envían cuando cambian de forma relevante (10 W o 5 % /
  2 V), a lo sumo cada 5 s, con un refresco cada 5 min. Prender y apagar se
  envía al instante.
- Si en Home Assistant no hay ningún equipo así, se publica por nombre como
  antes.

## 2.2.0 — 2026-10-23

- Inventario de equipos eléctricos instalados (dispositivos, entidades y áreas
  de Home Assistant).
- Consumo por hora desde las estadísticas de Home Assistant, con recuperación
  de las horas en que no hubo conexión.

## 2.0.0 — 2026-10-12

- Alta automática con código de activación: túnel, MQTT y configuración de
  `http:` sin pasos manuales.

## 1.0.0 — 2026-10-08

- Acceso remoto a Home Assistant a través del servidor de Zonia.
