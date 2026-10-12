ARG BUILD_FROM
FROM ${BUILD_FROM}

ARG BUILD_ARCH
# Misma versión que el frps del servidor (infra/docker-compose.yml).
ARG FRP_VERSION=0.61.1

RUN set -eux; \
    case "${BUILD_ARCH}" in \
      amd64) arch=amd64 ;; \
      aarch64) arch=arm64 ;; \
      armv7) arch=arm ;; \
      *) echo "arquitectura no soportada: ${BUILD_ARCH}"; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/frp_${FRP_VERSION}_linux_${arch}.tar.gz" \
      | tar -xz -C /tmp; \
    mv "/tmp/frp_${FRP_VERSION}_linux_${arch}/frpc" /usr/bin/frpc; \
    rm -rf /tmp/frp_*

# Agente (Python): activación, publicación de sensores, config de HA.
RUN apk add --no-cache python3 py3-pip \
    && pip install --no-cache-dir --break-system-packages \
       paho-mqtt==2.1.0 websocket-client==1.8.0

# CA de Zonia: valida el servidor de túneles y el broker MQTT.
COPY ca.crt /etc/zonia/ca.crt
COPY agent.py /app/agent.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
