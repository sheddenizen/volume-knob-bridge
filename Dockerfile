FROM python:3.13-slim

RUN pip install --no-cache-dir paho-mqtt==1.6.1 "websockets>=13"

WORKDIR /app
COPY ha_ws.py bridge.py ./

CMD ["python3", "bridge.py"]
