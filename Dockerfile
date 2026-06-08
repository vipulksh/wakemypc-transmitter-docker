FROM python:3.14-slim

# No build deps needed -- websockets is pure-Python.
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transmitter.py .

# Unbuffered so logs show up immediately in `docker logs`.
ENV PYTHONUNBUFFERED=1

# Run as a non-root user. WOL broadcast + LAN TCP need host networking
# (set in docker-compose.yml), not root.
RUN useradd --no-create-home --uid 10001 transmitter
USER transmitter

CMD ["python", "transmitter.py"]
