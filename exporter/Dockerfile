FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -r exporter && \
    useradd -r -g exporter -G dialout exporter

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sensor_exporter.py .

USER exporter

EXPOSE 9101

CMD ["python", "sensor_exporter.py"]
