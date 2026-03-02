#!/usr/bin/env python3
"""Prometheus exporter for PMS7003 and BME280 sensors."""

import logging
import os
import time

import smbus2
import bme280
from pms7003 import Pms7003Sensor, PmsSensorException
from prometheus_client import Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

SENSOR_NODE = os.environ.get("SENSOR_NODE", "chan01")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
PMS_SAMPLES = int(os.environ.get("PMS_SAMPLES", "5"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9101"))
BME280_ADDRESS = int(os.environ.get("BME280_ADDRESS", "0x76"), 0)
BME280_BUS = int(os.environ.get("BME280_BUS", "1"))
PMS_SERIAL_DEV = os.environ.get("PMS_SERIAL_DEV", "/dev/serial0")

labels = {"sensor_node": SENSOR_NODE}

# BME280 gauges
temperature_gauge = Gauge(
    "growbot_temperature_celsius", "Temperature in Celsius", ["sensor_node"]
)
humidity_gauge = Gauge(
    "growbot_humidity_percent", "Relative humidity percentage", ["sensor_node"]
)
pressure_gauge = Gauge(
    "growbot_pressure_hpa", "Barometric pressure in hPa", ["sensor_node"]
)

# PMS7003 mass concentration gauges
pm1_gauge = Gauge(
    "growbot_pm1_0_ugm3", "PM1.0 mass concentration ug/m3", ["sensor_node"]
)
pm25_gauge = Gauge(
    "growbot_pm2_5_ugm3", "PM2.5 mass concentration ug/m3", ["sensor_node"]
)
pm10_gauge = Gauge(
    "growbot_pm10_ugm3", "PM10 mass concentration ug/m3", ["sensor_node"]
)

# PMS7003 particle count gauges
particle_gauge = Gauge(
    "growbot_particle_count",
    "Particle count per 0.1L",
    ["sensor_node", "size"],
)

PARTICLE_SIZES = {
    "0.3": "n0_3",
    "0.5": "n0_5",
    "1.0": "n1_0",
    "2.5": "n2_5",
    "5.0": "n5_0",
    "10": "n10_0",
}


def read_bme280(bus, calibration_params):
    """Read BME280 sensor and update gauges."""
    try:
        data = bme280.sample(bus, BME280_ADDRESS, calibration_params)
        temperature_gauge.labels(**labels).set(data.temperature)
        humidity_gauge.labels(**labels).set(data.humidity)
        pressure_gauge.labels(**labels).set(data.pressure)
        log.info(
            "BME280: %.1f°C, %.1f%% RH, %.1f hPa",
            data.temperature,
            data.humidity,
            data.pressure,
        )
    except Exception:
        log.warning("BME280 read failed", exc_info=True)


def read_pms7003(sensor):
    """Read PMS7003 sensor, average over multiple samples, and update gauges."""
    readings = []
    for i in range(PMS_SAMPLES):
        try:
            data = sensor.read()
            readings.append(data)
        except PmsSensorException:
            log.warning("PMS7003 sample %d/%d failed", i + 1, PMS_SAMPLES)
        except Exception:
            log.warning("PMS7003 unexpected error on sample %d/%d", i + 1, PMS_SAMPLES, exc_info=True)

    if not readings:
        log.warning("PMS7003: all %d samples failed", PMS_SAMPLES)
        return

    def avg(key):
        return sum(r[key] for r in readings) / len(readings)

    pm1 = avg("pm1_0")
    pm25 = avg("pm2_5")
    pm10 = avg("pm10")

    pm1_gauge.labels(**labels).set(pm1)
    pm25_gauge.labels(**labels).set(pm25)
    pm10_gauge.labels(**labels).set(pm10)

    for size_label, data_key in PARTICLE_SIZES.items():
        particle_gauge.labels(sensor_node=SENSOR_NODE, size=size_label).set(
            avg(data_key)
        )

    log.info(
        "PMS7003: PM1.0=%.1f PM2.5=%.1f PM10=%.1f (%d/%d samples)",
        pm1,
        pm25,
        pm10,
        len(readings),
        PMS_SAMPLES,
    )


def main():
    log.info(
        "Starting growbot sensor exporter on :%d (node=%s, interval=%ds)",
        EXPORTER_PORT,
        SENSOR_NODE,
        POLL_INTERVAL,
    )

    # Init BME280
    bus = smbus2.SMBus(BME280_BUS)
    calibration_params = bme280.load_calibration_params(bus, BME280_ADDRESS)
    log.info("BME280 initialized on bus %d address 0x%02x", BME280_BUS, BME280_ADDRESS)

    # Init PMS7003
    pms_sensor = Pms7003Sensor(PMS_SERIAL_DEV)
    log.info("PMS7003 initialized on %s", PMS_SERIAL_DEV)

    start_http_server(EXPORTER_PORT)
    log.info("HTTP server listening on :%d", EXPORTER_PORT)

    try:
        while True:
            read_bme280(bus, calibration_params)
            read_pms7003(pms_sensor)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        pms_sensor.close()
        bus.close()


if __name__ == "__main__":
    main()
