"""Direct client: Modbus RTU over TCP to a Waveshare RS485-to-ETH gateway.

The interface is identical to ServerClient (api.VMCUbifluxAPI): get_data /
set_airflow_mode / set_airflow_rate / set_bypass_mode -- so sensor/number/select
don't care which client they use.

Throttling notes
----------------
The Vigor device ignores Modbus commands sent too close together. Two guards:
1. Read cache (CACHE_TTL): suppresses repeated bus reads within 5 s.
2. Write throttle (MIN_WRITE_INTERVAL): enforces a minimum gap between writes.
   Both reads and writes share _last_bus_ts so a write after a cache-miss read
   is also rate-limited.
"""
import logging
import threading
import time

from pymodbus.client import ModbusTcpClient

try:
    from .vigor import RTU_FRAMER, ModbusError, VigorDevice
except ImportError:  # pragma: no cover - path for tests outside the package
    from vigor import RTU_FRAMER, ModbusError, VigorDevice

_LOGGER = logging.getLogger(__name__)

CACHE_TTL = 5.0           # seconds -- suppress re-reads within this window
MIN_WRITE_INTERVAL = 2.0  # seconds -- minimum gap between consecutive writes

# result key -> VigorDevice method name
_READERS = {
    "serial_number":          "get_serial_number",
    "supply_temperature":     "get_supply_temperature",
    "supply_pressure":        "get_supply_pressure",
    "supply_humidity":        "get_supply_humidity",
    "supply_airflow_actual":  "get_supply_airflow_actual",
    "supply_airflow_preset":  "get_supply_airflow_preset",
    "extract_temperature":    "get_extract_temperature",
    "extract_pressure":       "get_extract_pressure",
    "extract_humidity":       "get_extract_humidity",
    "extract_airflow_actual": "get_extract_airflow_actual",
    "extract_airflow_preset": "get_extract_airflow_preset",
    "airflow_mode":           "get_airflow_mode",
    "bypass_status":          "get_bypass_status",
    "bypass_mode":            "get_bypass_mode",   # holding reg 6100
    "filter_status":          "get_filter_status",
}


class DirectClient:
    def __init__(self, host, port, slave, *, _device=None, _clock=time.monotonic):
        self._host = host
        self._port = port
        self._slave = slave
        self._lock = threading.Lock()
        self._clock = _clock
        self._cache = None
        self._cache_ts = 0.0
        # Shared last-bus-access timestamp (updated on reads AND writes)
        self._last_bus_ts = 0.0
        if _device is not None:
            # Test path: inject a ready VigorDevice, no real socket.
            self._device = _device
            self._client = None
        else:
            self._client = ModbusTcpClient(
                host, port=port, framer=RTU_FRAMER, timeout=10
            )
            self._device = VigorDevice(self._client, slave=slave)

    def _ensure_connected(self):
        if self._client is not None and not self._client.connected:
            self._client.connect()

    def close(self):
        if self._client is not None:
            self._client.close()

    def _poll(self):
        data = {}
        for key, getter in _READERS.items():
            try:
                data[key] = getattr(self._device, getter)()
            except (ModbusError, IndexError) as err:
                _LOGGER.debug("VMC direct read %s failed: %s", key, err)
                data[key] = None
        return data

    def get_data(self):
        now = self._clock()
        if self._cache is not None and (now - self._cache_ts) < CACHE_TTL:
            return self._cache
        with self._lock:
            now = self._clock()
            if self._cache is not None and (now - self._cache_ts) < CACHE_TTL:
                return self._cache
            try:
                self._ensure_connected()
                data = self._poll()
            except Exception as err:  # noqa: BLE001 - transport failure
                _LOGGER.warning("VMC direct poll failed: %s", err)
                return {"error": str(err)}
            self._cache = data
            self._cache_ts = self._clock()
            self._last_bus_ts = self._cache_ts  # reads count toward the throttle
            return data

    def _throttled_set(self, func, *args):
        """Execute a write with rate-limiting and cache invalidation."""
        with self._lock:
            elapsed = self._clock() - self._last_bus_ts
            if elapsed < MIN_WRITE_INTERVAL:
                wait = MIN_WRITE_INTERVAL - elapsed
                _LOGGER.debug("VMC write throttle: waiting %.2f s", wait)
                time.sleep(wait)
            try:
                self._ensure_connected()
                func(*args)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("VMC direct write failed: %s", err)
                return {"error": str(err)}
            finally:
                self._last_bus_ts = self._clock()
                self._cache = None
        return None

    def set_airflow_mode(self, mode):
        err = self._throttled_set(self._device.set_airflow_mode, mode)
        return err or {"status": f"Airflow mode set to {mode}"}

    def set_airflow_rate(self, rate):
        err = self._throttled_set(self._device.set_custom_airflow_rate, int(rate))
        return err or {"status": f"Airflow rate set to {rate} m3/h"}

    def set_bypass_mode(self, mode):
        """Set bypass mode via holding register 6100 (auto / open / closed)."""
        err = self._throttled_set(self._device.set_bypass_mode, mode)
        return err or {"status": f"Bypass mode set to {mode}"}
