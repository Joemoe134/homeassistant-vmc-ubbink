from unittest.mock import MagicMock
import unittest.mock as mock

import pytest

import direct
import vigor


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _device_returning(values):
    dev = MagicMock()
    for key, getter in direct._READERS.items():
        getattr(dev, getter).return_value = values.get(key, f"<{key}>")
    return dev


_FULL = {
    "serial_number": "001200340056",
    "supply_temperature": 21.5,
    "supply_pressure": 30,
    "supply_humidity": 45,
    "supply_airflow_actual": 150,
    "supply_airflow_preset": 150,
    "extract_temperature": 20.0,
    "extract_pressure": 28,
    "extract_humidity": 50,
    "extract_airflow_actual": 150,
    "extract_airflow_preset": 150,
    "airflow_mode": "normal",
    "bypass_status": "open",
    "bypass_mode": "auto",
    "filter_status": "normal",
}


def test_get_data_returns_all_keys():
    dev = _device_returning(_FULL)
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=FakeClock())
    data = client.get_data()
    assert set(data.keys()) == set(direct._READERS)
    assert data["supply_temperature"] == 21.5
    assert data["supply_humidity"] == 45


def test_get_data_caches_within_ttl():
    dev = _device_returning(_FULL)
    clock = FakeClock()
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=clock)
    client.get_data()
    client.get_data()
    assert dev.get_serial_number.call_count == 1


def test_get_data_repolls_after_ttl():
    dev = _device_returning(_FULL)
    clock = FakeClock()
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=clock)
    client.get_data()
    clock.t += 10
    client.get_data()
    assert dev.get_serial_number.call_count == 2


def test_field_level_error_becomes_none():
    dev = _device_returning(_FULL)
    dev.get_supply_humidity.side_effect = vigor.ModbusError("boom")
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=FakeClock())
    data = client.get_data()
    assert data["supply_humidity"] is None
    assert data["supply_temperature"] == 21.5


def test_set_airflow_rate_invalidates_cache():
    dev = _device_returning(_FULL)
    clock = FakeClock()
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=clock)
    client.get_data()
    assert dev.get_serial_number.call_count == 1
    client.set_airflow_rate(200)
    dev.set_custom_airflow_rate.assert_called_once_with(200)
    client.get_data()
    assert dev.get_serial_number.call_count == 2


def test_set_airflow_mode_delegates():
    dev = _device_returning(_FULL)
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=FakeClock())
    client.set_airflow_mode("high")
    dev.set_airflow_mode.assert_called_once_with("high")


def test_set_bypass_mode_delegates():
    dev = _device_returning(_FULL)
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=FakeClock())
    result = client.set_bypass_mode("open")
    dev.set_bypass_mode.assert_called_once_with("open")
    assert "error" not in result


def test_set_bypass_mode_invalidates_cache():
    dev = _device_returning(_FULL)
    clock = FakeClock()
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=clock)
    client.get_data()
    assert dev.get_serial_number.call_count == 1
    client.set_bypass_mode("closed")
    client.get_data()
    assert dev.get_serial_number.call_count == 2


def test_write_throttle_enforces_minimum_interval():
    dev = _device_returning(_FULL)
    clock = FakeClock()
    client = direct.DirectClient("1.2.3.4", 502, 20, _device=dev, _clock=clock)
    client._last_bus_ts = clock.t
    clock.t += direct.MIN_WRITE_INTERVAL / 2
    with mock.patch("direct.time.sleep") as mock_sleep:
        client.set_airflow_mode("high")
    assert mock_sleep.called
    assert mock_sleep.call_args[0][0] > 0
