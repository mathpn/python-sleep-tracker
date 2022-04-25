"""
Module containing helper functions and SensorDevice class to stream data from MetaWear devices.
"""

from enum import Enum
from functools import partial
import time
import multiprocessing as mp
from threading import Event
from typing import Optional

from mbientlab.metawear import cbindings, Const, libmetawear, MetaWear, parse_value

from src.logger import LOG


class MetaWearDevice:

    def __init__(self, mac_address: str):
        self.mac_address = mac_address
        self.device = MetaWear(self.mac_address)
        self.device.on_disconnect = lambda status: LOG.warning("disconnected from device")

    def connect(self):
        try:
            self.device.connect()
            LOG.info(f"Connected to {self.mac_address}")
        except Exception as exc:
            LOG.error(f"Failed to connect to {self.mac_address}: {exc}")

    def disconnect(self):
        libmetawear.mbl_mw_debug_reset(self.device.board)
        self.device.disconnect()

    @property
    def acc_model(self):
        return libmetawear.mbl_mw_metawearboard_lookup_module(
            self.device.board, cbindings.Module.ACCELEROMETER
        )

    @property
    def gyro_model(self):
        return libmetawear.mbl_mw_metawearboard_lookup_module(
            self.device.board, cbindings.Module.GYRO
        )

    def return_acc_freq_options(self) -> Enum:
        # TODO implement
        pass

    def return_acc_range_options(self) -> Optional[Enum]:
        model = self.acc_model
        enum_name = 'Accelerometer Range'
        if (
                model == Const.MODULE_ACC_TYPE_BMI160
                or model == Const.MODULE_ACC_TYPE_BMI270
                or model == Const.MODULE_ACC_TYPE_BMA255
            ):
            return _parse_option_enum(cbindings.AccBoschRange, enum_name)
        if model == Const.MODULE_ACC_TYPE_MMA8452Q:
            return _parse_option_enum(cbindings.AccMma8452qRange, enum_name)

        LOG.warning('Accelerometer not supported on this device')
        return None

    def return_gyro_freq_options(self) -> Optional[Enum]:
        model = self.gyro_model
        enum_name = 'Gyroscope Frequency'
        if model == Const.MODULE_GYRO_TYPE_BMI160:
            return _parse_option_enum(cbindings.GyroBoschOdr, enum_name)
        if model == Const.MODULE_GYRO_TYPE_BMI270:
            return _parse_option_enum(cbindings.GyroBoschOdr, enum_name)
        LOG.warning('Gyroscope not supported on this device')
        return None

    def return_gyro_range_options(self) -> Optional[Enum]:
        model = self.gyro_model
        enum_name = 'Gyroscope Range'
        if model == Const.MODULE_GYRO_TYPE_BMI160:
            return _parse_option_enum(cbindings.GyroBoschRange, enum_name)
        if model == Const.MODULE_GYRO_TYPE_BMI270:
            return _parse_option_enum(cbindings.GyroBoschRange, enum_name)
        LOG.warning('Gyroscope not supported on this device')
        return None


def _parse_option_enum(cbinding_enum, enum_name: str) -> Enum:
    """
    Helper function to parse the enum values from the cbindings module.
    """
    options = {
        option.strip('_'): value for option, value
        in cbinding_enum.__dict__.items() if '__' not in option
    }
    options_enum = Enum(enum_name, options)
    return options_enum


class SensorDevice:
    """
    Wrapper class around MetaWear device with convenience methods to stream sensor data.

    Attributes:
        device (MetaWear): MetaWear device object.
        samples (int): Number of samples streamed.
        acc_queue (mp.Queue): Queue to store accelerometer data.
        gyro_queue (mp.Queue): Queue to store gyroscope data.
        raw (bool): If True, raw sensor data is streamed.
        acc_callback (cbindings.FnVoid_VoidP_DataP): Accelerometer data callback.
        gyro_callback (cbindings.FnVoid_VoidP_DataP): Gyroscope data callback.
        acc (bool): Whether to stream accelerometer data.
        gyro (bool): Whether to stream gyroscope data.
        temperature (bool): Whether to stream temperature data.

    Methods:
        connect (): Connect to MetaWear device.
        register_sensors (frequency: float, acc_range: float = 8.0):
            Configure and subscribe to sensor signals.
        stream_data (): Stream sensor data.
        stop_streaming (): Stop streaming sensor data.
    """
    def __init__(self, device, acc_queue: mp.Queue, gyro_queue: mp.Queue, raw: bool = False):
        """
        Parameters:
            device (MetaWear): MetaWear device object.
            acc_queue (mp.Queue): Queue to store accelerometer data.
            gyro_queue (mp.Queue): Queue to store gyroscope data.
            raw (bool): If True, raw sensor data is streamed.
        """
        self.device = device
        self.samples = 0
        self.acc_queue = acc_queue
        self.gyro_queue = gyro_queue
        self.raw = raw
        self.acc_callback = cbindings.FnVoid_VoidP_DataP(self._acc_data_handler())
        self.gyro_callback = cbindings.FnVoid_VoidP_DataP(self._gyro_data_handler())
        self.acc = False
        self.gyro = False
        self.temperatue = False

    def _acc_data_handler(self):
        return partial(self._data_handler, data_queue=self.acc_queue, msg='acc')

    def _gyro_data_handler(self):
        return partial(self._data_handler, data_queue=self.gyro_queue, msg='gyro')

    def _data_handler(self, _, raw_data, data_queue: mp.Queue, msg: str) -> None:
        data = parse_value(raw_data)
        LOG.debug(f'{msg}: {data:.4f}')
        if self.raw:
            data = [data.x, data.y, data.z]
        else:
            data = [data]
        data_queue.put(data)
        self.samples += 1

    def connect(
        self, min_conn_interval: float = 7.5, max_conn_interval: float = 7.5, latency: int = 0, timeout: int = 6000
    ) -> None:
        """
        Connect to MetaWear device.
        Parameters:
            min_conn_interval (float): Minimum connection interval in miliseconds.
            max_conn_interval (float): Maximum connection interval in miliseconds.
            latency (int): Maximum number of consecutive non-answered calls allowed.
            timeout (int): Connection timeout in miliseconds.
        """
        self.device.on_disconnect = lambda status: LOG.warning("disconnected from device")
        self.device.connect()
        time.sleep(1)
        if not self.device.is_connected:
            raise ConnectionError("Failed to connect to MetaWear board")
        libmetawear.mbl_mw_settings_set_connection_parameters(
            self.device.board, min_conn_interval, max_conn_interval, latency, timeout
        )
        time.sleep(1)

    def stream_data(self) -> None:
        """ Stream sensor data. """
        if self.acc:
            libmetawear.mbl_mw_acc_enable_acceleration_sampling(self.device.board)
        if self.gyro:
            libmetawear.mbl_mw_gyro_bmi160_enable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")
        LOG.info("Streaming data...")
        libmetawear.mbl_mw_acc_start(self.device.board)
        libmetawear.mbl_mw_gyro_bmi160_start(self.device.board)

    def stop_streaming(self) -> None:
        """ Stop streaming data. """
        LOG.info("Stopping streaming...")
        if self.acc:
            LOG.info("removing acc callback")
            libmetawear.mbl_mw_acc_stop(self.device.board)
            libmetawear.mbl_mw_acc_disable_acceleration_sampling(self.device.board)
        if self.gyro:
            LOG.info("removing gyro callback")
            libmetawear.mbl_mw_gyro_bmi160_stop(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")
        time.sleep(1)
        LOG.info(f"streamed {self.samples} samples")
        self.samples = 0
        libmetawear.mbl_mw_debug_reset(self.device.board)

    def register_sensors(self, frequency: float, acc_range: float = 8.0) -> None:
        """ Configure and subscribe to sensor signals. """
        # TODO: Add temperature streaming
        # TODO: gyroscope frequency
        acc_signal = self._register_accelerometer(frequency, acc_range)
        gyro_signal = self._register_gyroscope()
        if not self.raw:
            LOG.debug(f'setting up acc signal processor: {acc_signal}')
            acc_signal = _create_standard_preprocessor(acc_signal, 5, 0.005)
            LOG.debug(f'output acc signal: {acc_signal}')
            LOG.debug(f'setting up gyro signal processor: {gyro_signal}')
            gyro_signal = _create_standard_preprocessor(gyro_signal, 5, 3)
            LOG.debug(f'output gyro signal: {gyro_signal}')
        _subscribe_to_signal(acc_signal, self.acc_callback)
        _subscribe_to_signal(gyro_signal, self.gyro_callback)

    def _register_accelerometer(self, frequency: float, value_range: float) -> int:
        """ Register the accelerometer data signal. """
        libmetawear.mbl_mw_acc_set_odr(self.device.board, frequency)
        libmetawear.mbl_mw_acc_set_range(self.device.board, value_range)
        libmetawear.mbl_mw_acc_write_acceleration_config(self.device.board)
        signal_id = libmetawear.mbl_mw_acc_get_acceleration_data_signal(self.device.board)
        self.acc = True
        return signal_id

    def _register_gyroscope(self) -> int:
        """ Register the gyroscope data signal """
        libmetawear.mbl_mw_gyro_bmi160_set_odr(self.device.board, cbindings.GyroBoschOdr._50Hz)
        libmetawear.mbl_mw_gyro_bmi160_set_range(self.device.board, cbindings.GyroBoschRange._1000dps)
        libmetawear.mbl_mw_gyro_bmi160_write_config(self.device.board)
        signal_id = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(self.device.board)
        self.gyro = True
        return signal_id


def _subscribe_to_signal(sensor_signal, callback) -> None:
    libmetawear.mbl_mw_datasignal_subscribe(sensor_signal, None, callback)


def _create_standard_preprocessor(signal_id: int, window: int, min_delta: float) -> int:
    """ Create a preprocessor for the given signal. """
    signal_id = _register_rss_preprocessor(signal_id)
    signal_id = _register_average_preprocessor(signal_id, window)
    signal_id = _register_delta_preprocessor(signal_id, min_delta)
    return signal_id


def _register_rss_preprocessor(signal_id: int) -> int:
    """ Register the RMS preprocessor. """
    register_callable = partial(libmetawear.mbl_mw_dataprocessor_rms_create, signal_id, None)
    return _register_data_processor(register_callable)


def _register_average_preprocessor(signal_id: int, window: int) -> int:
    """ Register moving average preprocessor. """
    register_callable = partial(libmetawear.mbl_mw_dataprocessor_average_create, signal_id, window, None)
    return _register_data_processor(register_callable)


def _register_delta_preprocessor(signal_id: int, min_delta: float) -> int:
    """ Register the delta preprocessor. """
    register_callable = partial(
        libmetawear.mbl_mw_dataprocessor_delta_create, signal_id,
        cbindings.DeltaMode.DIFFERENTIAL, min_delta, None)
    return _register_data_processor(register_callable)


def _register_data_processor(register_callable: partial) -> int:
    wait_event = Event()
    processor_signal = 0

    def processor_handler(_, pointer):
        nonlocal processor_signal
        processor_signal = pointer
        wait_event.set()

    rms_handler = cbindings.FnVoid_VoidP_VoidP(processor_handler)
    register_callable(rms_handler)
    wait_event.wait()
    return processor_signal
