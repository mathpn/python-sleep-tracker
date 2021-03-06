"""
Module with generic Device class and MetaWearDevice class.

The MetaWearDevice class provides a high-level interface configuring and streaming
sensor data from a MetaWear device.
"""

from abc import ABC, abstractmethod
from enum import Enum
from functools import partial
import time
from typing import Callable, Optional
from threading import Event

from mbientlab.metawear import cbindings, Const, libmetawear, MetaWear

from src.logger import LOG


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



class Device(ABC):

    @abstractmethod
    def connect(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def start_streaming(self) -> None:
        pass

    @abstractmethod
    def stop_streaming(self) -> None:
        pass

    @abstractmethod
    def subscribe_to_accelerometer(
        self, acc_callback: Callable, data_processor_creator: Optional[Callable] = None) -> None:
        pass

    @abstractmethod
    def subscribe_to_gyroscope(
        self, gyro_callback: Callable, data_processor_creator: Optional[Callable] = None) -> None:
        pass


class MetaWearDevice(Device):
    """
    High-level interface for a MetaWear device.

    Attributes:
        acc_model (int): Accelerometer model.
        gyro_model (int): Gyroscope model.
        mac_address (str): MAC address of the device.
        device (MetaWear): MetaWear device object.
        acc_configured (bool): Flag indicating if accelerometer is configured.
        gyro_configured (bool): Flag indicating if gyroscope is configured.
        acc_callback (Callable): Accelerometer callback function.
        gyro_callback (Callable): Gyroscope callback function.

    Methods:
        start_streaming: Start streaming sensor data.
        stop_streaming: Stop streaming sensor data.
        subscribe_to_accelerometer: Subscribe to accelerometer data.
        subscribe_to_gyroscope: Subscribe to gyroscope data.
        return_acc_freq_options: Return accelerometer frequency options.
        return_acc_range_options: Return accelerometer range options.
        return_gyro_freq_options: Return gyroscope frequency options.
        return_gyro_range_options: Return gyroscope range options.
    """

    def __init__(self, mac_address: str):
        self.mac_address = mac_address
        self.device = MetaWear(self.mac_address)
        self.device.on_disconnect = self._disconnect_callback
        self.connected = False
        self.acc_configured = False
        self.gyro_configured = False
        self.acc_callback = None
        self.gyro_callback = None
        self.device_info = {}

    def _disconnect_callback(self, status):
        LOG.warning(f"disconnected from device {self.mac_address}: {status}")
        self.connected = False

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
        try:
            self.device.connect()
        except Exception as exc:
            raise ConnectionError(f"Failed to connect to device {self.mac_address}: {exc}")
        time.sleep(1)
        libmetawear.mbl_mw_settings_set_connection_parameters(
            self.device.board, min_conn_interval, max_conn_interval, latency, timeout
        )
        self.connected = True

    def disconnect(self):
        libmetawear.mbl_mw_debug_reset(self.device.board)
        self.device.disconnect()

    @property
    def acc_model(self):
        if 'acc_model' not in self.device_info:
            acc_model =  libmetawear.mbl_mw_metawearboard_lookup_module(
                self.device.board, cbindings.Module.ACCELEROMETER
            )
            self.device_info['acc_model'] = acc_model
            print('saving acc model')
        return self.device_info['acc_model']

    @property
    def gyro_model(self):
        if 'gyro_model' not in self.device_info:
            gyro_model = libmetawear.mbl_mw_metawearboard_lookup_module(
                self.device.board, cbindings.Module.GYRO
            )
            self.device_info['gyro_model'] = gyro_model
            print('saving gyro model')
        return self.device_info['gyro_model']

    def return_acc_freq_options(self) -> Optional[Enum]:
        enum_name = 'Accelerometer Frequency'
        if self.acc_model == Const.MODULE_ACC_TYPE_BMI160:
            return _parse_option_enum(cbindings.AccBmi160Odr, enum_name)
        if self.acc_model == Const.MODULE_ACC_TYPE_BMI270:
            return _parse_option_enum(cbindings.AccBmi270Odr, enum_name)
        if self.acc_model == Const.MODULE_ACC_TYPE_BMA255:
            return _parse_option_enum(cbindings.AccBma255Odr, enum_name)
        if self.acc_model == Const.MODULE_ACC_TYPE_MMA8452Q:
            return _parse_option_enum(cbindings.AccMma8452qOdr, enum_name)

        LOG.warning('Accelerometer not supported on this device')
        return None

    def return_acc_range_options(self) -> Optional[Enum]:
        enum_name = 'Accelerometer Range'
        if self.acc_model in (
                Const.MODULE_ACC_TYPE_BMI160,
                Const.MODULE_ACC_TYPE_BMI270,
                Const.MODULE_ACC_TYPE_BMA255):
            return _parse_option_enum(cbindings.AccBoschRange, enum_name)
        if self.acc_model == Const.MODULE_ACC_TYPE_MMA8452Q:
            return _parse_option_enum(cbindings.AccMma8452qRange, enum_name)

        LOG.warning('Accelerometer not supported on this device')
        return None

    def return_gyro_freq_options(self) -> Optional[Enum]:
        enum_name = 'Gyroscope Frequency'
        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            return _parse_option_enum(cbindings.GyroBoschOdr, enum_name)
        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            return _parse_option_enum(cbindings.GyroBoschOdr, enum_name)
        LOG.warning('Gyroscope not supported on this device')
        return None

    def return_gyro_range_options(self) -> Optional[Enum]:
        enum_name = 'Gyroscope Range'
        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            return _parse_option_enum(cbindings.GyroBoschRange, enum_name)
        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            return _parse_option_enum(cbindings.GyroBoschRange, enum_name)
        LOG.warning('Gyroscope not supported on this device')
        return None

    def configure_accelerometer(self, acc_freq: Enum, acc_range: Enum) -> None:
        # Getting values from enum name strings is horrible but necessary
        # since dataprocessor is not available when configuring accelerometer
        # using lower level classes (like we do for the gyroscope)
        # Enums are used anyway to ensure valid options.
        acc_freq_float = _parse_enum_name(acc_freq.name)
        acc_range_float = _parse_enum_name(acc_range.name)
        libmetawear.mbl_mw_acc_set_odr(self.device.board, acc_freq_float)
        libmetawear.mbl_mw_acc_set_range(self.device.board, acc_range_float)
        self.acc_configured = True

    def configure_gyroscope(self, gyro_freq: Enum, gyro_range: Enum) -> None:
        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            libmetawear.mbl_mw_gyro_bmi160_set_odr(self.device.board, gyro_freq.value)
            libmetawear.mbl_mw_gyro_bmi160_set_range(self.device.board, gyro_range.value)
            libmetawear.mbl_mw_gyro_bmi160_write_config(self.device.board)
        elif self.gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            libmetawear.mbl_mw_gyro_bmi270_set_odr(self.device.board, gyro_freq.value)
            libmetawear.mbl_mw_gyro_bmi270_set_range(self.device.board, gyro_range.value)
            libmetawear.mbl_mw_gyro_bmi270_write_config(self.device.board)
        else:
            LOG.warning('Gyroscope not supported on this device')
            return
        self.gyro_configured = True

    def _get_acc_signal(self, data_processor_creator: Optional[Callable] = None) -> int:
        if not self.acc_configured:
            raise RuntimeError('Accelerometer must be configured before subscribing to signals')
        acc_signal_id = libmetawear.mbl_mw_acc_get_acceleration_data_signal(self.device.board)
        if data_processor_creator:
            acc_signal_id = data_processor_creator(acc_signal_id, 5, 0.01)
        return acc_signal_id

    def _get_gyro_signal(self, data_processor_creator: Optional[Callable]) -> int:
        if not self.gyro_configured:
            raise RuntimeError('Gyroscope must be configured before getting signal')
        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            gyro_signal_id = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(self.device.board)
        elif self.gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            gyro_signal_id = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(self.device.board)
        else:
            raise RuntimeError('Gyroscope not supported on this device')
        if data_processor_creator:
            gyro_signal_id = data_processor_creator(gyro_signal_id, 5, 3)
        return gyro_signal_id

    def subscribe_to_accelerometer(
            self, acc_callback: Callable,
            data_processor_creator: Optional[Callable] = _create_standard_preprocessor) -> None:
        self.acc_callback = cbindings.FnVoid_VoidP_DataP(acc_callback)
        acc_signal_id = self._get_acc_signal(data_processor_creator)
        libmetawear.mbl_mw_datasignal_subscribe(acc_signal_id, None, self.acc_callback)

    def subscribe_to_gyroscope(
            self, gyro_callback: Callable,
            data_processor_creator: Optional[Callable] = _create_standard_preprocessor) -> None:
        self.gyro_callback = cbindings.FnVoid_VoidP_DataP(gyro_callback)
        gyro_signal_id = self._get_gyro_signal(data_processor_creator)
        libmetawear.mbl_mw_datasignal_subscribe(gyro_signal_id, None, self.gyro_callback)

    def start_streaming(self) -> None:
        libmetawear.mbl_mw_acc_enable_acceleration_sampling(self.device.board)
        libmetawear.mbl_mw_acc_start(self.device.board)

        if self.gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            libmetawear.mbl_mw_gyro_bmi160_enable_rotation_sampling(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_start(self.device.board)
        elif self.gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            libmetawear.mbl_mw_gyro_bmi270_enable_rotation_sampling(self.device.board)
            libmetawear.mbl_mw_gyro_bmi270_start(self.device.board)
        else:
            raise RuntimeError('Gyroscope not supported on this device')

    def stop_streaming(self) -> None:
        LOG.info("removing acc callback")
        libmetawear.mbl_mw_acc_stop(self.device.board)
        libmetawear.mbl_mw_acc_disable_acceleration_sampling(self.device.board)
        LOG.info("removing gyro callback")
        gyro_model = self.gyro_model
        if gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            libmetawear.mbl_mw_gyro_bmi160_stop(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(self.device.board)
        elif gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            libmetawear.mbl_mw_gyro_bmi270_stop(self.device.board)
            libmetawear.mbl_mw_gyro_bmi270_disable_rotation_sampling(self.device.board)
        else:
            raise RuntimeError('Gyroscope not supported on this device')
        time.sleep(1)
        libmetawear.mbl_mw_debug_reset(self.device.board)


def _parse_option_enum(cbinding_enum, enum_name: str) -> Enum:
    """
    Helper function to parse the enum values from the cbindings module.
    """
    options = {
        option.strip('_').replace('_', '.'): value for option, value
        in cbinding_enum.__dict__.items() if '__' not in option
    }
    options_enum = Enum(enum_name, options)
    return options_enum


def _parse_enum_name(enum_name: str) -> float:
    """ Helper to parse a enum option name to a valid float. """
    clean_string = ''.join(ch for ch in enum_name if ch.isdigit())
    value = float(clean_string.replace('_', '.'))
    if value < 0 or value is None:
        raise ValueError('Invalid enum value: {}'.format(value))
    return value
