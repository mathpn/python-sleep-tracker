"""
Module with generic Device class and MetaWearDevice class.

The MetaWearDevice class provides a high-level interface configuring and streaming
sensor data from a MetaWear device.
"""

from abc import ABC, abstractmethod
from enum import Enum
from time import time
from typing import Callable, Optional

from mbientlab.metawear import cbindings, Const, libmetawear, MetaWear

from src.logger import LOG


class Device(ABC):

    @abstractmethod
    def start_streaming(self) -> None:
        pass
    
    @abstractmethod
    def stop_streaming(self) -> None:
        pass

    @abstractmethod
    def subscribe_to_accelerometer(self, acc_callback: Callable, data_processor_creator: Optional[Callable]) -> None:
        pass

    @abstractmethod
    def subscribe_to_gyroscope(self, gyro_callback: Callable, data_processor_creator: Optional[Callable]) -> None:
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
        self.device.on_disconnect = lambda status: LOG.warning(f"disconnected from device {self.mac_address}")
        self.acc_configured = False
        self.gyro_configured = False
        self.acc_callback = None
        self.gyro_callback = None

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
        try:
            self.device.connect()
        except Exception as exc:
            raise ConnectionError(f"Failed to connect to device {self.mac_address}: {exc}")
        time.sleep(1)
        libmetawear.mbl_mw_settings_set_connection_parameters(
            self.device.board, min_conn_interval, max_conn_interval, latency, timeout
        )

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

    def return_acc_freq_options(self) -> Optional[Enum]:
        model = self.acc_model
        enum_name = 'Accelerometer Frequency'
        if model == Const.MODULE_ACC_TYPE_BMI160:
            return _parse_option_enum(cbindings.AccBmi160Odr, enum_name)
        if model == Const.MODULE_ACC_TYPE_BMI270:
            return _parse_option_enum(cbindings.AccBmi270Odr, enum_name)
        if model == Const.MODULE_ACC_TYPE_BMA255):
            return _parse_option_enum(cbindings.AccBma255Odr, enum_name)
        if model == Const.MODULE_ACC_TYPE_MMA8452Q:
            return _parse_option_enum(cbindings.AccMma8452qOdr, enum_name)

        LOG.warning('Accelerometer not supported on this device')
        return None

    def return_acc_range_options(self) -> Optional[Enum]:
        model = self.acc_model
        enum_name = 'Accelerometer Range'
        if model in (
                Const.MODULE_ACC_TYPE_BMI160,
                Const.MODULE_ACC_TYPE_BMI270,
                Const.MODULE_ACC_TYPE_BMA255):
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

    def return_gyro_range_options(self) -> Optional[enum]:
        model = self.gyro_model
        enum_name = 'Gyroscope Range'
        if model == Const.MODULE_GYRO_TYPE_BMI160:
            return _parse_option_enum(cbindings.GyroBoschRange, enum_name)
        if model == Const.MODULE_GYRO_TYPE_BMI270:
            return _parse_option_enum(cbindings.GyroBoschRange, enum_name)
        LOG.warning('Gyroscope not supported on this device')
        return None

    def configure_accelerometer(self, acc_freq: Enum, acc_range: Enum) -> None:
        libmetawear.mbl_mw_acc_set_odr(self.device.board, acc_freq.value)
        libmetawear.mbl_mw_acc_set_range(self.device.board, acc_range.value)
        libmetawear.mbl_mw_acc_write_acceleration_config(self.device.board)
        self.acc_configured = True

    def configure_gyroscope(self, gyro_freq: Enum, gyro_range: Enum) -> None:
        gyro_model = self.gyro_model
        if gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            libmetawear.mbl_mw_gyro_bmi160_set_odr(self.device.board, gyro_freq.value)
            libmetawear.mbl_mw_gyro_bmi160_set_range(self.device.board, gyro_range.value)
        elif gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            libmetawear.mbl_mw_gyro_bmi270_set_odr(self.device.board, gyro_freq.value)
            libmetawear.mbl_mw_gyro_bmi270_set_range(self.device.board, gyro_range.value)
        else:
            LOG.warning('Gyroscope not supported on this device')
            return
        libmetawear.mbl_mw_gyro_write_config(self.device.board)
        self.gyro_configured = True

    def _get_acc_signal(self, data_processor_creator: Optional[Callable]) -> int:
        if not self.acc_configured:
            raise RuntimeError('Accelerometer must be configured before subscribing to signals')
        acc_signal_id = libmetawear.mbl_mw_acc_get_acceleration_data_signal(self.device.board)
        if data_processor_creator:
            acc_signal_id = data_processor_creator(acc_signal_id, 5, 0.005)
        return acc_signal_id

    def _get_gyro_signal(self, data_processor_creator: Optional[Callable]) -> int:
        if not self.gyro_configured:
            raise RuntimeError('Gyroscope must be configured before getting signal')
        gyro_model = self.gyro_model
        if gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            gyro_signal_id = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(self.device.board)
        elif gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
            gyro_signal_id = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(self.device.board)
        else:
            raise RuntimeError('Gyroscope not supported on this device')
        if data_processor_creator:
            gyro_signal_id = data_processor_creator(gyro_signal_id, 5, 3)
        return gyro_signal_id

    def subscribe_to_accelerometer(self, acc_callback: Callable, data_processor_creator: Optional[Callable]) -> None:
        self.acc_callback = cbindings.FnVoid_VoidP_DataP(acc_callback)
        acc_signal_id = self._get_acc_signal(data_processor_creator)
        libmetawear.mbl_mw_datasignal_subscribe(acc_signal_id, None, acc_callback)

    def subscribe_to_gyroscope(self, acc_callback: Callable, data_processor_creator: Optional[Callable]) -> None:
        self.acc_callback = cbindings.FnVoid_VoidP_DataP(acc_callback)
        gyro_signal_id = self._get_gyro_signal(data_processor_creator)
        libmetawear.mbl_mw_datasignal_subscribe(gyro_signal_id, None, acc_callback)

    def start_streaming(self) -> None:
        if not self.acc_callback or not self.gyro_callback:
            raise RuntimeError('No callback function provided to start streaming')
        libmetawear.mbl_mw_acc_enable_acceleration_sampling(self.device.board)
        libmetawear.mbl_mw_acc_start(self.device.board)
        gyro_model = self.gyro_model
        if gyro_model == Const.MODULE_GYRO_TYPE_BMI160:
            libmetawear.mbl_mw_gyro_bmi160_enable_rotation_sampling(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_start(self.device.board)
        elif gyro_model == Const.MODULE_GYRO_TYPE_BMI270:
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