from functools import partial
import signal
import sys
import time
import multiprocessing as mp
import queue
from threading import Event
from typing import List, Optional, Tuple

from mbientlab.metawear import cbindings, libmetawear, MetaWear, parse_value
from mbientlab import warble


def disable_control_c(func):
    """ Decorator to disable KeyboardInterrupt exit, use with caution. """

    def wrapper(*args, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)

    return wrapper


class DataWriter:
    """ Class used to write sensor data to a file. """

    def __init__(self, filename: str, units: str, raw: bool = False):
        self.filename = filename
        self.units = units
        self.raw = raw
        self.file = open(f"{self.filename}", "w", buffering=1, encoding="utf-8")
        self._write_header()

    def _write_header(self):
        """ Write the header to the file. """
        if self.raw:
            self.file.write(f"timestamp,x_{self.units},y_{self.units},z_{self.units}\n")
            return
        self.file.write(f"time,diff_rss_{self.units}\n")            

    def _format_line(self, data: List[float]) -> str:
        """ Format float data to string. """
        parse_floats = lambda x: f"{x:.4f}"
        return f"{time.time():.3f},{','.join(map(parse_floats, data))}\n"

    def write_data(self, data: List[float]) -> None:
        """ Write sensor data to file. """
        self.file.write(self._format_line(data))

    def close(self) -> None:
        """ Close the file. """
        self.file.close()


class StreamDevice:
    """
    Wrapper class around MetaWear device with convenience methods to stream sensor data.

    Attributes:
        device (MetaWear): MetaWear device object.
        samples (int): Number of samples streamed.
        acc_queue (mp.Queue): Queue to store accelerometer data.
        gyro_queue (mp.Queue): Queue to store gyroscope data.
        raw (bool): If True, raw sensor data is streamed.
        preprocess (bool): Whether to preprocess data or stream raw sensor data.
        acc_callback (cbindings.FnVoid_VoidP_DataP): Accelerometer data callback.
        gyro_callback (cbindings.FnVoid_VoidP_DataP): Gyroscope data callback.
        acc (bool): Whether to stream accelerometer data.
        gyro (bool): Whether to stream gyroscope data.
        temperature (bool): Whether to stream temperature data.

    Methods:
        connect (): Connect to MetaWear device.
        register_sensors (): Configure and subscribe to sensor signals.
        stream_data (): Stream sensor data.
        stop_streaming (): Stop streaming sensor data.
    """
    def __init__(self, device, acc_queue: mp.Queue, gyro_queue: mp.Queue, raw: bool = False):
        self.device = device
        self.samples = 0
        self.acc_queue = acc_queue
        self.gyro_queue = gyro_queue
        self.raw = raw
        self.acc_callback = cbindings.FnVoid_VoidP_DataP(self._acc_data_handler())
        self.gyro_callback = cbindings.FnVoid_VoidP_DataP(self._gyro_data_handler())
        self.processor: Optional[int] = None
        self.acc = False
        self.gyro = False
        self.temperatue = False

    def _acc_data_handler(self):
        return partial(self._data_handler, data_queue=self.acc_queue, msg='acc')

    def _gyro_data_handler(self):
        return partial(self._data_handler, data_queue=self.gyro_queue, msg='gyro')

    def _data_handler(self, _, raw_data, data_queue: mp.Queue, msg: str) -> None:
        data = parse_value(raw_data)
        print(f'{msg}: {data}')
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
        self.device.on_disconnect = lambda status: print("disconnected")
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
        print("Streaming data...")
        libmetawear.mbl_mw_acc_start(self.device.board)
        libmetawear.mbl_mw_gyro_bmi160_start(self.device.board)

    def stop_streaming(self) -> None:
        """ Stop streaming data. """
        print("Stopping streaming...")
        if self.acc:
            print("removing acc callback")
            libmetawear.mbl_mw_acc_stop(self.device.board)
            libmetawear.mbl_mw_acc_disable_acceleration_sampling(self.device.board)
        if self.gyro:
            print("removing gyro callback")
            libmetawear.mbl_mw_gyro_bmi160_stop(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")
        time.sleep(1)
        print(f"streamed {self.samples} samples")
        self.samples = 0
        libmetawear.mbl_mw_debug_reset(self.device.board)

    def register_sensors(self, frequency: float, acc_range: float = 8.0) -> None:
        """ Configure and subscribe to sensor signals. """
        # TODO: Add temperature streaming
        # TODO: gyroscope frequency
        acc_signal = self._register_accelerometer(frequency, acc_range)
        gyro_signal = self._register_gyroscope()
        if not self.raw:
            acc_signal = _create_standard_preprocessor(acc_signal, 5, 0.01)
            gyro_signal = _create_standard_preprocessor(gyro_signal, 5, 5)
        self._subscribe_raw_sensor(acc_signal, self.acc_callback)
        self._subscribe_raw_sensor(gyro_signal, self.gyro_callback)

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

    def _subscribe_raw_sensor(self, sensor_signal, callback) -> None:
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
        cbindings.DeltaMode.ABSOLUTE, min_delta, None)
    return _register_data_processor(register_callable)


def _register_data_processor(register_callable: partial) -> int:
    wait_event = Event()
    processor_signal = 0

    def processor_handler(context, pointer):
        nonlocal processor_signal
        processor_signal = pointer
        print(f'{processor_signal = }')
        wait_event.set()

    rms_handler = cbindings.FnVoid_VoidP_VoidP(processor_handler)
    register_callable(rms_handler)
    wait_event.wait()
    print(f'out {processor_signal = }')
    return processor_signal


@disable_control_c
def consume_data_queue(event: Event, data_queue: mp.Queue, writer: DataWriter, sleep_time: int = 2) -> None:
    """ Consume a data queue with a writer."""
    while True:
        try:
            data = data_queue.get(timeout=1)
            event.clear()
            writer.write_data(data)
        except queue.Empty:
            print("No data in queue")
            event.set()
            time.sleep(sleep_time)


def main():
    address = sys.argv[1]
    device = MetaWear(address)

    acc_queue = mp.Queue(25000)
    gyro_queue = mp.Queue(25000)
    acc_queue_event = mp.Event()
    gyro_queue_event = mp.Event()

    main_event = Event()
    acc_writer = DataWriter("acc_data.csv", "(g/s)", raw=False)
    gyro_writer = DataWriter("gyro_data.csv", "(rad/s)", raw=False)
    stream = StreamDevice(device, acc_queue, gyro_queue, raw=False)
    stream.connect()
    stream.register_sensors(50.0, 8.0)

    stream_process = mp.Process(target=stream.stream_data())
    acc_write_process = mp.Process(target=consume_data_queue, args=(acc_queue_event, acc_queue, acc_writer))
    gyro_write_process = mp.Process(target=consume_data_queue, args=(gyro_queue_event, gyro_queue, gyro_writer))
    stream_process.start()
    acc_write_process.start()
    gyro_write_process.start()
    try:
        main_event.wait()
    except KeyboardInterrupt:
        print()
        if stream_process.is_alive():
            stream_process.close()
        stream.stop_streaming()
        print("waiting for write process to finish, please wait...")
        acc_queue.close()
        gyro_queue.close()
        acc_queue_event.wait(5)
        gyro_queue_event.wait(5)
        acc_write_process.kill()
        gyro_write_process.kill()
        acc_writer.close()
        gyro_writer.close()
        print("exiting")
        main_event.set()


if __name__ == "__main__":
    main()
