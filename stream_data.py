import sys
import time
from ctypes import c_void_p
from threading import Event

from mbientlab.metawear import MetaWear, parse_value, libmetawear
from mbientlab.metawear.cbindings import *
from mbientlab.warble import *


class StreamWriter:
    """ Class used to write sensor data to a file. """

    def __init__(self, filename: str):
        self.filename = filename
        self.acc_file = open(f'acc_{self.filename}', 'w', encoding='utf-8')
        self.gyro_file = open(f'gyro_{self.filename}', 'w', encoding='utf-8')
        self._write_header()

    def _write_header(self):
        """ Write the header to the file. """
        self.acc_file.write("timestamp,x (g/s),y (g/s),z (g/s)\n")
        self.gyro_file.write("timestamp,x (rad/s),y (rad/s),z (rad/s)\n")

    def write_acc_data(self, data: CartesianFloat):
        """ Write sensor data to file. """
        list_data = [data.x, data.y, data.z]
        str_data = map(str, list_data)
        line = f"{time.time():.3f},{','.join(str_data)}\n"
        self.acc_file.write(line)

    def write_gyro_data(self, data: CartesianFloat):
        """ Write sensor data to file. """
        list_data = [data.x, data.y, data.z]
        str_data = map(str, list_data)
        line = f"{time.time():.3f},{','.join(str_data)}\n"
        self.gyro_file.write(line)

    def close(self):
        """ Close the file. """
        self.acc_file.close()
        self.gyro_file.close()

class StreamDevice:
    def __init__(self, device, stream_writer: StreamWriter):
        self.device = device
        self.samples = 0
        self.stream_writer = stream_writer
        self.acc_callback = FnVoid_VoidP_DataP(self._acc_data_handler)
        self.gyro_callback = FnVoid_VoidP_DataP(self._gyro_data_handler)
        self.acc = False
        self.gyro = False
        self.temperatue = False

    def _acc_data_handler(self, _, data):
        #print(parse_value(data))
        self.stream_writer.write_acc_data(parse_value(data))
        #self.samples += 1

    def _gyro_data_handler(self, _, data):
        #print(parse_value(data))
        self.stream_writer.write_gyro_data(parse_value(data))
        #self.samples += 1

    def connect(self, min_conn_interval: float = 7.5, max_conn_interval: float = 7.5, latency: int = 0, timeout: int = 6000):
        self.device.on_disconnect = lambda status: print("disconnected")
        self.device.connect()
        time.sleep(1)
        if not self.device.is_connected:
            raise ConnectionError("Failed to connect to MetaWear board")
        libmetawear.mbl_mw_settings_set_connection_parameters(
            self.device.board, min_conn_interval, max_conn_interval, latency, timeout)
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
        print('Stopping streaming...')
        if self.acc:
            print('removing acc callback')
            libmetawear.mbl_mw_acc_stop(self.device.board)
            libmetawear.mbl_mw_acc_disable_acceleration_sampling(self.device.board)
        if self.gyro:
            print('removing gyro callback')
            libmetawear.mbl_mw_gyro_bmi160_stop(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")
        print('closing file connection')
        self.stream_writer.close()
        print(f'streamed {self.samples} samples')
        self.samples = 0

    def register_fusion(self, frequency: float, acc_range: float = 8.0) -> None:
        acc_signal = self._register_accelerometer(frequency, acc_range)
        gyro_signal = self._register_gyroscope()
        self._subscribe_sensors(acc_signal, gyro_signal)

    def _register_accelerometer(self, frequency: float = 25.0, value_range: float = 8.0) -> int:
        """ Register the accelerometer data signal. """
        libmetawear.mbl_mw_acc_set_odr(self.device.board, frequency)
        libmetawear.mbl_mw_acc_set_range(self.device.board, value_range)
        libmetawear.mbl_mw_acc_write_acceleration_config(self.device.board)
        signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(self.device.board)
        self.acc = True
        return signal

    def _register_gyroscope(self) -> int:
        """ Register the gyroscope data signal """
        libmetawear.mbl_mw_gyro_bmi160_set_odr(self.device.board, GyroBoschOdr._50Hz)
        libmetawear.mbl_mw_gyro_bmi160_set_range(self.device.board, GyroBoschRange._1000dps)
        libmetawear.mbl_mw_gyro_bmi160_write_config(self.device.board)
        signal = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(self.device.board)
        self.gyro = True
        return signal

    def _subscribe_sensors(self, acc_signal, gyro_signal):
        libmetawear.mbl_mw_datasignal_subscribe(acc_signal, None, self.acc_callback)
        libmetawear.mbl_mw_datasignal_subscribe(gyro_signal, None, self.gyro_callback)



def main():
    import faulthandler
    faulthandler.enable(all_threads=True)

    address = sys.argv[1]
    device = MetaWear(address)

    event = Event()
    stream_writer = StreamWriter("data.csv")
    stream = StreamDevice(device, stream_writer)
    stream.connect()
    stream.register_fusion(50.0, 8.0)
    stream.stream_data()
    try:
        event.wait()
    except KeyboardInterrupt:
        stream.stop_streaming()
        event.clear()


if __name__ == "__main__":
    main()
