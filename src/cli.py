from typing import Dict, List
from src.device import MetaWearDevice
from src.metadata import MetadataDB


class CLI:

    def __init__(self, metadata_db_path: str):
        self.metadata_db = MetadataDB(metadata_db_path)

    def _input_username(self):
        username = input("Please provide a username: ")
        return username

    def _get_user_data(self, username: str) -> List[Dict]:
        devices = self.metadata_db.get_user_data
        return [device for device in devices if device]

    def _input_device_mac_address(self):
        mac_address = input("Please provide a mac address: ")
        return mac_address

    def _lookup_device(self, mac_address: str) -> Dict:
        device = self.metadata_db.lookup_device(mac_address)
        return device

    def log_in(self) -> Dict:
        username = self._input_username()
        devices = self._get_user_data(username)
        if devices:
            print(f'{username} has the following device configurations:')
            for i, device in enumerate(devices):
                print(f'Device {i+1}: {device}')
            use_device = input("Would you like to use one of them? (y/n) ")
            if use_device in 'Yy':
                device_id = int(input("Please provide the device id: "))
                device = devices[device_id - 1]
                print(f'Using device {device_id}')
                return device
        device = {}
        while not device:
            mac_address = input("Please provide a MAC address: ")
            device = self._configure_device(mac_address)
        return device

    def _configure_device(self, mac_address: str) -> Dict:
        device = MetaWearDevice(mac_address)
        acc_freq_options = device.return_acc_freq_options()
        gyro_freq_options = device.return_gyro_freq_options()
        acc_range_options = device.return_acc_range_options()
        gyro_range_options = device.return_gyro_range_options()
        print(f'{mac_address} has the following options:')
        print(f'Accelerometer Frequency: \n {acc_freq_options}')
        acc_freq = input("Please choose the index of the frequency you would like to use: ")
        try:
            acc_freq_options(int(acc_freq))
        except ValueError as exc:
            print(f'{exc}')
            return {}
        print(f'Accelerometer Range: \n {acc_range_options}')
        acc_range = input("Please choose the index of the range you would like to use: ")
        try:
            acc_range_options(int(acc_range))
        except ValueError as exc:
            print(f'{exc}')
            return {}
        print(f'Gyroscope Frequency: \n {gyro_freq_options}')
        gyro_freq = input("Please choose the index of the frequency you would like to use: ")
        try:
            gyro_freq_options(int(gyro_freq))
        except ValueError as exc:
            print(f'{exc}')
            return {}
        print(f'Gyroscope Range: \n {gyro_range_options}')
        gyro_range = input("Please choose the index of the range you would like to use: ")
        try:
            gyro_range_options(int(gyro_range))
        except ValueError as exc:
            print(f'{exc}')
            return {}

        device_config = {}
        device_config['mac_address'] = mac_address
        device_config['acc_freq'] = acc_freq_options(int(acc_freq))
        device_config['acc_range'] = acc_range_options(int(acc_range))
        device_config['gyro_freq'] = gyro_freq_options(int(gyro_freq))
        device_config['gyro_range'] = gyro_range_options(int(gyro_range))
        return device_config
