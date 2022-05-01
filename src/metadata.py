import sqlite3
from typing import Dict, List, Optional


class MetadataDB:
    """ Simple metadata database class. """
    # TODO device configuration should be set outside and constrained to valid values
    # TODO integrate with data processing
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = 1")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT)
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS user_devices (
                user_id INTEGER, device_id INTEGER, PRIMARY KEY (user_id, device_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (device_id) REFERENCES devices(device_id))
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (device_id INTEGER PRIMERY KEY, mac_address TEXT,
            raw_data INT, acc_frequency INT, gyro_frequency INT, acc_range INT, gyro_range INT)
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (session INTEGER PRIMARY KEY, user_id INT, timestamp INT,
            device_id INT, processed INT DEFAULT 0, FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (device_id) REFERENCES devices(device_id))
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS sessions_user_id ON sessions (user_id)")
        self.conn.commit()

    def add_user(self, username: str) -> None:
        try:
            self.conn.execute("INSERT INTO users (username) VALUES (??)", (username,))
        except sqlite3.IntegrityError:
            raise ValueError("Username already exists")
        self.conn.commit()

    def get_user_data(self, username: str) -> List[Dict]:
        cur = self.conn.execute("""
            SELECT devices.mac_address, devices.raw_data, devices.sample_frequency, devices.acc_range, devices.gyro_range
            FROM users
            INNER JOIN user_devices ON users.user_id = user_devices.user_id
            INNER JOIN devices ON devices.device_id = user_devices.device_id
            WHERE users.username = ?
        """, (username,))
        devices = cur.fetchall()
        return [_dict_from_row(device) for device in devices]


    def lookup_device(self, mac_address: str) -> Dict:
        cursor = self.conn.execute("SELECT * FROM devices WHERE mac_address = ?", (mac_address,))
        return _dict_from_row(cursor.fetchone())

    def add_device(self, device_config: Dict) -> None:
        self.conn.execute("""
            INSERT INTO devices (mac_address, raw_data, sample_frequency, acc_range, gyro_range)
            VALUES (?, ?, ?, ?, ?)", (mac_address, raw_data, acc_frequency, gyro_frequency, acc_range, gyro_range))
        """, (
                device_config['mac_address'], device_config['raw_data'],
                device_config['sample_frequency'], device_config['acc_range'],
                device_config['gyro_range']
            )
        )
        self.conn.commit()

    def add_session(self, user_id: int, timestamp: int, device_id: int) -> None:
        try:
            self.conn.execute("""
                INSERT INTO sessions (user_id, timestamp, device_id)
                VALUES (?, ?, ?)
            """, (user_id, timestamp, device_id))
        except sqlite3.IntegrityError:
            # TODO handle this error
            raise ValueError("invalid device_id or user_id")
        self.conn.commit()

    def get_sessions(self, user_id: int) -> sqlite3.Cursor:
        return self.conn.execute("""
            SELECT * FROM sessions WHERE user_id = ?
        """, (user_id,))


def _dict_from_row(row):
    if not row:
        return {}
    return dict(zip(row.keys(), row))