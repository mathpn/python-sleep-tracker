import sqlite3
from typing import Optional


class MetadataDB:
    """ Simple metadata database class. """
    # TODO device configuration should be set outside and constrained to valid values
    # TODO integrate with data processing
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
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

    def lookup_device(self, mac_address: str) -> Optional[sqlite3.Row]:
        cursor = self.conn.execute("SELECT * FROM devices WHERE mac_address = ?", (mac_address,))
        return cursor.fetchone()

    def add_device(
            self, mac_address: int, raw_data: bool, sample_frequency: int,
            acc_range: int, gyro_range: int) -> None:
        self.conn.execute("""
            INSERT INTO devices (mac_address, raw_data, sample_frequency, acc_range, gyro_range)
            VALUES (?, ?, ?, ?, ?)", (mac_address, raw_data, sample_frequency, acc_range, gyro_range))
        """, (mac_address, raw_data, sample_frequency, acc_range, gyro_range))
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
