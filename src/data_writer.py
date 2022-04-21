"""
This module contains the DataWriter class used to write streamed sensor data to a csv file.
"""

import time
from typing import List


class DataWriter:
    """ Class used to write sensor data to a csv file. """

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

    def write_data(self, data: List[float]) -> None:
        """ Write sensor data to file. """
        self.file.write(_format_line(data))

    def close(self) -> None:
        """ Close the file. """
        self.file.close()

    def __del__(self):
        """ Close the file when the object is deleted. """
        self.close()


def _format_line(data: List[float]) -> str:
    """ Format float data to string with timestamp. """
    parse_floats = lambda x: f"{x:.4f}"
    return f"{time.time():.3f},{','.join(map(parse_floats, data))}\n"
