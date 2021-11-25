import csv
import io
import logging
from typing import Any, Iterable, Mapping

from .base import BaseReader


class SimpleCSVReader(BaseReader):
    """
    A reader that reads rows from a CSV file and returns them as dictionaries.
    Not suitable for large files, as the entire file contents is loaded into memory.
    """

    requires_file_input = True
    file_input_help = "The source data (CSV) to import."

    def __init__(
        self,
        file: io.TextIOWrapper,
        start_row: int = None,
        stop_row: int = None,
    ) -> None:
        self.file = file
        super().__init__(start_row, stop_row)

    def fetch(self, start_row: int, stop_row: int) -> Iterable:
        for i, row in enumerate(csv.DictReader(self.file), start=1):
            if start_row and i < start_row:
                continue
            yield row
            if stop_row and i == stop_row:
                break

    def sanitize_row(self, row: dict) -> Mapping[str, Any]:
        for name, value in row.copy().items():
            row[name] = self.sanitize_column_value(name, value)
        return row

    def sanitize_column_value(self, name: str, value: str) -> Any:
        value = value.strip()

        if not value:
            return value

        variant = value.lower()
        if variant == "null":
            return None
        if variant == "true":
            return True
        if variant == "false":
            return False

        variant = value.lstrip("-")
        if variant and variant[0] != "0":
            if variant.isdigit():
                return int(value)
            if variant.replace(".", "", 1).isdigit():
                return float(value)

        return value
