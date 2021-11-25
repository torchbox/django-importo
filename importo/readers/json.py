import io
import json
from typing import Any, Iterable

from django.core.serializers.json import DjangoJSONEncoder

from .base import BaseReader


class SimpleJSONFileReader(BaseReader):
    """
    A reader that reads rows from a JSON file and returns them as dictionaries.
    Not suitable for large files, as the entire file contents is loaded into memory.
    """

    requires_file_input = True
    file_input_help = "The source data (JSON) to import."
    json_decoder = DjangoJSONEncoder

    def __init__(
        self,
        file: io.TextIOWrapper,
        start_row: int = None,
        stop_row: int = None,
    ) -> None:
        self.file = file
        super().__init__(start_row, stop_row)

    def fetch(self, start_row: int, stop_row: int) -> Iterable[dict]:
        with open(self.file, "rb") as f:
            data = json.loads(f.read(), cls=self.json_decoder)
        for i, row in enumerate(data, start=1):
            if start_row and i < start_row:
                continue
            yield row
            if stop_row and i == stop_row:
                break
