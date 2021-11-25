import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from importo.fields.base import BaseParsedField
from importo.fields.constants import CLEAN_COST_HIGH
from importo.wagtail.parsers.streamfield import StreamFieldContentParser


class StreamContentField(BaseParsedField):
    # TODO: Make this swappable via a setting
    default_parser = StreamFieldContentParser

    clean_cost = CLEAN_COST_HIGH
    default_fallback = []

    def __init__(self, *args, convert_to_string: bool = True, **kwargs):
        self.convert_to_string = convert_to_string
        super().__init__(*args, **kwargs)

    def to_python(self, value: Any) -> str:
        value = super().to_python(value)
        if self.is_empty(value):
            return self.default_fallback
        if self.convert_to_string:
            # Convert structured data to a JSON string
            return json.dumps(value, cls=DjangoJSONEncoder)
        return value
