from datetime import date, datetime
from typing import Any

from django.core.exceptions import ValidationError
from django.utils import dateparse

from importo.utils.datetime import timestamp_to_datetime

from . import error_codes, typed


class DateField(typed.BaseTypedField):
    """
    A field that converts epoch timestamps or ISO-formatted date string values
    into `datetime.date` objects.
    """

    return_type = date

    def to_python(self, value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, int):
            return timestamp_to_datetime(value).date()
        if isinstance(value, str):
            if value.isdigit() or value[0] == "-" and value[1:].isdigit():
                return timestamp_to_datetime(int(value)).date()
            try:
                return self.parse_from_string(value)
            except ValueError:
                return None
        msg = self.error_messages[error_codes.INCOERCABLE] % {
            "value": value,
            "return_type": date,
        }
        raise ValidationError(msg, code=error_codes.INCOERCABLE)

    def parse_from_string(self, value: str) -> date:
        """
        Returns a ``datetime.date`` object matching the supplied string value,
        or `None` if it is invalid or incorrectly formatted.
        """
        return dateparse.parse_date(value)


class DateTimeField(typed.BaseTypedField):
    """
    A field that converts epoch timestamps or ISO-formatted datetime string
    values into ``datetime.datetime`` objects.
    """

    return_type = datetime

    def to_python(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, int):
            return timestamp_to_datetime(value)
        if isinstance(value, str):
            if value.isdigit() or value[0] == "-" and value[1:].isdigit():
                return timestamp_to_datetime(int(value))
            try:
                return self.parse_from_string(value)
            except ValueError:
                return None
        msg = self.error_messages[error_codes.INCOERCABLE] % {
            "value": value,
            "return_type": datetime,
        }
        raise ValidationError(msg, code=error_codes.INCOERCABLE)

    def parse_from_string(self, value: str) -> datetime:
        """
        Returns a `datetime` object matching the supplied string value,
        or `None` if it is invalid or incorrectly formatted.
        """
        return dateparse.parse_datetime(value)
