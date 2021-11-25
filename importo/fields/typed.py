import html
import uuid
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Sequence, Type
from urllib.parse import unquote_plus

from django.core.exceptions import ValidationError
from django.utils.html import strip_tags
from django.utils.translation import gettext_lazy as _

from importo.constants import NOT_SPECIFIED

from . import base, constants, error_codes, strategy_codes

if TYPE_CHECKING:
    from importo.commands import BaseImportCommand


class BaseTypedField(base.Field):
    return_type: Type = None

    def get_return_type(self) -> Type:
        if self.return_type is None:
            raise NotImplementedError(
                f"{type(self).__name__} is missing a 'return_type' value. "
                "Did you forget set this attribute on your field class?"
            )
        return self.return_type

    def to_python(self, value: Any) -> Any:
        """
        Return a version of `value` converted to the native python type for
        this field type. `value` might be a raw value (as received from a
        reader class), or a `fallback` value.
        """
        return_type = self.get_return_type()
        if isinstance(value, return_type):
            # No conversion needed
            return value
        try:
            # Attempt conversion
            return return_type(value)
        except (TypeError, ValueError):
            msg = self.error_messages[error_codes.INCOERCABLE] % {
                "value": value,
                "return_type": return_type,
            }
            raise ValidationError(msg, code=error_codes.INCOERCABLE)


class TextField(BaseTypedField):
    return_type = str
    default_fallback = ""

    on_max_length_exceeded_choices = [
        NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.TRIM_TO_FIT,
        strategy_codes.USE_FALLBACK,
    ]
    on_max_length_exceeded_default = strategy_codes.RAISE_ERROR

    default_error_messages = {
        strategy_codes.MAX_LENGTH_EXCEEDED: _(
            "The value is %(value_length)s characters long, which exceeds the %(max_length)s character limit."
        ),
    }

    def __init__(
        self,
        *,
        source: str = None,
        target_field: str = None,
        fallback: Optional[Any] = NOT_SPECIFIED,
        on_missing_value: Optional[str] = NOT_SPECIFIED,
        on_empty_value: Optional[str] = NOT_SPECIFIED,
        required: bool = False,
        modifiers: Optional[Sequence[Callable]] = None,
        strip_html: bool = False,
        strip_line_breaks: bool = False,
        max_length: int = None,
        on_max_length_exceeded: Optional[str] = NOT_SPECIFIED,
        error_messages: Optional[Mapping[str, str]] = None,
        validators: Optional[Sequence[callable]] = (),
        command: Optional["BaseImportCommand"] = None,
    ):
        if modifiers:
            self.modifiers = list(modifiers)
        else:
            self.modifiers = []
        self.strip_html = strip_html
        self.strip_line_breaks = strip_line_breaks
        self.max_length = max_length
        self.on_max_length_exceeded = on_max_length_exceeded
        super().__init__(
            source=source,
            target_field=target_field,
            fallback=fallback,
            on_missing_value=on_missing_value,
            on_empty_value=on_empty_value,
            required=required,
            error_messages=error_messages,
            validators=validators,
            command=command,
        )

    @property
    def on_max_length_exceeded(self):
        """
        Return the preferred strategy for handling values that exceed the maximum length.
        """
        if self._on_max_length_exceeded != NOT_SPECIFIED:
            return self._on_max_length_exceeded
        try:
            return self.command.on_max_length_exceeded
        except AttributeError:
            return self.on_empty_value_default

    @on_max_length_exceeded.setter
    def on_max_length_exceeded(self, value: Any):
        """
        Validate and set the preferred strategy for handling empty values for this field.
        """
        valid_choices = self.on_max_length_exceeded_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_max_length_exceeded' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_max_length_exceeded = value

    def to_python(self, value: Any) -> Any:
        value = unquote_plus(super().to_python(value))
        # Apply custom modifiers
        for modifier in self.modifiers:
            value = modifier(value)
        # Remove html
        if self.strip_html:
            for match_string in constants.REPLACE_WITH_SPACE:
                value = value.replace(match_string, " ")
            value = strip_tags(html.unescape(value))
        # Remove line breaks
        if self.strip_line_breaks:
            value = value.replace("\r\n", " ")
        value = value.strip()

    def clean(self, value):
        value = self.to_python()
        try:
            self.validate(value)
        except ValidationError as e:
            if e.code == error_codes.MAX_LENGTH_EXCEEDED:
                strategy = self.on_max_length_exceeded
                if strategy == strategy_codes.TRIM_TO_FIT:
                    value = value[: self.max_length]
                elif strategy == strategy_codes.USE_FALLBACK:
                    return self.get_fallback()
                elif callable(strategy):
                    return strategy(value)
                else:
                    raise
            else:
                raise

        self.run_validators(value)
        return value

    def validate(self, value) -> None:
        super().validate(value)
        if self.max_length and len(value) > self.max_length:
            msg = self.error_messages[error_codes.MAX_LENGTH_EXCEEDED] % {
                "value": value,
                "value_length": len(value),
                "max_length": self.max_length,
            }
            raise ValidationError(msg, code=error_codes.MAX_LENGTH_EXCEEDED)


class BooleanField(BaseTypedField):
    return_type = bool
    true_values = (True, "true", "1", 1, "yes")
    false_values = (False, "false", "0", 0, "no")

    def to_python(self, value: Any) -> bool:
        test_val = value
        if isinstance(value, str):
            test_val = value.lower()
        if test_val in self.true_values:
            return True
        if test_val in self.false_values:
            return False
        msg = self.error_messages[error_codes.INCOERCABLE] % {
            "value": value,
            "return_type": self.return_type,
        }
        raise ValidationError(msg, code=error_codes.INCOERCABLE)


class IntegerField(BaseTypedField):
    return_type = int


class FloatField(BaseTypedField):
    return_type = float


class UUIDField(BaseTypedField):
    return_type = uuid.UUID
