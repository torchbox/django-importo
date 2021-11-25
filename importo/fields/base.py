from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Sequence, Tuple

from django.core.exceptions import ValidationError
from django.db.models import Model
from django.utils.translation import gettext_lazy as _

from importo.constants import NOT_SPECIFIED
from importo.errors import SkipField, SkipRow
from importo.parsers import BaseParser
from importo.utils.classes import CommandBoundObject, CopyableMixin
from importo.utils.values import ValueExtractionError, extract_row_value

from . import constants, error_codes, strategy_codes

if TYPE_CHECKING:
    from importo.commands import BaseImportCommand


class EmptyValueError(ValueError):
    """
    Raised when a field finds an empty value in the source data, when it expected a non-empty value.
    """

    pass


class Field(CopyableMixin, CommandBoundObject):
    """
    A base class for all import fields, that applies no cleaning or additional
    processing to the value returned by the reader.
    """

    # used when field is missing or empty and no fallback provided
    default_fallback = None
    on_missing_value_choices = [
        NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.USE_FALLBACK,
        strategy_codes.SKIP_FIELD,
        strategy_codes.SKIP_ROW,
    ]
    on_missing_value_default = strategy_codes.USE_FALLBACK
    on_empty_value_choices = [
        NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.USE_FALLBACK,
        strategy_codes.SKIP_FIELD,
        strategy_codes.SKIP_ROW,
    ]
    on_empty_value_default = strategy_codes.USE_FALLBACK

    # used to determine the order in which fields are cleaned
    clean_cost = constants.CLEAN_COST_LOW

    # as django.forms.Field
    empty_values = list(constants.EMPTY_VALUES)
    default_validators = []
    default_error_messages = {
        error_codes.INVALID: _("'%(value)s' is not a valid value for this field type."),
        error_codes.INCOERCABLE: _(
            "Could not coerce '%(value)s' to type %(return_type)s."
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
        required: bool = True,
        error_messages: Optional[Mapping[str, str]] = None,
        validators: Optional[Sequence[callable]] = (),
        command: Optional["BaseImportCommand"] = None,
    ):
        self.name = None
        self.source = source
        self.target_field = target_field
        self.required = required

        self._validate_option_values(
            required, fallback, on_missing_value, on_empty_value
        )

        if required:
            self.on_missing_value = NOT_SPECIFIED
            self.on_empty_value = NOT_SPECIFIED
            self.fallback = NOT_SPECIFIED
        else:
            self.on_missing_value = on_missing_value
            self.on_empty_value = on_empty_value
            self.fallback = fallback

        messages = {}
        for c in reversed(self.__class__.__mro__):
            messages.update(getattr(c, "default_error_messages", {}))
        messages.update(error_messages or {})
        self.error_messages = messages

        self.validators = [*self.default_validators, *validators]
        super().__init__(command)

    def _validate_option_values(
        self, required, fallback, on_missing_value, on_empty_value
    ) -> None:
        if required:
            for value, option_name in (
                (fallback, "fallback"),
                (on_missing_value, "on_missing_value"),
                (on_empty_value, "on_empty_value"),
            ):
                if value != NOT_SPECIFIED:
                    raise TypeError(
                        f"{repr(self)}: Using 'required=True' makes the '{option_name}' value redundant "
                        "(errors are always raised for missing or empty values)."
                    )

    def __repr__(self):
        return f"<{type(self).__name__} source='{self.source}' target_field='{self.target_field}'>"

    def __str__(self):
        return self.__repr__()

    @property
    def on_missing_value(self):
        """
        Return the preferred strategy for handling missing values for this field.
        """
        if self._on_missing_value != NOT_SPECIFIED:
            return self._on_missing_value
        try:
            return self.command.on_missing_value
        except AttributeError:
            return self.on_missing_value_default

    @on_missing_value.setter
    def on_missing_value(self, value: Any):
        """
        Validate and set the preferred strategy for handling missing values for this field.
        """
        valid_choices = self.on_missing_value_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_missing_value' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_missing_value = value

    @property
    def on_empty_value(self):
        """
        Return the preferred strategy for handling empty values for this field.
        """
        if self._on_empty_value != NOT_SPECIFIED:
            return self._on_empty_value
        try:
            return self.command.on_empty_value
        except AttributeError:
            return self.on_empty_value_default

    @on_empty_value.setter
    def on_empty_value(self, value: Any):
        """
        Validate and set the preferred strategy for handling empty values for this field.
        """
        valid_choices = self.on_empty_value_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_empty_value' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_empty_value = value

    def is_empty(self, value: Any) -> bool:
        return value in self.empty_values

    def to_python(self, value: Any) -> Any:
        """
        Return ``value`` converted to the relevant Python type for
        this field type. By default, ``value`` is returned unchanged.
        """
        return value

    def clean(self, value: Any) -> Any:
        """
        Ensures ``value`` is converted to the relevant Python type for this
        field type and validates that value. If no `ValidationError` is
        raised, the validated value is returned.
        """
        value = self.to_python(value)
        self.validate(value)
        self.run_validators(value)
        return value

    def get_fallback(self) -> Any:
        """
        Return a value to use when ``self.source`` is missing from the
        source data, or is present, but with an 'empty' value. The value will
        sent to `to_python()` before being used, but won't be validated.
        """
        value = self.fallback
        if value == NOT_SPECIFIED:
            value = self.default_fallback
        if callable(value):
            value = value()
        return value

    def validate(self, value: Any) -> None:
        """
        Override this method to apply validation specific to a custom field
        type. The method should simply check the value and raise a
        `ValidationError`.
        """
        pass

    def run_validators(self, value: Any) -> None:
        if value in self.empty_values:
            return
        errors = []
        for v in self.validators:
            try:
                v(value)
            except ValidationError as e:
                if hasattr(e, "code") and e.code in self.error_messages:
                    e.message = self.error_messages[e.code]
                errors.extend(e.error_list)
        if errors:
            raise ValidationError(errors)

    def contribute_to_cleaned_data(
        self, cleaned_data: Dict[str, Any], raw_data: Any, is_new: bool
    ) -> None:
        """
        Updates the supplied `cleaned_data` dict with cleaned values extracted from `raw_data`.
        Note: The in-memory `cleaned_data` dict is modified in-place, and no value is returned.

        Override this method when a field needs to extract, clean or set multiple values. By
        default, fields extract a single value and clean it for use as a model field value.
        But, unlike Django Form fields, import fields might need to combine data from several
        different colums, or the cleaned value might need to affect more than one attribute
        on the target model instance (or maybe even both).
        """
        lookup_value = self.source or self.name
        target_attr = self.target_field or self.name
        cleaned_data[target_attr] = self.extract_and_clean_value(lookup_value, raw_data)

    def extract_and_clean_value(self, lookup_value: str, raw_data: Any):
        """
        Attempts to extract a single value from `raw_data`, apply transformation or
        validation as necessary, and return the final 'clean' value.

        Most commonly, this method will raise a `ValidationError` when there is a
        problem. But, depending on the various error-handling strategies applied to
        the field, it might also raise `SkipField`, `SkipRow`, or some other
        exception from 'further down' the processing chain.
        """

        # Reraise ValueExtractionError and EmptyValueError as ValidationErrors
        # with more helpful messages, but allow all other exceptions to 'bubble up'
        try:
            raw_value, value_requires_cleaning = self.extract_value(
                lookup_value, raw_data
            )
        except ValueExtractionError:
            raise ValidationError(
                f"{type(self)} '{self.name}' could not extract '{lookup_value}' from the source data.",
                code=error_codes.VALUE_MISSING,
            )
        except EmptyValueError:
            raise ValidationError(
                f"{type(self)} '{self.name}' unexpectedly found an empty value for '{lookup_value}'.",
                code=error_codes.VALUE_EMPTY,
            )

        if not value_requires_cleaning:
            return raw_value
        return self.clean(raw_value)

    def extract_value(self, key: str, raw_data: Any) -> Tuple(Any, bool):
        """
        Attempt to extract a value from `raw_data` using `key`. If the value is missing
        or empty, raise some kind of error, or return a different value according to the
        'on_missing_value' and 'on_empty_value' strategy values for this field.

        Returns a two-tuple, where the first item is the value that should be
        used, and the second is a boolean indicating whether the value should undergo
        validation and/or conversion by the field's `clean()`and `to_python()` methods.
        For example, a raw extracted value almost certainly should, but a fallback
        value, or value returned by custom 'on_missing_value' handler are under
        developer control, and so do not need to be validated/converted.

        May raise `ValueExtractionError` if the 'on_missing_value' strategy is
        'RAISE_ERROR' (the default behaviour for 'required' fields).

        May raise `EmptyValueError` if the 'on_empty_value' strategy is
        'RAISE_ERROR' (the default behaviour for 'required' fields).
        """
        requires_cleaning = True
        try:
            value = extract_row_value(key, raw_data)
        except ValueExtractionError:
            strategy = self.on_missing_value
            if callable(strategy):
                value = strategy()
                requires_cleaning = False
            elif strategy == strategy_codes.USE_FALLBACK:
                value = self.get_fallback()
                requires_cleaning = False
            elif strategy == strategy_codes.SKIP_FIELD:
                raise SkipField
            elif strategy == strategy_codes.SKIP_ROW:
                raise SkipRow
            else:
                raise

        if self.is_empty(value):
            strategy = self.on_empty_value
            if callable(strategy):
                value = strategy(value)
                requires_cleaning = False
            elif strategy == strategy_codes.USE_FALLBACK:
                value = self.get_fallback(value)
                requires_cleaning = False
            elif strategy == strategy_codes.SKIP_FIELD:
                raise SkipField
            elif strategy == strategy_codes.SKIP_ROW:
                raise SkipRow
            else:
                raise EmptyValueError

            return value, requires_cleaning

    def update_object(
        self, obj: Model, cleaned_data: Dict[str, Any], is_new: bool
    ) -> None:
        """
        Updates the relevant field values on the target model instance (`obj`) according
        to the values in `cleaned_data`. Note: The in-memory `obj` is modified in-place,
        and no value is returned.

        By default, fields update a single field value on a model instance (typically
        using 'value' they were responsible for cleaning). But, unlike Django Form fields,
        import fields may need to update more than one field, and this method can be
        overridden in those cases.
        """
        target_attr = self.target_field or self.name
        if target_attr not in cleaned_data:
            return None
        self.set_object_value(obj, target_attr, cleaned_data[target_attr])

    def set_object_value(self, obj, attribute_name, value):
        setattr(obj, attribute_name, value)


class NoopField(Field):
    pass


class ListField(Field):

    clean_cost = constants.CLEAN_COST_MEDIUM
    default_fallback = []

    def __init__(
        self,
        sub_fields: Dict[str, Field],
        *,
        source: str,
        target_field: str,
        flatten: bool = False,
        fallback: Optional[Any] = NOT_SPECIFIED,
        on_missing_value: Optional[str] = NOT_SPECIFIED,
        on_empty_value: Optional[str] = NOT_SPECIFIED,
        required: bool = True,
        error_messages: Optional[Mapping[str, str]] = None,
        validators: Optional[Sequence[callable]] = (),
        command: Optional["BaseImportCommand"] = None,
    ):
        if flatten and len(sub_fields) > 1:
            raise ValueError(
                "The 'flatten' option can only be used in combination "
                "with a single subfield."
            )
        self.flatten = flatten
        self.sub_fields = {}
        for name, field in sub_fields.items():
            field.name = name
            self.sub_fields[name] = field

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

    def bind_to_command(self, command: "BaseImportCommand") -> None:
        super().bind_to_command(command)
        for field in self.sub_fields.values():
            field.bind_to_command(command)

    def to_python(self, value):
        return_value = []
        for v in value:
            try:
                cleaned_data = {}
                row_errors = defaultdict(list)
                for name, field in self.sub_fields.values():
                    try:
                        field.contribute_to_cleaned_data(cleaned_data, v)
                    except SkipField:
                        continue
                    except ValidationError as e:
                        row_errors[name].append(e)
                if row_errors:
                    # TODO: Find a way to support skipping of eroneous rows
                    raise ValidationError(row_errors)
                if self.flatten:
                    try:
                        return_value.append(cleaned_data.values()[0])
                    except IndexError:
                        pass
                else:
                    return_value.append(cleaned_data)
            except SkipRow:
                continue
        return return_value


class BaseParsedField(Field):
    default_parser = None

    def __init__(self, *args, parser: BaseParser = None, **kwargs):
        self.parser = parser or self.default_parser
        super().__init__(*args, **kwargs)

    def get_parser(self):
        return self.parser(**self.get_parser_kwargs())

    def get_parser_kwargs(self):
        return {"command": self.command}

    def to_python(self, value):
        parser = self.get_parser()
        value = parser.parse(str(value))
        # TODO: Find a better way to surface / persist parser errors/warnings
        if parser.messages:
            self.log_debug(
                f"The following issues were encountered when parsing '{self.source}':"
            )
            for i, msg in enumerate(parser.messages, 1):
                self.log_debug(f"{i}. {msg}")
        return value
