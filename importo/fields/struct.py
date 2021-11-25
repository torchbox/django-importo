from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Sequence

from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

from importo.constants import NOT_SPECIFIED
from importo.errors import SkipField, SkipRow

from . import base, constants

if TYPE_CHECKING:
    from importo.commands import BaseImportCommand


class ListField(base.Field):

    clean_cost = constants.CLEAN_COST_MEDIUM
    default_fallback = []

    def __init__(
        self,
        sub_fields: Dict[str, base.Field],
        *,
        flatten: bool = False,
        source: str,
        target_field: str,
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
