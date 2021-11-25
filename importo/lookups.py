import re
from typing import Any, Dict, Sequence

from django.core.exceptions import FieldDoesNotExist, ValidationError
from django.db.models import Model
from django.db.models.base import ModelBase
from django.db.models.query import QuerySet

from importo.utils.url import looks_like_internal_url


class BaseLookup:
    __slots__ = ["valid_patterns", "invalid_patterns"]

    def __init__(
        self,
        *,
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
    ):
        self.valid_patterns = valid_patterns or ()
        self.invalid_patterns = invalid_patterns or ()

    def validate_lookup_value(self, value: Any, *, model: ModelBase) -> bool:
        """
        Raise ValueError if ``value`` looks like a bad match for this lookup.
        """
        if not value:
            raise ValueError

        if self.valid_patterns:
            matched = False
            for pattern in self.valid_patterns:
                if pattern.match(str(value)):
                    matched = True
                    break
            if not matched:
                raise ValueError

        if self.invalid_patterns:
            matched = False
            for pattern in self.invalid_patterns:
                if pattern.match(str(value)):
                    matched = True
                    break
            if matched:
                raise ValueError

    def find(self, value: Any, *, queryset: QuerySet) -> Model:
        """
        Return an object from ``queryset`` matching the supplied
        ``lookup_value``, or raise ``ObjectDoesNotExist``.
        """
        raise NotImplementedError


class FieldLookup(BaseLookup):
    __slots__ = ["lookup_field", "lookup_type", "valid_patterns", "invalid_patterns"]

    def __init__(
        self,
        *,
        lookup_field: str,
        lookup_type: str = "exact",
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
    ):
        self.lookup_field = lookup_field
        self.lookup_type = lookup_type
        super().__init__(
            valid_patterns=valid_patterns, invalid_patterns=invalid_patterns
        )

    def validate_lookup_value(self, value: Any, *, model: ModelBase) -> bool:
        """
        Raise ValueError if ``value`` looks like a bad match for this lookup.
        """
        if not value:
            raise ValueError

        if "__" not in self.lookup_field:
            try:
                field = model._meta.get_field(self.lookup_field)
                if field.editable:
                    try:
                        field.clean(value, None)
                    except ValidationError:
                        raise ValueError
                else:
                    try:
                        v = field.to_python(value)
                    except (ValueError, TypeError):
                        raise ValueError
                    try:
                        field.run_validators(v)
                    except ValidationError:
                        raise ValueError
            except FieldDoesNotExist:
                pass

        super().validate_lookup_value(value, model=model)

    def find(self, value: Any, *, queryset: QuerySet) -> Model:
        return self.filter_queryset(value, queryset).get()

    def filter_queryset(self, value: Any, queryset: QuerySet) -> QuerySet:
        lookup_kwargs = self.get_lookup_kwargs(value)
        return queryset.filter(**lookup_kwargs)

    def get_lookup_kwargs(self, value: Any) -> Dict[str, Any]:
        return {f"{self.lookup_field}__{self.lookup_type}": value}
