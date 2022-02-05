import re
from typing import TYPE_CHECKING, Any, Callable, Sequence, Union

from django.core.exceptions import FieldDoesNotExist, ImproperlyConfigured, ValidationError
from django.db.models import Field, Model, Q
from django.db.models.query import QuerySet

from importo.constants import NOT_SPECIFIED
from importo.finders.lookup_value import LookupValue
from importo.utils.multi_table_inheritance import (
    get_concrete_local_field_names,
    get_concrete_subclasses_with_field,
)

from .base import BaseLookupOption, LookupValueError


if TYPE_CHECKING:
    from importo.finders.base import BaseFinder


__all__ = [
    "ModelFieldValidationFailed",
    "ModelFieldLookupOption",
    "MultipleFieldTypesError",
    "MTIModelFieldLookupOption",
]


class ModelFieldValidationFailed(LookupValueError):
    pass


class ModelFieldLookupOption(BaseLookupOption):

    # NOTE: constants are defined here to make them
    # available without additional imports
    RAISE_ERROR = "RAISE_ERROR"
    USE_FIRST_ITEM = "RETURN_FIRST_ITEM"
    USE_LAST_ITEM = "RETURN_LAST_ITEM"

    on_multiple_objects_found_choices = [
        NOT_SPECIFIED,
        RAISE_ERROR,
        USE_FIRST_ITEM,
        USE_LAST_ITEM,
    ]

    on_multiple_objects_found_default = RAISE_ERROR

    def __init__(
        self,
        field_name: str,
        *,
        case_sensitive: bool = True,
        on_multiple_objects_found: Union[str,Callable] = NOT_SPECIFIED,
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
    ):
        self.field_name = field_name
        self.model_field = None
        self.on_multiple_objects_found = on_multiple_objects_found
        super().__init__(
            case_sensitive=case_sensitive,
            valid_patterns=valid_patterns,
            invalid_patterns=invalid_patterns,
        )

    def on_finder_bound(self, finder: "BaseFinder"):
        self.model = finder.get_model()
        self.model_field = self.get_model_field()

    def is_enabled(self):
        return self.model_field is not None

    @property
    def on_multiple_objects_found(self):
        if self._on_multiple_objects_found != NOT_SPECIFIED:
            return self._on_multiple_objects_found
        return self.on_multiple_objects_found_default

    @on_multiple_objects_found.setter
    def on_multiple_objects_found(self, value):
        valid_choices = self.on_multiple_objects_found_choices
        if not callable(value) and value not in valid_choices:
            raise ValueError(
                "'on_multiple_objects_found' must be a callable or one of "
                f"the following values (not '{value}'): {valid_choices}."
            )
        self._on_multiple_object_returned = value

    def get_model_field(self) -> Field:
        try:
            return self.model._meta.get_field(self.field_name)
        except FieldDoesNotExist:
            if self.field_name.endswith("_id"):
                return self.model._meta.get_field(self.field_name[:-3]).target_field
            raise

    # -------------------------------------------------------------------------
    # Validation methods
    # -------------------------------------------------------------------------

    def validate_with_model_field(self, value: LookupValue) -> None:
        field = self.model_field
        if field.editable:
            try:
                field.clean(value.raw, None)
            except ValidationError:
                raise ModelFieldValidationFailed
        else:
            try:
                v = field.to_python(value.raw)
            except (ValueError, TypeError):
                raise ModelFieldValidationFailed
            try:
                field.run_validators(v)
            except ValidationError:
                raise ModelFieldValidationFailed

    def validate_lookup_value(self, value: LookupValue) -> None:
        """
        Raises ``ValueError`` if it looks like this object couldn't possibly find a matching
        object for the supplied ``value``.
        """
        self.validate_with_model_field(value)
        super().validate_lookup_value(value)

    # -------------------------------------------------------------------------
    # Cache key generation
    # -------------------------------------------------------------------------

    def get_extra_cache_keys(self, lookup_value: LookupValue) -> Sequence[Any]:
        keys = []
        try:
            cleaned = self.model_field.to_python(lookup_value.raw)
        except (ValueError, TypeError):
            pass
        else:
            keys.append(cleaned)
            if isinstance(cleaned, str) and not self.case_sensitive:
                keys.append(cleaned.lower())
        return keys

    def get_extra_cache_keys_from_result(self, result: Model) -> Sequence[Any]:
        field_val = getattr(result, self.field_name)
        keys = [field_val]
        if isinstance(field_val, str) and not self.case_sensitive:
            keys.append(field_val.lower())
        return keys

    def find(
        self,
        value: LookupValue,
        base_queryset: QuerySet,
    ) -> Model:

        queryset = self.filter_queryset(base_queryset, value)

        # NOTE: Even if we don't know there will be multiple matches, we can
        # use the preferred strategy proactively, to improve efficiency
        strategy = self.on_multiple_objects_found

        if callable(strategy):
            # Result counts should be low, so evaluating the
            # queryset here to avoid a separate count() query
            results = list(queryset)
            if len(results) == 1:
                return results[0]
            # Let the callable decide which result to use
            return queryset(queryset)

        elif strategy == self.RAISE_ERROR:
            return queryset.get()
        elif strategy == self.USE_FIRST_ITEM:
            result = queryset.first()
        else:
            assert strategy == self.USE_LAST_ITEM
            result = queryset.last()
        if result is None:
            raise self.model.DoesNotExist
        return result

    # -------------------------------------------------------------------------
    # ORM lookup methods
    # -------------------------------------------------------------------------

    def filter_queryset(self, queryset: QuerySet, lookup_value: LookupValue) -> QuerySet:
        return queryset.filter(self.get_q(lookup_value))

    def get_q(self, lookup_value: LookupValue) -> Q:
        """
        Return a ``django.db.models.Q`` object that can be used to filter a ``QuerySet``
        of type ``self.model`` to find a match for the supplied ``lookup_value``.
        """
        field_name = self.get_q_field_name(lookup_value)
        match_type = self.get_q_match_type(lookup_value)
        value = self.get_q_value(lookup_value)
        kwargs = {f"{field_name}__{match_type}":  value}
        return Q(**kwargs)

    def get_q_field_name(self, lookup_value: LookupValue) -> str:
        return self.field_name

    def get_q_match_type(self, lookup_value: LookupValue) -> str:
        if self.case_sensitive:
            return "exact"
        return "iexact"

    def get_q_value(self, lookup_value: LookupValue) -> Any:
        return lookup_value.raw


class MultipleFieldTypesError(ImproperlyConfigured):
    pass


class MTIModelFieldLookupOption(ModelFieldLookupOption):
    """
    A specialised version of ModelFieldLookupOption for models that might
    be using multi-table inheritance to support polymorphism (like Wagtail's
    Page model does). Will accept ``field_name`` values for fields that are
    not on the specified ``model`` class, but ARE used on one or more
    subclasses, and magically traverse relationships to find matches.

    Raises `MultipleFieldTypesError`` if the named field has been added to
    subclasses using different field types.
    """

    def get_relevant_subclasses(self):
        return get_concrete_subclasses_with_field(
            self.model, self.field_name
        )

    def get_model_field(self) -> Field:
        try:
            return super().get_model_field()
        except FieldDoesNotExist:
            field_types = set()
            fields = set()
            for subclass in self.get_relevant_subclasses().keys():
                field_types.add(type(field_types))
                fields.add(subclass._meta.get_field(self.field_name))
            if len(field_types) > 1:
                raise MultipleFieldTypesError(
                    f"Subclasses of {self.model} have '{self.field_name}' fields with differing types, "
                    "which is not supported. Consider renaming some of these fields."
                )
            for field in fields:
                return field
            raise FieldDoesNotExist(
                f"{self.model} has no concrete subclasses with a field named '{self.field_name}'."
            )

    def get_q(self, lookup_value: LookupValue) -> Q:
        field_name = self.get_q_field_name(lookup_value)
        match_type = self.get_q_match_type(lookup_value)
        value = self.get_q_value(lookup_value)
        if field_name in get_concrete_local_field_names(self.model):
            kwargs = {f"{field_name}__{match_type}": value}
            return Q(**kwargs)
        subclass_field_q = Q()
        for related_name in get_concrete_subclasses_with_field(
            self.model, field_name
        ).values():
            kwargs = {f"{related_name}__{field_name}__{match_type}": value}
            subclass_field_q |= Q(**kwargs)
        return subclass_field_q
