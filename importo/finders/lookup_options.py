import re
import copy
from typing import TYPE_CHECKING, Any, Callable, Sequence, Union

from django.core.exceptions import FieldDoesNotExist, ImproperlyConfigured, ValidationError
from django.db.models import Field, Model, Q
from django.db.models.base import ModelBase
from django.db.models.query import QuerySet

from importo.constants import NOT_SPECIFIED
from importo.models import LegacyModelMixin
from importo.utils.classes import CopyableMixin
from importo.utils.multi_table_inheritance import (
    get_concrete_local_field_names,
    get_concrete_subclasses,
    get_concrete_subclasses_with_field,
)

if TYPE_CHECKING:
    from .base import BaseFinder
    from .lookup_value import LookupValue


class MultipleFieldTypesError(ImproperlyConfigured):
    pass


class LookupValueError(ValueError):
    pass


class ValueTypeIncompatible(LookupValueError):
    pass


class ModelFieldValidationFailed(LookupValueError):
    pass


class ValueDoesNotMatchValidPatterns(LookupValueError):
    pass


class ValueMatchesInvalidPatterns(LookupValueError):
    pass


class BaseLookupOption(CopyableMixin):

    def __init__(
        self,
        *,
        case_sensitive: bool = True,
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
    ):
        self.case_sensitive = case_sensitive
        self.valid_patterns = valid_patterns or ()
        self.invalid_patterns = invalid_patterns or ()
        self._finder = None

    def get_finder_bound_copy(self, finder: 'BaseFinder') -> 'BaseLookupOption':
        new = copy.copy(self)
        new.finder = finder
        return new

    @property
    def finder(self) -> Union[None, "BaseFinder"]:
        return self._finder

    @finder.setter
    def finder(self, finder: "BaseFinder") -> None:
        self._finder = finder
        self.on_finder_bound(finder)

    def is_enabled(self):
        return True

    def value_is_compatible(self, value: LookupValue) -> bool:
        """
        Returns a boolean indicating whether this instance could possibly find
        a model instance for the supplied ``value``.
        """
        if value.is_empty:
            return False
        try:
            self.validate_lookup_value(value)
        except LookupValueError:
            return False
        return True

    def validate_lookup_value(self, value: LookupValue) -> None:
        """
        Raises ``ValueError`` if ``value`` looks like a bad match for this lookup.
        """
        self.validate_with_valid_paterns(value)
        self.validate_with_invalid_paterns(value)

    def value_matches_pattern(self, value: LookupValue, pattern: re.Pattern) -> bool:
        if self.case_sensitive:
            return bool(pattern.match(value.raw))
        return bool(pattern.match(value.raw, re.IGNORECASE))

    def value_matches_any_patterns(self, value: LookupValue, *patterns: re.Pattern) -> bool:
        for pattern in patterns:
            if self.value_matches_pattern(value, pattern):
                return True
        return False

    def validate_with_valid_paterns(self, value: LookupValue) -> None:
        """
        Raises ``ValueDoesNotMatchValidPatterns`` if ``value`` does NOT match one of
        the ``valid_patterns`` specified for this lookup.
        """
        if self.valid_patterns and not self.value_matches_any_patterns(value, *self.valid_patterns):
            raise ValueDoesNotMatchValidPatterns

    def validate_with_invalid_paterns(self, value: LookupValue) -> None:
        """
        Raises ``ValueMatchesInvalidPatterns`` if ``value`` matches one of the
        ``invalid_patterns`` specified for this lookup.
        """
        if self.invalid_patterns and self.value_matches_any_patterns(value, *self.invalid_patterns):
            raise ValueMatchesInvalidPatterns

    def find(
        self,
        value: LookupValue,
        queryset: QuerySet,
    ) -> Model:
        """
        Return a single object from ``queryset`` matching the supplied ``value``.
        """
        raise NotImplementedError

    def get_extra_cache_keys(self, lookup_value: LookupValue) -> Sequence[Any]:
        """
        Returns a sequence of cache keys to be used (in addition to ``raw_value``)
        to look up results from (or adding a result to) a finder's result cache.
        """
        return []

    def get_extra_cache_keys_from_result(self, result: Model) -> Sequence[Any]:
        """
        Returns a sequence of cache keys to be used (in addition to those from
        ``get_extra_cache_keys()``) when adding a result to a finder's
        result cache.
        """
        return []


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


class MTIFieldLookupOption(ModelFieldLookupOption):
    """
    A specialised version of ModelFieldLookupOption for models that use multi-table
    inheritance (like Wagtail's Page model does). Will accept ``field_name`` values
    for fields that are not on the specified `model` class, but ARE used on one or
    more subclasses, and magically filter accross database tables to find matches.
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

    def get_q(self, lookup_value: "LookupValue") -> Q:
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


class LegacyIDLookupOption(MTIFieldLookupOption):
    multiple_matching_fields = False
    no_matching_fields = False

    def on_finder_bound(self, finder: "BaseFinder"):
        """
        Instead of throwing exceptions due to the field not being found,
        set some internal flags that will result in this option being
        silently ignored.
        """
        try:
            super().on_finder_bound(finder)
        except MultipleFieldTypesError:
            self.multiple_matching_fields = True
        except FieldDoesNotExist:
            self.no_matching_fields = True

    def is_enabled(self):
        """
        Use the flags set in 'on_finder_bound()' to indicate that
        this option is 'disabled'.
        """
        return (
            super().is_enabled()
            and not self.multiple_matching_fields
            and not self.no_matching_fields
        )

    def get_relevant_subclasses(self):
        return {model: related_name for model, related_name in get_concrete_subclasses(self.model).items() if issubclass(model, LegacyModelMixin)}

    def get_model_field(self) -> Field:
        if issubclass(self.model, LegacyModelMixin):
            try:
                return self.model._meta.get_field(self.model.LEGACY_ID_FIELD)
            except FieldDoesNotExist:
                pass

        field_types = set()
        fields = set()
        for subclass in self.get_relevant_subclasses().keys():
            # NOTE: get_relevant_subclasses() only returns concrete models
            # that subclass LegacyModelMixin. Therefore, all should have
            # LEGACY_ID_FIELD set (hence no try/except here)
            legacy_id_field = subclass._meta.get_field(subclass.LEGACY_ID_FIELD)
            field_types.add(type(legacy_id_field))
            fields.add(legacy_id_field)

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
