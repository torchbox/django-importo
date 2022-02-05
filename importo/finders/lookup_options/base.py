import copy
import re
from typing import TYPE_CHECKING, Any, Sequence, Union

from django.db.models import Model
from django.db.models.query import QuerySet

from importo.finders.lookup_value import LookupValue
from importo.utils.classes import CopyableMixin

if TYPE_CHECKING:
    from importo.finders.base import BaseFinder

__all__ = [
    "LookupValueError",
    "ValueTypeIncompatible",
    "ValueDoesNotMatchValidPatterns",
    "ValueMatchesInvalidPatterns",
    "BaseLookupOption",
]


class LookupValueError(ValueError):
    pass


class ValueTypeIncompatible(LookupValueError):
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
