from typing import TYPE_CHECKING, Any, Sequence, Set, Union
from urllib.parse import ParseResult, urlparse

from django.core.validators import EMPTY_VALUES
from django.utils.functional import cached_property

from importo.utils.urlpath import normalize_path

if TYPE_CHECKING:
    from .base import BaseFinder
    from .lookup_options import BaseLookupOption



class LookupValueNotSupported(Exception):
    pass


class LookupValue:

    def __init__(self, raw: Any = None, finder: 'BaseFinder' = None):
        self.raw = raw
        self.finder = None
        self.compatible_lookup_options = ()

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.raw}>"

    @property
    def is_empty(self) -> bool:
        return self.raw in EMPTY_VALUES

    def check_finder_compatibility(self) -> None:
        """
        Checks this instance for compatibility with each of the finder's
        lookup options. Raises ``LookupValueNotSupported`` if the value
        is incompatible with all lookup options.
        """
        self.compatible_lookup_options = self.get_compatible_lookup_options()
        if not self.compatible_lookup_options:
            raise LookupValueNotSupported

    def get_compatible_lookup_options(self):
        """
        Checks this instance for compatibility with each of the finder's
        lookup options, and returns a tuple of the compatible ones.
        """
        return tuple(option for option in self.finder.bound_lookup_options if option.is_enabled() and option.value_is_compatible(self))

    @cached_property
    def urlparsed(self) -> ParseResult:
        """
        If the underlying raw value is a string, returns the result of
        ``urllib.parse.urlparse()`` for that value. Otherwise returns
        ``None``. The ``ParseResult`` is cached so that it can easily
        be used by multiple lookup options.
        """
        if isinstance(self.raw, str):
            return urlparse(self.raw)
        return None

    @cached_property
    def normalized_path(self) -> str:
        return normalize_path(self.urlparsed.path)

    @cached_property
    def cache_keys(self) -> Set[Any]:
        """
        Returns a list of 'keys' that should be used to get/set cache items
        for this value. We use more than one cache key, because multiple raw
        values can match to the same object e.g. ("06", "6", 6), and we want
        to reuse cached values as much as possible as long as they are correct,
        regardless of the specific version that is used.
        """
        keys = [self.raw]
        for lookup in self.valid_lookup_options:
            for key in lookup.get_extra_cache_keys(self):
                if key not in keys:
                    keys.append(key)
        return keys
