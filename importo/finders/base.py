from typing import TYPE_CHECKING, Any, Sequence, Union

from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django.db.models import Model
from django.db.models.base import ModelBase
from django.db.models.query import QuerySet

from importo.utils.classes import CommandBoundObject

from .lookup_options import BaseLookupOption
from .lookup_value import LookupValue

if TYPE_CHECKING:
    from importo.commands import BaseCommand


class CachedValueNotFound(Exception):
    pass


class BaseFinder(CommandBoundObject):
    model: ModelBase = None
    only_fields: Sequence[str] = []
    select_related: Sequence[str] = None
    lookup_options: Sequence[BaseLookupOption] = []

    # By default, assume that no new model instances will be created
    # during the current import
    cache_lookup_failures: bool = True

    @classmethod
    def get_lookup_options(cls):
        return cls.lookup_options

    def __init__(self, command: "BaseCommand"):
        super().__init__(command)
        self.result_cache = {}
        # Generate a list of lookup options that are bound to this instance.
        # We doing this here means that errors can be raised on finder
        # initialization, which is much more obvious than generating lazily
        self.bound_lookup_options = []
        for option in self.get_lookup_options():
            self.bound_lookup_options.append(option.get_finder_bound_copy(self))

    def get_model(self):
        return self.model

    def get_lookup_value(self, raw_value: Any) -> LookupValue:
        """
        Returns a ``LookupValue`` instance to help with cache and database lookups for the
        supplied ``raw_value``.

        Raises ``LookupValueNotSupported`` if none of the lookup options configured for this
        class could possibly return a match for ``raw_value``.

        Used internally by the ``find()`` method, but can also be used by fields, parsers
        and other entities to test whether a given finder might return a match for a value,
        without investing in expensive lookups. For example:

        .. code-block:: python

            from importo.finders import ValueNotSupportedByFinder

            for finder in finder_list:
                try:
                    # Check value validity before investing in an expensive lookup
                    lookup_val = finder.get_lookup_value(value)
                except ValueNotSupportedByFinder:
                    # This finder could not possibly find a match, so skip it
                    continue
                else:
                    try:
                        # Try to find a match, reusing the LookupValue already
                        # returned by get_lookup_value()
                        return finder.find(lookup_val)
                    except ObjectDoesNotExist:
                        # No cigar... Maybe the next finder will have more luck?
                        continue
        """
        value = LookupValue(raw_value, self)
        value.check_finder_compatibility()
        return value

    def find(self, value: Any) -> Model:
        """
        Return a single object matching the supplied value. Results are cached to improve efficiency of repeat lookups.

        Raises ``LookupValueNotSupported`` if the value is unsuitable for all of the available lookup options.

        Raises ``ObjectDoesNotExist`` if no such model instance can be found.

        If you find yourself wanting to override this method, consider overriding one of the following methods instead:

        - ``get_from_cache(self, lookup_value)``
        - ``get_single_match(self, lookup_value)``
        - ``add_to_cache(self, lookup_value)``
        """
        if isinstance(value, LookupValue):
            lookup_value = value
        else:
            lookup_value = self.get_lookup_value(value)

        # A consistently worded error that can be raised below
        not_found = self.model.DoesNotExist(
            f"No {self.model} was found matching '{lookup_value.raw}'."
        )

        # First, try for a cached result
        try:
            result = self.get_from_cache(lookup_value)
            if result is not None:
                return result
            else:
                raise not_found
        except CachedValueNotFound:
            pass

        # Try to get a match from the database
        try:
            result = self.get_single_match(lookup_value)
            self.add_to_cache(result, lookup_value)
        except ObjectDoesNotExist:
            if self.cache_lookup_failures:
                self.add_to_cache(None, lookup_value)
            raise not_found
        return result

    def get_single_match(self, lookup_value: LookupValue) -> Model:
        """
        Return a single object from the database matching the supplied ``lookup_value``,
        using any lookup options that are valid.

        Raises ``ObjectDoesNotExist`` if no match can be found.
        """
        base_queryset = self.get_queryset()
        for option in lookup_value.valid_lookup_options:
            try:
                option.get_match(
                    lookup_value,
                    base_queryset,
                    on_multiple_matches=self.on_multiple_matches,
                )
            except ObjectDoesNotExist:
                continue
        raise ObjectDoesNotExist

    def get_queryset(self) -> QuerySet:
        qs = self.get_model().objects.all()
        if self.only_fields:
            qs = qs.only(*self.only_fields)
        if self.select_related:
            qs = qs.select_related(*self.select_related)
        return qs

    def get_from_cache(self, lookup_value: LookupValue) -> Union[Model, None]:
        """
        Try to return a value from this finder's 'lookup cache', or raise ``CachedValueNotFound``
        if the cache contains no such item.
        """
        for key in lookup_value.cache_keys:
            try:
                return self.result_cache[key]
            except KeyError:
                continue
        raise CachedValueNotFound(f"No cached results were found for '{lookup_value}'.")

    def add_to_cache(
        self, lookup_value: LookupValue, result: Union[Model, None]
    ) -> None:
        keys = lookup_value.cache_keys
        if result is not None:
            for option in self.lookup_options:
                keys.update(option.get_cache_keys_for_result(result))
        for key in keys:
            self.result_cache[key] = result

    def clear_cache(self) -> None:
        self.result_cache.clear()
