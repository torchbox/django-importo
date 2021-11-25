import os
import re
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Union
from urllib.parse import ParseResult, urlparse

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import FieldDoesNotExist, ObjectDoesNotExist, ValidationError
from django.core.management.base import BaseCommand
from django.db.models import Model, Q, EmailField, IntegerField, UUIDField
from django.db.models.base import ModelBase
from django.db.models.query import QuerySet
from django.utils.functional import cached_property

from .utils import get_dummy_request
from .utils.classes import CommandBoundObject
from .utils.io import filename_from_url
from .utils.query import get_legacy_page_matches, get_legacy_path_matches

from importo.lookups import BaseLookup

if TYPE_CHECKING:
    from importo.commands import BaseCommand


class BaseFinder(CommandBoundObject):
    model: ModelBase = None
    only_fields: Sequence[str] = []
    lookup_options: Sequence[BaseLookup] = []

    def __init__(self, command: 'BaseCommand'):
        super().__init__(command)
        self.result_cache = {}

    def get_queryset(self) -> QuerySet:
        qs = self.get_model().objects.all()
        if self.only_fields:
            qs = qs.only(*self.only_fields)
        return qs

    def find(self, value: Any) -> Model:
        try:
            self.get_from_cache(value)
        except KeyError:
            pass
        result = self.lookup_instance(value)
        self.add_to_cache(result, value)

    def lookup_instance(self, value: Any) -> Model:
        model = self.get_model()
        for option in self.lookup_options:
            try:
                option.is_valid_lookup_value(value, model)
            except ValueError:
                continue
            try:
                return option.find(value, self.get_queryset())
            except ObjectDoesNotExist:
                pass
        raise self.model.DoesNotExist(f"No {model._meta.verbose_name} could be found matching '{value}'.")

    def add_to_cache(self, lookup_value: Any, result: Model) -> None:
        self.result_cache[lookup_value] = result
        try:
            if legacy_id := getattr(result, 'legacy_id', None):
                self.result_cache[legacy_id] = result
        except AttributeError:
            pass

    def get_from_cache(self, lookup_value: Any) -> Model:
        return self.result_cache[lookup_value]

    def clear_cache(self):
        self.result_cache.clear()






class BaseLegacyModelFinder(BaseFinder):

    model: ModelBase = None
    only_fields: Sequence[str] = []

    def __init__(self, command: BaseCommand = None):
        # Simple, local dict caches to store results for each importer run
        self._legacy_id_lookup_cache = {}
        super().__init__(command)

    def add_to_cache(self, value, other_cache_key: Any = None):
        if legacy_id_field := getattr(value, "LEGACY_ID_FIELD", None):
            if key := getattr(value, legacy_id_field, None):
                self._legacy_id_lookup_cache[key] = value
        super().add_to_cache(value, other_cache_key)

    def clear_caches(self):
        super().clear_caches()
        self._legacy_id_lookup_cache = {}

    def find(self, value: Any) -> Model:
        if self.looks_like_legacy_id(value):
            try:
                return self.find_by_legacy_id(value)
            except self.model.DoesNotExist:
                pass
        if self.looks_like_other_value(value):
            return self.find_by_other(value)
        raise self.model.DoesNotExist

    def looks_like_legacy_id(self, value):
        # TODO: examine model field to figure this out
        return isinstance(value, int) or (isinstance(value, str) and value.isdigit())

    def find_by_legacy_id(self, value: Any) -> Model:
        try:
            return self._legacy_id_lookup_cache[value]
        except KeyError:
            pass

        result = self.get_legacy_id_match(value)

        self.add_to_cache(result)
        return result

    def get_legacy_id_match_queryset(self, value: Any) -> QuerySet:
        legacy_id_field = getattr(self.get_model(), "LEGACY_ID_FIELD", None)
        return self.get_queryset().filter(**{legacy_id_field: value})

    def get_legacy_id_match(self, value: Any, prefer_match_on: str = None) -> Model:
        matches = tuple(self.get_legacy_id_match_queryset(value))
        if not matches:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching legacy_id '{value}' does not exist."
            )
        if len(matches) == 1 or not prefer_match_on:
            return matches[0]
        for match in matches:
            if str(getattr(match, prefer_match_on, "")) == str(value):
                return match
        return matches[0]

    def get(self, pk: Any) -> Model:
        """
        Get an instance by it's primary key value.
        """
        try:
            return self._pk_lookup_cache[pk]
        except KeyError:
            pass

        result = self.get_queryset().get(pk=pk)

        # Cache result before returning
        self.add_to_cache(result)
        return result

    @cached_property
    def other_finders(self) -> Dict[str, BaseFinder]:
        if self.command is None:
            return {}
        return self.command.finders


class UserFinder(BaseLegacyModelFinder):

    model = User

    def looks_like_other_value(self, value):
        # TODO: examine model.username_field to figure this out
        return isinstance(value, str)

    def get_other_match_queryset(self, value: Any) -> QuerySet:
        filter_kwargs = {self.model.USERNAME_FIELD: str(value)}
        return self.get_queryset().filter(**filter_kwargs)
