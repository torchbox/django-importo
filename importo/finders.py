from typing import TYPE_CHECKING, Any, Sequence

from django.contrib.auth import get_user_model
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django.db.models import Model
from django.db.models.base import ModelBase
from django.db.models.query import QuerySet

from importo.lookups import BaseLookup
from importo.utils.classes import CommandBoundObject

if TYPE_CHECKING:
    from importo.commands import BaseCommand

User = get_user_model()


class BaseFinder(CommandBoundObject):
    model: ModelBase = None
    only_fields: Sequence[str] = []
    lookup_options: Sequence[BaseLookup] = []

    def __init__(self, command: "BaseCommand"):
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
        raise self.model.DoesNotExist(
            f"No {model._meta.verbose_name} could be found matching '{value}'."
        )

    def add_to_cache(self, lookup_value: Any, result: Model) -> None:
        self.result_cache[lookup_value] = result
        try:
            if legacy_id := getattr(result, "legacy_id", None):
                self.result_cache[legacy_id] = result
        except AttributeError:
            pass

    def get_from_cache(self, lookup_value: Any) -> Model:
        return self.result_cache[lookup_value]

    def clear_cache(self):
        self.result_cache.clear()


class UserFinder(BaseFinder):

    model = User

    def looks_like_other_value(self, value):
        # TODO: examine model.username_field to figure this out
        return isinstance(value, str)

    def get_other_match_queryset(self, value: Any) -> QuerySet:
        filter_kwargs = {self.model.USERNAME_FIELD: str(value)}
        return self.get_queryset().filter(**filter_kwargs)
