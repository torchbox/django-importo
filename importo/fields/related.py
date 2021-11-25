import html
import json
import os
import sys
import uuid
from datetime import date, datetime
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from urllib.parse import unquote_plus, urlparse

import bleach
import PIL
from django.contrib.auth import get_user_model
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile, UploadedFile
from django.core.management.base import BaseCommand
from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import Model
from django.utils import dateparse
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from importo.constants import NOT_SPECIFIED
from importo.errors import SkipField, SkipRow

from . import constants, error_codes, strategy_codes
from .base import Field

User = get_user_model()


# -------------------------------------------------------------------
# Model instance lookup fields
# -------------------------------------------------------------------


class BaseRelationshipField(Field):
    on_not_found_choices = [
        strategy_codes.NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.SKIP_FIELD,
        strategy_codes.USE_FALLBACK,
    ]
    on_not_found_default = strategy_codes.RAISE_ERROR
    default_error_messages = {
        error_codes.OBJECT_NOT_FOUND: _(
            "No object could be found matching the value '%(value)s'."
        ),
    }

    # Cleaning requires database lookups, so should be done after
    # more 'cheaply cleaned' fields
    clean_cost = constants.CLEAN_COST_MEDIUM

    def __init__(
        self,
        *,
        source: str = None,
        target_field: str = None,
        fallback: Optional[Any] = NOT_SPECIFIED,
        on_missing_value: Optional[str] = NOT_SPECIFIED,
        on_empty_value: Optional[str] = NOT_SPECIFIED,
        on_not_found: Optional[Union[str, None]] = NOT_SPECIFIED,
        required: bool = True,
        error_messages: Optional[Mapping[str, str]] = None,
        validators: Optional[Sequence[callable]] = (),
        command: Optional[BaseCommand] = None,
    ):
        self.on_not_found = on_not_found
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

    @property
    def on_not_found(self):
        if self._on_not_found != NOT_SPECIFIED:
            return self._on_not_found
        try:
            return self.command._on_not_found
        except AttributeError:
            return self.on_not_found_default

    @on_not_found.setter
    def on_not_found(self, value):
        valid_choices = self.on_not_found_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_not_found' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_not_found = value

    def get_return_type(self, value: Any) -> Type:
        raise NotImplementedError

    def find_instance(self, value: Any) -> Model:
        raise NotImplementedError

    def handle_not_found(self, value):
        strategy = self.on_not_found
        if strategy == strategy_codes.SKIP_FIELD:
            raise SkipField
        elif strategy == strategy_codes.SKIP_ROW:
            raise SkipRow
        elif strategy == strategy_codes.USE_FALLBACK:
            return self.to_python(self.get_fallback())
        elif callable(strategy):
            return self.to_python(strategy(value))
        # Assume strategy == RAISE_ERROR
        code = error_codes.OBJECT_NOT_FOUND
        msg = self.error_messages[code] % {
            "value": value,
        }
        raise ValidationError(msg, code=code)

    def to_python(self, value: Any) -> Model:
        return_type = self.get_return_type()
        if self.is_empty(value) or isinstance(value, return_type):
            return value
        try:
            return self.find_instance(value)
        except return_type.DoesNotExist:
            return self.handle_not_found(value)


class BaseFinderField(BaseRelationshipField):
    finder_name = ""

    @property
    def finder(self):
        return self.command.finders[self.finder_name]

    def find_instance(self, value: Any) -> Model:
        return self.finder.find(value)

    def get_return_type(self):
        return getattr(self, "return_type", None) or self.finder.model


class BaseMappedReferenceField(BaseRelationshipField):
    model = None
    legacy_id_format = int

    def get_return_type(self):
        return self.model

    def to_python(self, value: Any) -> Model:
        if isinstance(value, self.model):
            return value
        return super().to_python(self.legacy_id_format(value))

    def get_queryset(self):
        queryset = self.model.objects.all()
        try:
            from wagtail.core.models import Page

            if issubclass(self.model, Page):
                return queryset.defer_streamfields()
        except ImportError:
            return queryset

    @cached_property
    def legacy_id_to_object_map(self):
        return self.get_queryset().in_bulk(field_name=self.model.LEGACY_ID_FIELD)

    def find_instance(self, value: Any):
        try:
            return self.legacy_id_to_object_map[value]
        except KeyError:
            raise self.model.DoesNotExist


class UserReferenceField(BaseFinderField):
    """
    A field that converts legacy user usernames or ID values to Django User
    objects, with the help of the bound command instance's `UserFinder`.
    """

    finder_name = "users"
