import functools
import re
from typing import Any, Dict, Set, Tuple, Type

from django.core.exceptions import FieldDoesNotExist
from django.db.models import OneToOneRel, Q
from django.db.models.functions import Coalesce
from wagtail.core.models import Page, get_page_models
from wagtail.core.query import PageQuerySet

from importo.wagtail.models import LegacyPageMixin


def get_legacy_page_type_related_names(
    values: Dict[Type, str], model_class: Type, known_subclasses=None, prefix=None
):
    if known_subclasses is None:
        known_subclasses = set(
            model
            for model in get_page_models()
            if not model._meta.abstract
            and issubclass(model, model_class)
            and issubclass(model, LegacyPageMixin)
        )

    for rel in (
        rel
        for rel in model_class._meta.related_objects
        if isinstance(rel, OneToOneRel) and rel.related_model in known_subclasses
    ):
        rel_name = f"{prefix}__{rel.name}" if prefix else rel.name
        values[rel.related_model] = rel_name
        get_legacy_page_type_related_names(
            values, rel.related_model, known_subclasses, rel_name
        )
    return values


@functools.lru_cache(maxsize=None)
def get_concrete_subclass_related_names(model: Type) -> Dict[Type, str]:
    return get_legacy_page_type_related_names({}, model)


@functools.lru_cache(maxsize=None)
def get_concrete_local_field_names(model: Type) -> Set[str]:
    return set(
        f.name
        for f in model._meta.get_fields(include_parents=False, include_hidden=False)
    )


def get_legacy_page_field_values(
    field_name: str, queryset: PageQuerySet = None, exclude_nulls=False
) -> Tuple[Any]:

    if queryset is None:
        queryset = Page.objects.all()

    coalesce_keys = []
    for model, related_name in get_concrete_subclass_related_names(
        queryset.model
    ).items():
        try:
            model._meta.get_field(field_name)
            coalesce_keys.append(f"{related_name}__{field_name}")
        except FieldDoesNotExist:
            pass

    if not coalesce_keys:
        return ()

    queryset = queryset.annotate(**{field_name: Coalesce(*coalesce_keys)})
    if exclude_nulls:
        queryset = queryset.exclude(**{f"{field_name}__isnull": True})
    return tuple(queryset.values_list(field_name, flat=True))


def get_legacy_page_matches(
    value: Any, *field_names: str, queryset: PageQuerySet = None, lookup_type=None
):
    if lookup_type is None:
        lookup_type = "exact"
    q = Q()

    if queryset is None:
        queryset = Page.objects.all()

    for name in field_names:

        if name == "legacy_id" and getattr(queryset.model, "LEGACY_ID_FIELD", None):
            q |= Q(**{f"{queryset.model.LEGACY_ID_FIELD}__{lookup_type}": value})
        else:
            try:
                queryset.model._meta.get_field(name)
            except FieldDoesNotExist:
                pass
            else:
                q |= Q(**{f"{name}__{lookup_type}": value})

    for model, related_name in get_concrete_subclass_related_names(
        queryset.model
    ).items():
        model_field_names = get_concrete_local_field_names(model)
        for name in field_names:
            if name == "legacy_id" and getattr(model, "LEGACY_ID_FIELD", None):
                lookup_field = model.LEGACY_ID_FIELD
            else:
                lookup_field = name
            if lookup_field in model_field_names:
                q |= Q(**{f"{related_name}__{lookup_field}__{lookup_type}": value})

    if not q:
        return queryset.none()
    return queryset.filter(q)


def get_legacy_path_matches(value: str, queryset: PageQuerySet = None, exact=True):
    if exact:
        lookup_val = value
        lookup_type = "exact"
    else:
        lookup_val = r"^/?" + re.escape(value.strip("/ ")) + r"/?$"
        lookup_type = "iregex"

    return get_legacy_page_matches(
        lookup_val, "legacy_path", queryset=queryset, lookup_type=lookup_type
    )


def get_legacy_id_matches(value: Any, queryset: PageQuerySet = None):
    return get_legacy_page_matches(value, "legacy_id", queryset=queryset)
