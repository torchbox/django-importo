import functools
from typing import Dict, Set, Type

from django.apps import apps
from django.db.models import OneToOneRel


def get_concrete_subclass_related_names(
    values: Dict[Type, str], model_class: Type, known_subclasses=None, prefix=None
):
    if known_subclasses is None:
        known_subclasses = set(
            model
            for model in apps.get_models()
            if issubclass(model, model_class) and not model._meta.abstract
        )

    for rel in (
        rel
        for rel in model_class._meta.related_objects
        if isinstance(rel, OneToOneRel) and rel.related_model in known_subclasses
    ):
        rel_name = f"{prefix}__{rel.name}" if prefix else rel.name
        values[rel.related_model] = rel_name
        get_concrete_subclass_related_names(
            values, rel.related_model, known_subclasses, rel_name
        )
    return values


@functools.lru_cache(maxsize=None)
def get_concrete_subclasses(model: Type) -> Dict[Type, str]:
    return get_concrete_subclass_related_names({}, model)


@functools.lru_cache(maxsize=None)
def get_concrete_subclasses_with_field(model: Type, field_name: str) -> Dict[Type, str]:
    return {
        subclass: related_name
        for subclass, related_name in get_concrete_subclass_related_names(model).items()
        if field_name in get_concrete_local_field_names(subclass)
    }


@functools.lru_cache(maxsize=None)
def get_concrete_local_field_names(model: Type) -> Set[str]:
    return set(
        f.name
        for f in model._meta.get_fields(include_parents=False, include_hidden=False)
    )
