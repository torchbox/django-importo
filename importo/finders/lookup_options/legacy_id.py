from typing import TYPE_CHECKING, Mapping

from django.core.exceptions import FieldDoesNotExist
from django.db.models import Field
from django.db.models.base import ModelBase

from importo.models import LegacyModelMixin
from importo.utils.multi_table_inheritance import get_concrete_subclasses

from .modelfield import MTIModelFieldLookupOption, MultipleFieldTypesError

if TYPE_CHECKING:

    from importo.finders.base import BaseFinder

__all__ = [
    "LegacyIDLookupOption",
]


class LegacyIDLookupOption(MTIModelFieldLookupOption):
    multiple_matching_fields = False
    no_matching_fields = False

    def on_finder_bound(self, finder: "BaseFinder") -> None:
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

    def is_enabled(self) -> bool:
        """
        Use the flags set in 'on_finder_bound()' to indicate that
        this option is 'disabled'.
        """
        return (
            super().is_enabled()
            and not self.multiple_matching_fields
            and not self.no_matching_fields
        )

    def get_relevant_subclasses(self) -> Mapping["ModelBase", str]:
        return {
            model: related_name
            for model, related_name in get_concrete_subclasses(self.model).items()
            if issubclass(model, LegacyModelMixin)
        }

    def get_model_field(self) -> "Field":
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
