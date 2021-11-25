from typing import Any, List

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.fields import Field


class LegacyModelMixin(models.Model):
    LEGACY_ID_FIELD: str = ""

    class Meta:
        abstract = True

    @property
    def legacy_id(self) -> Any:
        return getattr(self, self.LEGACY_ID_FIELD, None)

    @property
    def is_legacy(self) -> bool:
        return self.legacy_id is not None

    @classmethod
    def extra_search_fields(cls) -> List[Any]:
        from wagtail.search import index

        return [
            index.FilterField(cls.LEGACY_ID_FIELD),
            index.SearchField(cls.LEGACY_ID_FIELD),
        ]


class LegacyImportedModelMixin(LegacyModelMixin):
    last_imported_at = models.DateTimeField(null=True, editable=False)

    @classmethod
    def extra_search_fields(cls) -> List[Any]:
        from wagtail.search import index

        return super().extra_search_fields + [
            index.FilterField("last_imported_at"),
        ]


class LegacyReferenceModelMixin(models.Model):
    """
    A mixin that can be used with models that have a `ForeignKey` to a model
    that might not have been populated when data for another model is model
    is imported, but will likely exist when the migration has completed.

    When the object does not yet exist, the `ForeignKey` field named by
    'REAL_REFERENCE_FIELD' is left blank, and the 'legacy_id' value is
    stored instead. These 'legacy_id' values will be evaluated again later
    in the import process (once the target model is populated).
    """

    REAL_REFERENCE_FIELD: str = ""

    legacy_id = models.CharField(
        editable=False, null=True, max_length=255, db_index=True
    )

    class Meta:
        abstract = True

    def full_clean(self, *args, **kwargs) -> None:
        """
        Overrides Model.full_clean() to silence validation errors for REAL_REFERENCE_FIELD
        when 'legacy_id' is set, allowing the object to be saved.

        NOTE: REAL_REFERENCE_FIELD must be defined with 'null=True' for this to work.
        """
        try:
            super().full_clean(*args, **kwargs)
        except ValidationError as e:
            for i, error in enumerate(e.error_dict.get(self.REAL_REFERENCE_FIELD, [])):
                if error.code == "required" and self.legacy_id:
                    e.error_dict[self.REAL_REFERENCE_FIELD].pop(i)
                    if not e.error_dict[self.REAL_REFERENCE_FIELD]:
                        del e.error_list[self.REAL_REFERENCE_FIELD]
            if e.error_dict:
                raise
