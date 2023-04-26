from django.db import models
from django.conf import settings
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from importo.models import BaseImportedURIEntity


class ImportedWagtailPage(BaseImportedURIEntity):
    original_language = models.CharField(
        verbose_name=_("original language"),
        default=settings.LANGUAGE_CODE,
        db_index=True,
        max_length=15,
    )
    ia_fixup_required = models.BooleanField(default=False)

    object = models.OneToOneField(
        "wagtailcore.Page", related_name="import_record"
    )

    @cached_property
    def original_parent_path(self):
        return "/".join(self.original_path.rstrip("/ ").split()[:-1])


class ImportedWagtailDocument(BaseImportedURIEntity):
    original_language = models.CharField(
        verbose_name=_("original language"),
        default=settings.LANGUAGE_CODE,
        db_index=True,
        max_length=15,
    )
    object = models.OneToOneField(
        getattr(settings, "WAGTAILDOCS_DOCUMENT_MODEL", "wagtaildocs.Document"),
        related_name="import_record",
    )


class ImportedWagtailImage(BaseImportedURIEntity):
    object = models.OneToOneField(
        getattr(settings, "WAGTAILIMAGES_IMAGE_MODEL", "wagtailimages.Image"),
        related_name="import_record",
    )
