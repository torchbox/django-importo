from django.db import models
from django.http.request import HttpRequest
from django.utils.functional import cached_property
from django.utils.text import slugify
from django.utils.translation import gettext_lazy as _
from wagtail.models import Page
from wagtail.documents.models import AbstractDocument
from wagtail.images.models import AbstractImage

from importo.models import LegacyImportedModelMixin, LegacyImportedModelWithFileMixin
from importo.utils.urlpath import normalize_path


class LegacyPageMixin(LegacyImportedModelMixin):
    legacy_domain = models.CharField(
        verbose_name=_("legacy domain"),
        max_length=255,
        blank=True,
        null=True,
        db_index=True,
    )
    legacy_path = models.CharField(
        verbose_name=_("legacy path"),
        blank=True,
        null=True,
        max_length=255,
        db_index=True,
    )

    class Meta:
        abstract = True

    @property
    def legacy_url(self):
        if self.legacy_domain and self.legacy_path:
            return "http://" + self.legacy_domain.rstrip("/") + self.legacy_path

    @cached_property
    def specific_parent_page(self) -> Page:
        return self.get_parent().specific_deferred

    def get_ideal_path(self, request: HttpRequest = None) -> str:
        value = self.legacy_path or self.get_url(request)
        return value.rstrip("/")

    def get_ideal_parent_path(self, request: HttpRequest = None) -> str:
        ideal_path = self.get_ideal_path(request)
        segments = list(seg for seg in ideal_path.split("/") if seg)
        if segments:
            segments.pop()
        return normalize_path("/".join(segments))

    def get_ideal_slug(self, request: HttpRequest = None) -> str:
        ideal_path = self.get_ideal_path(request)
        segments = list(seg for seg in ideal_path.split("/") if seg)
        if segments:
            return slugify(segments.pop())
        return "home"

    def has_ideal_path(self, request: HttpRequest = None) -> bool:
        if not self.legacy_path:
            return True
        site_id, root_url, current_path = self.get_url_parts(request)
        return current_path.rstrip("/") == self.get_ideal_path(request)

    def has_ideal_parent(self, request: HttpRequest = None) -> bool:
        if not self.legacy_path:
            return True
        (
            site_id,
            root_url,
            current_parent_path,
        ) = self.specific_parent_page.get_url_parts(request)
        return normalize_path(current_parent_path) == self.get_ideal_parent_path(
            request
        )

    def has_ideal_slug(self, request: HttpRequest = None) -> bool:
        if not self.legacy_path:
            return True
        return self.slug == self.get_ideal_slug(request)


class LegacyAbstractDocument(LegacyImportedModelWithFileMixin, AbstractDocument):
    class Meta(AbstractDocument.Meta):
        abstract = True


class LegacyAbstractImage(LegacyImportedModelWithFileMixin, AbstractImage):
    class Meta(AbstractImage.Meta):
        abstract = True
