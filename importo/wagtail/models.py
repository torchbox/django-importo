from typing import Any, List

from django.db import models
from django.http.request import HttpRequest
from django.utils.functional import cached_property
from wagtail.core.models import Page
from wagtail.search import index

from importo.models import LegacyImportedModelMixin


class LegacyPageMixin(LegacyImportedModelMixin):
    legacy_path = models.CharField(blank=True, null=True, max_length=255, db_index=True)

    class Meta:
        abstract = True

    def save(self, *args, **kwargs) -> None:
        # Empty strings are not unique, but multiple NULLs are fine
        if self.legacy_path is not None and self.legacy_path.strip() == "":
            self.legacy_path = None
        super().save(*args, **kwargs)

    @classmethod
    def extra_search_fields(cls) -> List[index.BaseField]:
        return super().extra_search_fields() + [index.FilterField("legacy_path")]

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
        return "/" + "/".join(segments)

    def get_ideal_slug(self, request: HttpRequest = None) -> str:
        ideal_path = self.get_ideal_path(request)
        segments = list(seg for seg in ideal_path.split("/") if seg)
        if segments:
            return segments.pop()
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
        return current_parent_path.rstrip("/") == self.get_ideal_parent_path(request)

    def has_ideal_slug(self, request: HttpRequest = None) -> bool:
        if not self.legacy_path:
            return True
        return self.slug == self.get_ideal_slug(request)
