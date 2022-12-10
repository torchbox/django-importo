import os
import re
from typing import Any, Iterable, Optional, Sequence, Union
from urllib.parse import ParseResult, urlparse

from django.core.management.base import BaseCommand
from django.db.models import Model, Q
from django.http import Http404
from django.utils.functional import cached_property
from wagtail.documents import get_document_model
from wagtail.images import get_image_model

from importo.finders import BaseFinder
from importo.finders.lookup_options.filename import FilePathLookupOption
from importo.finders.lookup_options.legacy_id import LegacyIDLookupOption
from importo.models import LegacyImportedModelWithFileMixin, LegacyModelMixin

from .lookup_options import LegacyFileURLLookupOption

Image = get_image_model()
Document = get_document_model()


class BaseMediaFinder(BaseFinder):
    valid_file_url_patterns = None

    @classmethod
    def get_filepath_field_names(cls):
        return ["file"]

    @classmethod
    def get_lookup_options(cls):
        options = []
        if issubclass(cls.model, LegacyModelMixin):
            options.append(LegacyIDLookupOption())
        if issubclass(cls.model, LegacyImportedModelWithFileMixin):
            options.append(
                LegacyFileURLLookupOption(valid_patterns=cls.valid_file_url_patterns)
            )
        for name in cls.get_filepath_field_names:
            options.append(FilePathLookupOption(name))
        options.extend(cls.lookup_options or [])
        return options


class DocumentFinder(BaseMediaFinder):
    """
    Helps importers to find Wagtail Document instances by path/url or legacy id.
    """

    model = Document

    valid_file_url_patterns = [
        r"\.(pdf|doc|docx|odt|odp|xls|xlsx|ods|csv|tsv|pps|ppt|pptx|zip|tar)$"
    ]


class ImageFinder(BaseFinder):
    """
    Helps importers to find Wagtail Image instance by path/url or legacy id.
    """

    model = Image

    valid_file_url_patterns = [r"\.(png|gif|jpg|jpeg|webp)$"]
