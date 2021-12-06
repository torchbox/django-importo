import os
import re
from typing import Any, Iterable, Optional, Sequence, Union
from urllib.parse import ParseResult, urlparse

from django.core.management.base import BaseCommand
from django.db.models import Model, Q
from django.http import Http404
from django.utils.functional import cached_property
from importo.finders.lookup_options.filename import FilePathLookupOption
from importo.finders.lookup_options.legacy_id import LegacyIDLookupOption
from importo.finders.lookup_options.path import LegacyPathLookupOption
from wagtail.core.models import Page, Site
from wagtail.core.query import PageQuerySet
from wagtail.core.sites import get_site_for_hostname
from wagtail.core.urls import serve_pattern
from wagtail.documents import get_document_model
from wagtail.images import get_image_model

from importo.finders import BaseFinder
from importo.finders import BaseLookupOption
from importo.finders.lookup_options import ModelFieldLookupOption
from importo.models import LegacyModelMixin
from importo.utils import get_dummy_request
from importo.utils.io import filename_from_url
from importo.wagtail.utils.query import get_legacy_page_matches, get_legacy_path_matches

Image = get_image_model()
Document = get_document_model()

class BaseMediaFinder(BaseFinder):

    valid_path_patterns = None

    @classmethod
    def get_filename_field_names(cls):
        return ["file"]

    @classmethod
    def get_lookup_options(cls):
        options = []
        if issubclass(cls.model, LegacyModelMixin):
            options.extend([
                LegacyIDLookupOption(),
                ModelFieldLookupOption(
                    "legacy_file_path", valid_patterns=cls.valid_path_patterns
                )
            ])
        for name in cls.get_filename_field_names:
            options.append(FilePathLookupOption(name))
        return options



class DocumentFinder(BaseMediaFinder):
    """
    Helps importers to find Wagtail Document instances by path/url or legacy id.
    """

    model = Document

    valid_path_patterns = [
        r"\.(pdf|doc|docx|odt|odp|xls|xlsx|ods|csv|tsv|pps|ppt|pptx|zip|tar)$"
    ]




class ImageFinder(BaseFinder):
    """
    Helps importers to find Wagtail Image instance by path/url or legacy id.
    """

    model = Image

    valid_path_patterns = [r"\.(png|gif|jpg|jpeg|webp)$"]
