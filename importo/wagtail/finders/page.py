import os
import re
from typing import Any, Iterable, Optional, Sequence, Union
from urllib.parse import ParseResult, urlparse
from django.core.exceptions import ObjectDoesNotExist

from django.core.management.base import BaseCommand
from django.http import Http404
from django.utils.functional import cached_property
from wagtail.core.models import Page

from importo.finders import BaseFinder
from importo.finders.lookup_options import LegacyIDLookupOption, LegacyURLLookupOption

from .lookup_options import RoutableURLLookupOption

class PageFinder(BaseFinder):
    """
    Helps importers to find Wagtail Page instance by legacy id, legacy path
    or page URL.
    """
    model = Page

    lookup_options = [
        LegacyIDLookupOption(),
        LegacyURLLookupOption(),
        RoutableURLLookupOption(),
    ]
