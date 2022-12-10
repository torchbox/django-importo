from wagtail.models import Page

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
