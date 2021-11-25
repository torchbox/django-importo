import os
import re
from typing import Any, Iterable, Optional, Sequence, Union
from urllib.parse import ParseResult, urlparse

from django.core.management.base import BaseCommand
from django.db.models import Model, Q
from django.http import Http404
from django.utils.functional import cached_property
from wagtail.core.models import Page, Site
from wagtail.core.query import PageQuerySet
from wagtail.core.sites import get_site_for_hostname
from wagtail.core.urls import serve_pattern
from wagtail.documents import get_document_model
from wagtail.images import get_image_model

from importo.finders import BaseFinder
from importo.utils import get_dummy_request
from importo.utils.io import filename_from_url
from importo.wagtail.utils.query import get_legacy_page_matches, get_legacy_path_matches

Image = get_image_model()
Document = get_document_model()


class BaseMediaFinder(BaseFinder):
    """
    A base class for ImageFinder and DocumentFinder that is capable of finding
    files by path/filename as well as by legacy_id or pk.
    """

    def get_storage(self):
        return self.model._meta.get_field("file").storage

    def get_other_match(self, value: str) -> Model:
        return self.get_filename_match(value)

    def get_filename_match(self, value: str):
        not_found_msg = f"{self.model.__name__} matching '{value}' does not exist."

        # Remove path, and give filename the same treatment it would have recieved when cleaned
        filename = filename_from_url(os.path.basename(value))

        # Avoid lookups for filenames without a 2-4 char extension
        if not re.search(r"\.[a-zA-Z]{2,4}$", value):
            raise self.model.DoesNotExist(not_found_msg)

        # Give filename the same treatment it would have recieved when saved
        filename = self.get_storage().generate_filename(filename)

        # Assemble regex for finding an exact match, or a match with a 7 character
        # 'unique' string appended (the default behaviour for Django file storages
        # when a filename is not unique at the time of upload)
        file_root, file_ext = os.path.splitext(filename)
        filename_regex = (
            r"" + re.escape(file_root) + r"(_[a-zA-Z0-9]{7})?" + re.escape(file_ext)
        )
        q = Q()
        for field in self.get_filename_field_names():
            q |= Q(**{f"{field}__regex": filename_regex})

        # Analyse matches to identify the best match
        matches = tuple(self.get_queryset().filter(q))
        if not matches:
            raise self.model.DoesNotExist(not_found_msg)
        partial_matches = []
        for match in matches:
            if match.filename == filename:
                return match
            else:
                partial_matches.append(match)

        # Order partial matches by quality before choosing the first one
        partial_matches.sort(
            key=lambda x: self.get_partial_match_quality(
                x.filename, file_root, file_ext
            ),
            reverse=True,
        )
        try:
            return partial_matches[0]
        except IndexError:
            raise self.model.DoesNotExist(not_found_msg)

    def get_partial_match_quality(
        self,
        filename: str,
        lookup_file_root: str,
        lookup_file_ext: str,
    ):
        if filename.startswith(lookup_file_root):
            if filename.endswith(lookup_file_ext):
                return 5
            return 4
        return 1


class DocumentFinder(BaseMediaFinder):
    """
    Helps importers to find Wagtail Document instances by path/url or legacy id.
    """

    model = Document

    document_url_patterns = [
        r"\.(pdf|doc|docx|odt|odp|xls|xlsx|ods|csv|tsv|pps|ppt|pptx|zip|tar)$",
    ]

    def get_filename_field_names(self):
        return ["file"]

    def looks_like_other_value(self, value):
        return isinstance(value, str) and self.looks_like_document_url(value)

    def looks_like_document_url(self, value: Union[str, ParseResult]):
        """
        Returns a boolean indicating whether the provided `url` is worth
        attempting to find a matching Wagtail document for.

        Returns `False` for anything that:

        * Looks like an obvious 'external' URL (has a hostname for a different site)
        * Uses a scheme that isn't 'http' or 'https' (e.g. tel, mailto, sftp)
        * Matches at least one one pattern from self.document_url_patterns
        """
        if isinstance(value, ParseResult):
            parse_result = value
        else:
            parse_result = urlparse(value)
        if not self.looks_like_internal_url(parse_result):
            return False
        for pattern in self.document_url_patterns:
            if re.search(pattern, parse_result.path, re.IGNORECASE):
                return True
        return False


class ImageFinder(BaseMediaFinder):
    """
    Helps importers to find Wagtail Image instance by path/url or legacy id.
    """

    model = Image

    image_url_patterns = [
        r"\.(png|gif|jpg|jpeg|webp)$",
    ]

    def get_filename_field_names(self):
        return ["file"]

    def looks_like_other_value(self, value):
        return isinstance(value, str) and self.looks_like_image_url(value)

    def looks_like_image_url(self, value: Union[str, ParseResult]):
        """
        Returns a boolean indicating whether the provided `url` is worth
        attempting to find a matching Wagtail image for.

        Returns `False` for anything that:

        * Looks like an obvious 'external' URL (has a hostname for a different site)
        * Uses a scheme that isn't 'http' or 'https' (e.g. tel, mailto, sftp)
        * Matches at least one one pattern from self.image_url_patterns
        """
        if isinstance(value, ParseResult):
            parse_result = value
        else:
            parse_result = urlparse(value)
        if not self.looks_like_internal_url(parse_result):
            return False
        for pattern in self.image_url_patterns:
            if re.search(pattern, parse_result.path, re.IGNORECASE):
                return True
        return False


class SiteFinder(BaseFinder):
    model = Site

    def get_queryset(self):
        return self.model.objects.all().select_related("root_page")

    @cached_property
    def all_sites(self):
        return self.get_queryset()

    def find(self, hostname: str, port: int = None):
        key = f"{hostname}:{port}"
        try:
            return self._other_lookup_cache[key]
        except KeyError:
            pass

        # Use the same approach as Wagtail's Site.find_for_request()
        # to identify the most relevant site
        result = get_site_for_hostname(hostname, port)
        self.add_to_cache(result, key)
        return result


class PageFinder(BaseFinder):
    """
    Helps importers to find Wagtail Page instance by url/path or legacy id.
    """

    model = Page
    only_fields = [
        "id",
        "path",
        "title",
        "content_type",
        "url_path",
        "depth",
        "show_in_menus",
    ]

    non_page_url_patterns: Sequence[re.Pattern] = [
        r"\.(pdf|doc|docx|odt|odp|xls|xlsx|ods|csv|tsv|pps|ppt|pptx|zip|tar|xml|json|png|gif|jpg|jpeg|webp|tif|bmp)$"
    ]

    def __init__(self, command: BaseCommand = None):
        # Dummy value for calling Page.route()
        self.dummy_request = get_dummy_request()
        super().__init__(command)

    @cached_property
    def site_finder(self):
        return self.other_finders.get("sites", SiteFinder())

    def add_to_cache(self, value, other_cache_key: Any = None):
        self._other_lookup_cache[value.get_url(request=self.dummy_request)] = value
        super().add_to_cache(value, other_cache_key)

    def get_legacy_id_match_queryset(self, value: Any) -> PageQuerySet:
        if self.model is Page:
            return get_legacy_page_matches(value, "legacy_id", self.get_queryset())
        return super().get_legacy_id_match_queryset(value)

    def get_legacy_id_match(self, value, prefer_match_on="drupal_id"):
        return super().get_legacy_id_match(value, prefer_match_on)

    def looks_like_other_value(self, value):
        return (
            bool(value) and isinstance(value, str) and self.looks_like_page_url(value)
        )

    def looks_like_page_url(self, value: Union[str, ParseResult]):
        """
        Returns a boolean indicating whether the provided `url` is worth
        attempting to find a matching Wagtail page for.

        Returns `False` for anything that:

        * Looks like an obvious 'external' URL (has a hostname for a different site)
        * Uses a scheme that isn't 'http' or 'https' (e.g. tel, mailto, sftp)
        * Contains periods or other characters not recognised by Wagtail's 'serve' URL
        * Matches a pattern from `self.non_page_url_patterns`
        """
        if isinstance(value, ParseResult):
            parse_result = value
        else:
            parse_result = urlparse(value)

        if not (
            self.looks_like_internal_url(parse_result)
            and not parse_result.path.isdigit()
            and re.match(serve_pattern, parse_result.path)
        ):
            return False

        for pattern in self.non_page_url_patterns:
            if re.match(pattern, parse_result.path):
                return False

        return True

    def find_by_other(self, value: str) -> Page:
        """
        Return a `Page` matching the provided 'url' value - which could be
        a simple page path, or could also include other things like a domain,
        fragment identifier or GET params, that must be discarded.
        """
        parsed_url = urlparse(value)
        return self.find_by_path(
            parsed_url.path, parsed_url.hostname, parsed_url.port or 80
        )

    def find_by_path(
        self, path: str, hostname: Optional[str] = None, port: Optional[int] = None
    ) -> Page:
        """
        Return a `Page` matching the provided 'path' value - which should
        just be a simple 'path' string without any extra gubbins to confuse
        lookups. First we try routing to a page, like Wagtail would do if
        you visit the URL in a browser. If that doesn't work, we check for
        redirects that may have been created (to account for pages that may
        have moved).

        If provided, 'hostname' and 'port', are used to 'narrow down' the
        search to within relevant sites only, improving efficiency.
        """
        if path != "/":
            path = path.rstrip("/")
        cache_key = f"{hostname}:{port}:{path}"

        # Return cached result if there is one
        try:
            return self._other_lookup_cache[cache_key]
        except KeyError:
            pass

        result = None

        try:
            result = self.get_legacy_path_match(path)
        except self.model.DoesNotExist:
            pass

        for site in self.get_sites_to_search(hostname, port):
            try:
                result = self.route_to_page(site, path)
            except Http404:
                continue

        if result is None:
            # No match was found in any of the sites
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching path '{path}' does not exist."
            )

        result = result.specific_deferred
        self.add_to_cache(result, cache_key)
        return result

    def get_legacy_path_match(self, value: str) -> Page:
        result = get_legacy_path_matches(
            str(value), queryset=self.get_queryset(), exact=False
        ).first()
        if result is None:
            raise self.model.DoesNotExist(
                f"Page with legacy_path '{value}' does not exist."
            )
        return result

    def route_to_page(self, site: Site, path: str) -> Page:
        # Only attempt routing if the URL pattern matches that applied to Wagtail's 'serve' URL
        # (excludes paths with file extensions and the like)
        if re.match(serve_pattern, path):
            path_components = [pc for pc in path.split("/") if pc]

            # If we already have a page in the cache that is
            # close to the page we are looking for, route from
            # there to save a few database queries
            route_start_page = None
            remaining_components = path_components

            for i in range(1, len(path_components) - 1):
                partial_path = "/" + "/".join(path_components[:-i])
                lookup_keys = (
                    f"{site.hostname}:{site.port}:{partial_path}",
                    f"None:80:{partial_path}",
                )
                for key in lookup_keys:
                    route_start_page = self._other_lookup_cache.get(key)
                    if route_start_page is not None:
                        start_index = len(path_components) - i
                        remaining_components = path_components[start_index]
                        break
                if route_start_page is not None:
                    break

            # Route from the site's home page if there's nothing better
            if route_start_page is None:
                route_start_page = site.root_page.specific

            return route_start_page.route(self.dummy_request, remaining_components).page
        raise Http404

    def get_sites_to_search(
        self, hostname: str = None, port: int = None
    ) -> Iterable[Site]:
        """
        Return the sites that should be searched for pages in ``find_by_path()``
        Results are cached to avoid repeat queries during the same import run.
        """
        if not hostname:
            return self.site_finder.all_sites

        sites = []
        try:
            sites.append(self.site_finder.find(hostname, port))
        except Site.DoesNotExist:
            pass

        return sites
