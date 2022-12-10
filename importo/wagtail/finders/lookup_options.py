import re
from typing import Iterable, Sequence

from django.core.exceptions import ObjectDoesNotExist
from django.http import Http404
from django.utils.functional import cached_property
from wagtail.core.models import Page, Site
from wagtail.core.query import PageQuerySet
from wagtail.core.urls import serve_pattern

from importo.finders.lookup_options import (
    BaseLookupOption,
    LookupValueError,
    ValueDomainInvalid,
    ValueTypeIncompatible,
)
from importo.finders.lookup_value import LookupValue
from importo.utils.urlpath import is_external_url
from importo.wagtail.utils import get_dummy_request


class ValueIncludesQueryString(LookupValueError):
    pass


class ValueIncludesFragment(LookupValueError):
    pass


class InvalidPageURLValue(LookupValueError):
    pass


class RoutableURLLookupOption(BaseLookupOption):
    def __init__(
        self,
        *,
        case_sensitive: bool = True,
        reject_urls_with_querystrings: bool = False,
        reject_urls_with_fragments: bool = False,
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
        patterns_match_path_only: bool = True,
    ):
        self.patterns_match_path_only = patterns_match_path_only
        self.reject_urls_with_querystrings = reject_urls_with_querystrings
        self.reject_urls_with_fragments = reject_urls_with_fragments
        super().__init__(
            case_sensitive=case_sensitive,
            valid_patterns=valid_patterns,
            invalid_patterns=invalid_patterns,
        )

    def value_matches_pattern(self, value: LookupValue, pattern: re.Pattern) -> bool:
        """
        Overrides ``BaseLookupOption.value_matches_pattern()`` to check the extracted
        ``path`` value against patterns, instead of the full raw value.
        """
        if self.case_sensitive:
            return bool(pattern.match(value.urlparsed.path))
        return bool(pattern.match(value.urlparsed.path, re.IGNORECASE))

    def validate_lookup_value(self, value: LookupValue) -> None:
        if not isinstance(value.raw, str):
            raise ValueTypeIncompatible
        if value.raw.is_digit():
            raise LookupValueError
        # Avoid lookups for domains we are not interested in
        if is_external_url(value.urlparsed):
            raise ValueDomainInvalid
        if self.reject_urls_with_querystrings and value.urlparsed.query:
            raise ValueIncludesQueryString
        if self.reject_urls_with_fragments and value.urlparsed.fragment:
            raise ValueIncludesFragment
        if not re.match(serve_pattern, value.urlparsed.path):
            raise InvalidPageURLValue
        super().validate_lookup_value(value)

    @cached_property
    def dummy_request(self):
        return get_dummy_request()

    @cached_property
    def all_sites(self):
        return Site.objects.all().select_related("root_page")

    def get_relevant_sites(
        self, hostname: str = None, port: int = None
    ) -> Iterable[Site]:
        if hostname is None:
            yield from self.all_sites
        else:
            hostname_and_port_match = None
            hostname_and_default_match = None
            default_match = None
            hostname_only_match = None

            for site in self.all_sites:
                if site.hostname.lower() == hostname.lower():
                    if site.port == port:
                        hostname_and_port_match = site
                    elif site.is_default_site:
                        hostname_and_default_match = site
                    else:
                        hostname_only_match = site
                elif site.is_default_site:
                    default_match = site

            if hostname_and_port_match:
                yield hostname_and_port_match
            elif hostname_and_default_match:
                yield hostname_and_default_match
            elif default_match:
                yield default_match
            elif hostname_only_match:
                yield hostname_only_match

    def find(self, value: LookupValue, queryset: PageQuerySet) -> Page:
        parse_result = value.urlparsed
        components = [pc for pc in parse_result.path.split("/") if pc]

        for site in self.get_relevant_sites(parse_result.hostname, parse_result.port):
            try:
                result = site.root_page.specific.route(
                    self.dummy_request, components
                ).page
            except Http404:
                continue
            if queryset.exists(id=result.id):
                return result
        raise ObjectDoesNotExist


class LegacyFileURLLookupOption(BaseLookupOption):
    pass
