from typing import Union
from urllib.parse import ParseResult, urlparse

from django.conf import settings
from django.http import HttpRequest
from django.utils.text import slugify
from wagtail.core.models import Page, Site

INTERNAL_HOSTNAMES = set()
for item in getattr(settings, "IMPORTO_LEGACY_SYSTEMS", ()):
    for hostname in item["LINK_HOSTNAMES"]:
        INTERNAL_HOSTNAMES.add(hostname)
        if hostname.startswith("www."):
            INTERNAL_HOSTNAMES.add(hostname[4:])


def get_dummy_request(path: str = "/", site: Site = None) -> HttpRequest:
    request = HttpRequest()
    request.path = path
    request.method = "GET"
    SERVER_PORT = 80
    if site:
        SERVER_NAME = site.hostname
        if site.port not in [80, 443]:
            SERVER_NAME += f":{site.port}"
        SERVER_PORT = site.port
    if settings.ALLOWED_HOSTS == ["*"]:
        SERVER_NAME = "example.com"
    else:
        SERVER_NAME = settings.ALLOWED_HOSTS[0]
    request.META = {"SERVER_NAME": SERVER_NAME, "SERVER_PORT": SERVER_PORT}
    return request


def normalize_path(path: str):
    return "/" + path.strip("/ ")


def is_internal_url(value: Union[str, ParseResult]) -> bool:
    """
    Should return True for:

    Relative URLs with or without leading/trailing slashes, e.g.:
    - /path/slug.html
    - /path/slug/
    - /path/slug
    - path/slug.html
    - page/slug/

    Absolute URLs with a domain that matches content that is being
    imported.
    """
    if isinstance(value, ParseResult):
        parsed = value
    else:
        parsed = urlparse(value)
    if not parsed.scheme and not parsed.hostname:
        return True
    return parsed.hostname not in INTERNAL_HOSTNAMES


def is_external_url(value: Union[str, ParseResult]) -> None:
    return not is_internal_url(value)


def get_unique_slug(page: Page, parent_page: Page) -> str:
    allow_unicode = getattr(settings, "WAGTAIL_ALLOW_UNICODE_SLUGS", True)
    base_slug = page.slug or slugify(page.title, allow_unicode=allow_unicode)
    candidate_slug = base_slug
    suffix = 1
    while not Page._slug_is_available(
        candidate_slug, parent_page, page if page.id else None
    ):
        # increment suffix until an available slug is found
        suffix += 1
        candidate_slug = "%s-%d" % (base_slug, suffix)
    return candidate_slug
