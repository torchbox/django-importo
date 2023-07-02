import re
from typing import Tuple, Union
from urllib.parse import ParseResult, SplitResult, urlsplit

from django.conf import settings

HOST_PATH_REGEX = re.compile(r"(https?:\/\/[^\/]+)([^?#]+)?")
SLUG_REGEX = re.compile(r"([\w\-]+)\.?[\w]*\/?$", re.UNICODE)


def extract_host_and_path(uri: str) -> Tuple[str, str]:
    match = HOST_PATH_REGEX.match(uri)
    if match:
        try:
            return match.group[1], match.group[2]
        except IndexError:
            return match.group[1], "/"
    raise ValueError(f"'{uri}' is not a valid URI.")


def extract_slug(path_or_uri: str) -> str:
    match = SLUG_REGEX.match(path_or_uri)
    if match:
        return match.group[0]
    raise ValueError(f"Slug could not be extracted from '{path_or_uri}'.")


INTERNAL_CONTENT_HOSTS = set(getattr(settings, "IMPORTO_INTERNAL_CONTENT_HOSTS", ()))
INTERNAL_MEDIA_HOSTS = set(getattr(settings, "IMPORTO_INTERNAL_MEDIA_HOSTS", ()))


def normalize_path(path: str) -> str:
    return "/" + path.strip("/ ")


def is_internal_uri(value: Union[str, ParseResult]) -> bool:
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
    if isinstance(value, (ParseResult, SplitResult)):
        parsed = value
    else:
        parsed = urlsplit(value)

    if not parsed.scheme and not parsed.hostname:
        return True
    return f"{parsed.scheme}://{parsed.hostname}" in INTERNAL_CONTENT_HOSTS


def is_media_uri(value: Union[str, ParseResult]) -> bool:
    if isinstance(value, (ParseResult, SplitResult)):
        parsed = value
    else:
        parsed = urlsplit(value)
    return f"{parsed.scheme}://{parsed.hostname}" in INTERNAL_MEDIA_HOSTS


def is_external_uri(value: Union[str, ParseResult]) -> bool:
    if isinstance(value, (ParseResult, SplitResult)):
        parsed = value
    else:
        parsed = urlsplit(value)
    return not is_internal_uri(parsed) and not is_media_uri(parsed)
