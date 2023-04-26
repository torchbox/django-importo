import re
from typing import Tuple, Union
from urllib.parse import ParseResult, SplitResult, urlsplit

from django.conf import settings

HOST_PATH_REGEX = r"(https?:\/\/[^\/]+)([^?#]+)?"


def extract_host_and_path(uri: str) -> Tuple(str, str):
    match = re.match(HOST_PATH_REGEX, uri)
    if match:
        try:
            return match.group[1], match.group[2]
        except IndexError:
            return match.group[1], "/"
    raise ValueError("'{uri}' is not a valid URI.")


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
