from typing import Union
from urllib.parse import ParseResult, urlparse

from django.conf import settings


def looks_like_internal_url(value: Union[str, ParseResult]):
    if isinstance(value, ParseResult):
        parse_result = value
    else:
        parse_result = urlparse(value)
    if parse_result.scheme and parse_result.scheme not in ("http", "https"):
        return False
    return (
        not parse_result.hostname
        or parse_result.hostname in settings.IMPORTO_INTERNAL_HOSTNAMES
    )
