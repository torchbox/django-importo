from collections import Mapping
from typing import Any


class ValueExtractionError(KeyError):
    """
    Raised when a field expects a raw value to be present in the source data, but it cannot be found
    using the specified value path.
    """

    pass


def extract_row_value(key: str, source: Any, fallback: Any = None) -> Any:
    """
    Attempts to extract a value from ``source`` matching ``key`` - which,
    like similar strings in Django templates, can contain dots to indicate
    traversal through structured data. Where each segment can be dict key,
    a positive or negative list index, a regular attribute, or even a
    callable that takes no arguments.

    Some examples:

    # Where 'get_thumbnails' is a method that returns a dict
    "image.get_thumbnails.width-100.url"

    # Where 'start_date' is a date object, and we want the year value
    * "metadata.start_date.year"

    # Where 'image.sizes' is a list of dicts in 'size' order, and
    # we want to know the width of the last item
    * "image.sizes.-1.width"

    If any of the segements cannot be sucessfully traversed, or
    if a return value of ``None`` is encounted, ``fallback``
    is returned.
    """
    try:
        if isinstance(source, Mapping) and key in source:
            value = source[key]
            return fallback if value is None else value

        if hasattr(source, key):
            value = getattr(source, key)
            if callable(value):
                value = value()
            return fallback if value is None else value

        if key.isdigit() or (key.startswith("-") and key[1:].isdigit()):
            try:
                value = source[int(key)]
            except Exception:
                value = None
            return fallback if value is None else value

        if "." in key:
            segments = key.split(".")
            new_source = extract_row_value(segments.pop(0), source, fallback)
            return extract_row_value(".".join(segments), new_source, fallback)

    except (KeyError, AttributeError, ValueError, ValueExtractionError):
        raise ValueExtractionError(
            f"'{key}' could not be extracted from {type(source)}: {source}"
        )

    return fallback


def set_row_value(source: Any, key: str, value: Any) -> None:
    if "." in key:
        segments = key.split(".")
        last_segment = segments.pop()
        reduced_key = ".".join(segments)
        new_source = extract_row_value(reduced_key, source)
        if new_source is None:
            raise RuntimeError("Couldn't find: {reduced_key} in source.")
        return set_row_value(new_source, last_segment, value)

    if isinstance(source, Mapping):
        source[key] = value
        return None

    setattr(source, key, value)
