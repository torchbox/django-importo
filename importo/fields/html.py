from typing import Any, Mapping, Sequence
from urllib.parse import unquote_plus

import bleach

from importo.fields.base import Field
from importo.utils.html import tidy_html


class HTMLField(Field):
    allowed_tags = [
        "a",
        "abbr",
        "acronym",
        "b",
        "bdi",
        "blockquote",
        "cite",
        "code",
        "dd",
        "dl",
        "dt",
        "em",
        "h2",
        "h3",
        "h4",
        "h5",
        "i",
        "li",
        "ol",
        "p",
        "small",
        "span",
        "strong",
        "ul",
    ]

    allowed_attrs = {
        "a": ["class", "href", "target", "title"],
        "abbr": ["title"],
        "acronym": ["title"],
        "cite": ["dir", "lang", "title"],
        "span": ["dir", "class", "lang", "title"],
        "h2": ["dir", "class", "lang", "title"],
        "h3": ["dir", "class", "lang", "title"],
        "h4": ["dir", "class", "lang", "title"],
        "h5": ["dir", "class", "lang", "title"],
    }

    def __init__(
        self,
        *args,
        allowed_tags: Sequence[str] = None,
        allowed_attrs: Mapping[str, str] = None,
        remove_empty_paragraphs: bool = True,
        remove_excess_whitespace: bool = True,
        remove_linebreaks: bool = False,
        **kwargs,
    ):
        if allowed_tags is not None:
            self.allowed_tags = allowed_tags
        if allowed_attrs is not None:
            self.allowed_attrs = allowed_attrs
        self.remove_empty_paragraphs = remove_empty_paragraphs
        self.remove_excess_whitespace = remove_excess_whitespace
        self.remove_linebreaks = remove_linebreaks
        super().__init__(*args, **kwargs)

    def to_python(self, value: Any) -> str:
        value = unquote_plus(str(value))

        # TODO: Add some way for the field to highlight/log when HTML is stripped
        value = bleach.clean(
            value, tags=self.allowed_tags, attributes=self.allowed_attrs, strip=True
        )
        return tidy_html(
            value,
            remove_empty_paragraphs=self.remove_empty_paragraphs,
            remove_excess_whitespace=self.remove_excess_whitespace,
            remove_linebreaks=self.remove_linebreaks,
        )
