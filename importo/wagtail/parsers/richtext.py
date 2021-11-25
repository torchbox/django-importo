import uuid
from urllib.parse import urlparse

import bs4
from django.core.exceptions import ObjectDoesNotExist
from tate.utils.html import tidy_html

from .base import BaseParser


class LinkMatchError:
    __slots__ = ["msg", "exception"]

    def __init__(self, msg: str, exception: Exception = None):
        self.msg = msg
        self.exception = exception


class RichTextParser(BaseParser):
    """
    A parser that turns a block of HTML (string) into a value suitable for a
    ``RichTextField`` of ``RichTextBlock``. This process typically involves:

    -   Stripping out any unsuitable HTML
    -   Updating images with special data attributes that are usually added by Wagtail.
    -   Updating page, document and footnote links to include special data attributes
        that are usually added by Wagtail.

    The following helper methods/properties are inherited from ``BaseParser``:

    - get_soup(value)
    - image_finder (property)
    - document_finder (property)
    - page_finder (property)
    - find_image(value)
    - find_document(value)
    - find_page(value)
    """

    tags_replace = {
        "b": "strong",
        "i": "em",
        "h5": "p",
        "h6": "p",
    }

    allowed_tags = [
        "a",
        "br",
        "em",
        "h2",
        "h3",
        "h4",
        "img",
        "li",
        "ol",
        "p",
        "strong",
        "ul",
    ]

    allowed_attributes = {
        "a": ["href", "title", "class", "name", "id"],
        "img": ["alt" "src", "class", "id"],
    }

    def parse(self, value, link_replacement_only=False) -> str:
        self.link_match_errors = []
        self.messages = []
        if not value:
            return ""
        self.soup = self.get_soup(value)
        if not link_replacement_only:
            self.replace_tags()
            self.remove_unwanted_html()
            self.update_footnote_links()
        self.update_internal_links()
        return tidy_html(str(self.soup))

    def replace_tags(self, tag=None):
        if tag is None:
            tag = self.soup

        if isinstance(tag, list):
            for item in tag:
                self.replace_tags(item)
            return

        if getattr(tag, "name", None) in self.tags_replace:
            tag.name == self.tags_replace[tag.name]

    def remove_unwanted_html(self, tag=None):
        if tag is None:
            tag = self.soup

        if not isinstance(tag, bs4.BeautifulSoup) and getattr(tag, "name", None):
            if tag.name not in self.allowed_tags:
                tag.unwrap()
            else:
                allowed_attrs = self.allowed_attributes.get(tag.name) or ()
                for key in tuple(tag.attrs.keys()):
                    if key not in allowed_attrs:
                        del tag.attrs[key]

        if hasattr(tag, "contents"):
            for child in tag.contents:
                self.remove_unwanted_html(child)

    def update_footnote_links(self) -> None:
        """
        Turn footnote links into ``<footnote>`` elements, with an ``id``
        attribute value matching the UUID of the relevant ``Footnote``
        from ``self.footnotes_data``.
        """
        for tag in self.soup.select('a[href^="#footnote"]'):
            # Use same method as FootnoteParser.parse() to turn
            # the 6-digit value from Drupal to a full UUID
            id = uuid.uuid3(uuid.NAMESPACE_DNS, tag["href"].split("_").pop())
            footnote = self.soup.new_tag("footnote", id=id)
            footnote.string = f"[{str(id)[:6]}]"
            tag.replace_with(footnote)

    def update_internal_links(self) -> None:
        for tag in self.soup.find_all("a", href=True):
            """
            For links that look like Document links
            1.  Add a ``linktype`` attribute with the value ``"document"``.
            2.  Add an ``id`` attribute with a value matching the PK of the ``Document`` object.
            3.  Remove the ``href`` attribute.

            For links that look like Page links
            1.  Add a ``linktype`` attribute with the value ``"page"``.
            2.  Add an ``id`` attribute with a value matching the PK of the relevant
            ``Page`` object.
            3.  Remove the ``href`` attribute.
            """
            url = tag["href"]

            # Add missing scheme to external urls
            if url.startswith("/www."):
                tag["href"] = f"http:/{url}"
                continue

            try:
                parse_result = urlparse(url)
            except ValueError as e:
                self.link_match_errors.append(
                    LinkMatchError(f"Invalid richtext link encountered: '{url}'", e)
                )
                continue

            if parse_result.fragment:
                self.log_debug(f"Leaving richtext link with fragment alone: '{url}'.")
                continue

            if self.document_finder.looks_like_document_url(parse_result):
                self.log_debug(f"Looking for document '{url}'.")
                try:
                    document = self.find_document(parse_result.path)
                except ObjectDoesNotExist as e:
                    msg = "Failed to update richtext link"
                    self.link_match_errors.append(LinkMatchError(msg, e))
                    self.log_debug(msg)
                else:
                    self.log_debug("Richtext link updated successfully")
                    tag["linktype"] = "document"
                    tag["id"] = document.pk
                    del tag["href"]
                continue  # Avoid trying to match URL to a page

            if self.page_finder.looks_like_page_url(parse_result):
                self.log_debug(f"Looking for page '{url}'.")
                try:
                    page = self.find_page(url)
                except ObjectDoesNotExist as e:
                    msg = "Failed to update richtext link"
                    self.link_match_errors.append(LinkMatchError(msg, e))
                    self.log_debug(msg)
                else:
                    self.log_debug("Richtext link updated successfully")
                    tag["linktype"] = "page"
                    tag["id"] = page.pk
                    del tag["href"]
                continue
