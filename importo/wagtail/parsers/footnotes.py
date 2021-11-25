import uuid
from typing import Sequence

from wagtail_footnotes.models import Footnote

from .base import BaseRichTextContainingParser
from .richtext import RichTextParser


class FootnotesParser(BaseRichTextContainingParser):
    """
    The Tate API provides footnotes in HTML form (a <ul> with an <li> for each item).
    This parser needs to break that HTML down and return a list of (unsaved)
    ``wagtailfootnotes.models.Footnote`` objects, so they can be saved with the rest
    of the page data.

    Because the ``Footnote`` model uses a ``RichTextField`` for ``text``, the content
    of each item must be parsed using `self.parse_richtext()`.
    """

    richtext_parse_class = RichTextParser

    def parse(self, value: str) -> Sequence[Footnote]:
        self.messages = []
        footnotes = []
        soup = self.get_soup(value)
        for item in soup.select("li.footnote"):
            # Use same method as RichtextParser.update_footnote_links()
            # to turn the 6-digit value from Drupal to a full UUID
            id = uuid.uuid3(uuid.NAMESPACE_DNS, item["id"].split("_").pop())
            contents = "".join(str(c) for c in item.contents)
            footnotes.append(
                Footnote(
                    uuid=id,
                    text=self.parse_richtext(contents),
                )
            )
        return footnotes
