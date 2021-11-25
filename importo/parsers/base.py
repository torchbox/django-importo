import warnings
from typing import Any

import bs4
from django.utils.functional import cached_property
from tate.legacy.finders import DocumentFinder, ImageFinder, PageFinder
from tate.legacy.utils.classes import CommandBoundObject
from wagtail.images import get_image_model

Image = get_image_model()


class BaseParser(CommandBoundObject):
    def parse(self, value: Any) -> Any:
        self.messages = []

    @staticmethod
    def get_soup(value: str) -> bs4.BeautifulSoup:
        """
        Return a ``bs4.BeautifulSoup`` instance representing the provided
        ``value``, which should be the contents of a HTML document, or
        more likely, as snippet of HTML as a string.

        NOTE: We're using the 'lxml' parser here, as it's already in the
        project's requirements, is faster, and will produce more
        consistant results than the default.
        """
        soup = bs4.BeautifulSoup(value, features="lxml")
        # Remove body, head and html tags (likely added by bs4)
        for elem in soup.find_all("body"):
            elem.unwrap()
        for elem in soup.find_all("head"):
            elem.unwrap()
        for elem in soup.find_all("html"):
            elem.unwrap()
        return soup

    def get_or_create_finder(self, key: str, finder_class: type):
        try:
            return self.command.finders[key]
        except AttributeError:
            warnings.warn(
                f"{type(self).__name__} instance is not bound to a command instance with "
                f"a 'finders' attribute, so is creating its own {finder_class} instance. "
                "Did you forget to run bind_to_command()?"
            )
        except KeyError:
            warnings.warn(
                f"The command instance bound to {type(self).__name__} cannot share a "
                f"finder instance matching the key '{key}', so the "
                f"{type(self).__name__} is creating its own {finder_class} instance."
            )
        return finder_class()

    @cached_property
    def page_finder(self) -> PageFinder:
        return self.get_or_create_finder("pages", PageFinder)

    @cached_property
    def document_finder(self) -> DocumentFinder:
        return self.get_or_create_finder("documents", DocumentFinder)

    @cached_property
    def image_finder(self) -> ImageFinder:
        return self.get_or_create_finder("images", ImageFinder)

    def find_image(self, value: Any):
        """
        Return a Wagtail image instance matching a supplied 'legacy system ID' value,
        or path/filename string.

        Raises ``django.core.exceptions.ObjectDoesNotExist`` if no such image can be found.
        """
        return self.image_finder.find(value)

    @cached_property
    def fallback_image(self):
        return Image.objects.all().first()

    def find_document(self, value: Any):
        """
        Return a Wagtail document instance matching a supplied 'legacy system ID' value,
        or path/filename string.

        Raises ``django.core.exceptions.ObjectDoesNotExist`` if no such document can be found.
        """
        return self.document_finder.find(value)

    def find_page(self, value: Any):
        """
        Return a Wagtail ``Page`` instance matching a supplied 'legacy system ID' value,
        url or path string.

        Raises ``Page.DoesNotExist`` if no such page can be found.
        """
        return self.page_finder.find(value)


class BaseRichTextContainingParser(BaseParser):
    richtext_parse_class = None

    @cached_property
    def richtext_parser(self):
        return self.richtext_parse_class(self.command)

    def parse_richtext(self, value: str) -> str:
        value = self.richtext_parser.parse(value)
        self.messages.extend(self.richtext_parser.messages)
        return value
