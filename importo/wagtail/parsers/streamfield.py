import re
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlparse

from bs4.element import NavigableString, Tag
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.utils.text import slugify
from tate.core.blocks.banners import (
    BannerHeightChoices,
    TextAlignmentChoices,
    TextStyleChoices,
)
from tate.core.blocks.utils import EmbedHTMLBlock, TableBlock
from tate.core.constants import (
    CTAIconChoices,
    CTARowAlignmentChoices,
    MediaBlockStyleChoices,
    PromoCardStyleChoices,
    StripColumnChoices,
    StripLayoutChoices,
    StripStyleChoices,
    WrapStyleChoices,
)
from tate.events.models import EventVenuePage
from wagtail.images import get_image_model

from .base import BaseRichTextContainingParser
from .richtext import RichTextParser
from .utils import dump

if TYPE_CHECKING:
    from tate.art.archives.models import ArchiveItemPage, ArchivePage
    from tate.art.artists.models import ArtistPage
    from tate.art.artworks.models import ArtworkPage
    from tate.events.models import EventPage


Image = get_image_model()


API_EMBED_TYPE_TO_CARD_TYPE = {
    "artist": "artist",
    "artwork": "artwork",
    "node": "page",
    "venue": "page",
    "gallery": "page",
    "audio_node": "page",
    "video_node": "page",
    "event": "event",
    "promo": "promo",
    "external": "external",
    "shop_ref": "shop_product",
    "shop": "shop_product",
}

API_EMBED_TYPE_TO_FIGURE_TYPE = {
    "column": "column",
    "image": "image",
    "quote": "quote",
    "media": "legacy_embed",
    "code": "legacy_embed",
    "artwork": "image",
}

COLUMNS_INT_TO_BLOCK_VALUE = {
    1: StripColumnChoices.ONE,
    2: StripColumnChoices.TWO,
    3: StripColumnChoices.THREE,
    4: StripColumnChoices.FOUR,
    5: StripColumnChoices.FIVE,
    6: StripColumnChoices.SIX,
}

RICHTEXT_BLOCK_ELEMENT_NAMES = ("ol", "p", "h3", "h4", "h5", "ul")

RICHTEXT_INLINE_ELEMENT_NAMES = ("a", "em", "i", "span", "strong", "small")


class StreamFieldContentParser(BaseRichTextContainingParser):
    """
    A parser that interprets the structured ``page_sections`` data provided by
    the Tate API, and turns it into structured 'streamfield block' data,
    suitable for use as the page's ``body`` field value.

    The field using this parser will take care of serializing the data to a
    JSON string, so the parser doesn't need to worry about that.

    Any bits of content intended for a ``RichTextBlock`` should be parsed
    using ``self.parse_richtext(value)``.

    The following helper methods are inherited from ``BaseParser`` to utilize
    in custom code:

    - get_soup(value)
    - image_finder (property)
    - document_finder (property)
    - page_finder (property)
    - find_image(value)
    - find_document(value)
    - find_page(value)
    """

    richtext_parse_class = RichTextParser
    ignore_section_types = []

    def parse(self, value: Sequence[Dict[str, Any]]):
        self.messages = []
        self.value = value

        self.clean_banners()
        self.clean_strips()
        self.clean_autostrips()
        self.clean_content_sections()
        self.clean_article_footers()
        self.clean_custom_blocks()
        self.remove_redundant_sections()

        return self.value

    # Finder methods with built-in logging

    def log_not_found(
        self,
        obj_type: str,
        value: Union[int, str],
        block_type: str = None,
        block_id: uuid.UUID = None,
    ):
        msg = f"No {obj_type} was found matching '{value}'."
        if block_type and block_id:
            msg += f" A placeholder value was used for <{block_type} id='{block_id}'>."
        self.messages.append(msg)

    def log_image_download_error(
        self,
        uri: str,
        block_type: str,
        block_id: uuid.UUID,
        error: ValidationError,
    ):
        self.messages.append(
            f"Image '{uri}' could not be downloaded for <{block_type} id='{block_id}'>: {error}."
        )

    def find_or_download_image_for_block(
        self,
        file_path: str,
        block_type: str,
        block_id: uuid.UUID,
        title: str = "",
        alt: str = "",
        caption: str = "",
    ) -> Image:
        from tate.legacy.constants import SHRINK_IMAGE
        from tate.legacy.fields import ImageFileField

        file_path = file_path.replace(
            "public://", "https://www.tate.org.uk/sites/default/files/"
        )

        try:
            self.log_debug(f"Looking for existing image: '{file_path}'")
            return self.find_image(file_path)
        except ObjectDoesNotExist:
            pass

        image_field = ImageFileField(
            "+",
            "file",
            max_width=4000,
            max_height=4000,
            on_max_dimensions_exceeded=SHRINK_IMAGE,
            command=self.command,
        )
        try:
            image_file = image_field.clean(file_path)
        except ValidationError as e:
            self.log_image_download_error(file_path, e, block_type, block_id)
            raise Image.DoesNotExist(
                f"The image '{file_path}' could be found locally OR downloaded."
            )

        kwargs = {
            "title": title,
            "alt": alt,
            "legacy_path": file_path,
            "rich_caption": self.parse_richtext(caption),
        }
        # Add the downloaded image to the library
        if not kwargs["title"]:
            for_obj = self.command.current_object
            kwargs["title"] = (
                f"Downloaded {block_type} image for {type(for_obj).__name__} "
                f"{for_obj.pk or '(NEW)'}"
            )

        obj = Image(**kwargs)
        error_to_reraise = ValidationError(
            f"Error creating new image with details: {kwargs}"
        )
        try:
            # ImageFileField.update_object() does some useful additional stuff,
            # like setting the file_hash, width and height fields
            image_field.update_object(obj, image_file, True)
        except Exception as e:
            raise error_to_reraise from e
        try:
            # save the fully updated image
            obj.save()
        except Exception as e:
            raise error_to_reraise from e

        # Add to the finder cache for faster repeat lookups
        self.image_finder.add_to_cache(obj, file_path)
        return obj

    def find_document_for_block(
        self, lookup_value: Union[int, str], block_type: str, block_id: uuid.UUID
    ):
        if isinstance(lookup_value, str):
            lookup_value = lookup_value.strip()
        try:
            return self.find_document(lookup_value)
        except ObjectDoesNotExist:
            self.log_not_found("Document", lookup_value, block_type, block_id)
            raise

    def find_page_for_block(
        self, lookup_value: Union[int, str], block_type: str, block_id: uuid.UUID
    ):
        if isinstance(lookup_value, str):
            lookup_value = lookup_value.strip()
        try:
            return self.find_page(lookup_value)
        except ObjectDoesNotExist:
            self.log_not_found("Page", lookup_value, block_type, block_id)
            raise

    # Extra Tate-specific finders

    def find_artist(self, value) -> "ArtistPage":
        return self.command.finders["artists"].find(value)

    def find_artist_for_block(
        self, lookup_value: Union[int, str], block_type: str, block_id: uuid.UUID
    ):
        if isinstance(lookup_value, str):
            lookup_value = lookup_value.strip()
        try:
            return self.find_artist(lookup_value)
        except ObjectDoesNotExist:
            self.log_not_found("ArtistPage", lookup_value, block_type, block_id)
            raise

    def find_artwork(self, value) -> "ArtworkPage":
        return self.command.finders["artworks"].find(value)

    def find_artwork_for_block(
        self, lookup_value: Union[int, str], block_type: str, block_id: uuid.UUID
    ):
        if isinstance(lookup_value, str):
            lookup_value = lookup_value.strip()
        try:
            return self.find_artwork(lookup_value)
        except ObjectDoesNotExist:
            self.log_not_found("ArtworkPage", lookup_value, block_type, block_id)
            raise

    def find_collection_page(
        self, value
    ) -> Union["ArtworkPage", "ArchivePage", "ArchiveItemPage"]:
        return self.command.finders["collection_pages"].find(value)

    def find_collection_page_for_block(
        self, lookup_value: str, block_type: str, block_id: uuid.UUID
    ):
        try:
            return self.find_collection_page(lookup_value.strip())
        except ObjectDoesNotExist:
            self.log_not_found("CollectionPage", lookup_value, block_type, block_id)
            raise

    def find_event(self, value) -> "EventPage":
        return self.command.finders["events"].find(value)

    def find_event_for_block(
        self, lookup_value: Union[int, str], block_type: str, block_id: uuid.UUID
    ):
        if isinstance(lookup_value, str):
            lookup_value = lookup_value.strip()
        try:
            return self.find_event(lookup_value)
        except ObjectDoesNotExist:
            self.log_not_found("EventPage", lookup_value, block_type, block_id)
            raise

    def generate_id(self):
        return uuid.uuid4()

    def clean_custom_blocks(self):
        pass

    def remove_redundant_sections(self):
        value_new = []
        for section in self.value:
            if section["type"] not in self.ignore_section_types:
                value_new.append(section)
        self.value = value_new

    def clean_banners(self):
        value_new = []
        for section in self.value:
            if section["type"] == "strip_banner_v2":
                value_new.append(self.banner_block_from_api_data(section))
            else:
                value_new.append(section)
        self.value = value_new

    def banner_block_from_api_data(self, section: Dict[str, Any]) -> Dict[str, Any]:
        block_id = self.generate_id()
        height_int = int(section.get("banner_height", 33))
        if height_int == 33:
            height = BannerHeightChoices.THIRD
        elif height_int == 50:
            height = BannerHeightChoices.HALF
        elif height_int == 60:
            height = BannerHeightChoices.SIXTY
        elif height_int == 70:
            height = BannerHeightChoices.SEVENTY
        elif height_int == 80:
            height = BannerHeightChoices.EIGHTY
        elif height_int == 90:
            height = BannerHeightChoices.NINETY
        else:
            height = BannerHeightChoices.FULL

        text_alignment = TextAlignmentChoices.CENTERED
        text_style = TextStyleChoices.BLACK_TEXT
        for classname in section.get("classes") or []:
            if "black__overlay" in classname:
                text_style = TextStyleChoices.BLACK_TEXT_WITH_OVERLAY
            elif "white__overlay" in classname:
                text_style = TextStyleChoices.WHITE_TEXT_WITH_OVERLAY
            elif "white" in classname:
                text_style = TextStyleChoices.WHITE_TEXT
            elif "left" in classname:
                text_alignment = TextAlignmentChoices.LEFT
            elif "right" in classname:
                text_alignment = TextAlignmentChoices.RIGHT

        blocks = self.extract_content_blocks_from_paragraph_text(
            section.get("banner_overlay_content"),
            allow_h2_in_richtext=True,
            heading_blocks_supported=False,
            cta_row_blocks_supported=True,
            table_html_blocks_supported=False,
            embed_html_blocks_supported=False,
        )

        text = "".join(rt["value"] for rt in blocks if rt["type"] == "rich_text")
        ctas = []
        for block in blocks:
            if block["type"] == "cta_row":
                ctas = block["value"]["items"]

        background_image = None
        if image_uri := section.get("banner_image"):
            try:
                background_image = self.find_or_download_image_for_block(
                    image_uri, "ImageBanner", block_id
                ).pk
            except (ObjectDoesNotExist, ValidationError):
                background_image = self.fallback_image.pk

        return {
            "type": "banner",
            "value": {
                "background_image": background_image,
                "height": height,
                "text": text,
                "text_style": text_style,
                "text_alignment": text_alignment,
                "ctas": ctas,
            },
            "id": block_id,
        }

    def clean_strips(self):
        value_new = []
        for section in self.value:
            if section["type"] == "strip":
                if title := section.get("title", "").strip():
                    value_new.append(
                        self.heading_block_from_title(title, show_in_inpage_nav=True)
                    )
                value_new.append(self.strip_block_from_api_data(section))
            else:
                value_new.append(section)
        self.value = value_new

    def strip_block_from_api_data(self, section: Dict[str, Any]) -> Dict[str, Any]:
        background = section.get("kids_background", "")
        cards = section.get("cards") or section.get("embeds") or []
        columns = COLUMNS_INT_TO_BLOCK_VALUE.get(section.get("columns", len(cards)))
        layout = ""
        style = ""
        for classname in section.get("classes", []):
            if "carousel" in classname:
                layout = StripLayoutChoices.CAROUSEL
            elif "masonry" in classname:
                layout = StripLayoutChoices.MASONRY
            elif "title-over-image" in classname:
                layout = StripLayoutChoices.OVER_IMAGE
            elif "image-canvas" in classname:
                layout = StripLayoutChoices.CANVAS
            elif "border" in classname:
                layout = StripLayoutChoices.CANVAS_BORDER
            elif "align" in classname:
                layout = StripLayoutChoices.CENTER_ALIGN
            elif "cloud" in classname:
                layout = StripLayoutChoices.ART_TERMS_TAG_CLOUD
            elif "portrait" in classname:
                style = StripStyleChoices.PORTRAIT
            elif "landscape" in classname:
                style = StripStyleChoices.LANDSCAPE
            elif "2-col-mobile" in classname:
                style = StripStyleChoices.PORTRAIT_2_COL_MOBILE
            elif "thumbnail" in classname:
                style = StripStyleChoices.THUMBNAIL
            elif "alternate" in classname:
                style = StripStyleChoices.ALTERNATE

        return {
            "type": "strip",
            "value": {
                "background": background,
                "cards": self.get_card_blocks_from_api_data(cards),
                "columns": columns,
                "style": style,
                "layout": layout,
            },
        }

    def clean_autostrips(self):
        value_new = []
        for section in self.value:
            if (
                section["type"] == "autostrip"
                and section.get("content_type", "") == "auto_content_strip"
                and section.get("content_category", "") == "press_release"
            ):
                # Special case for PressLandingPage
                pass
            elif section["type"] == "autostrip":
                strip_block = self.get_strip_block_from_api_data(section)
                if strip_block is None:
                    self.messages.append(
                        f"Removing unsupported 'autostrip': {dump(section)}"
                    )
                    continue
                if title := section.get("title", "").strip():
                    value_new.append(
                        self.heading_block_from_title(title, show_in_inpage_nav=True)
                    )
                value_new.append(strip_block)
            else:
                value_new.append(section)
        self.value = value_new

    def get_strip_block_from_api_data(
        self, section: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        ct = section.get("content_type", "")
        if ct == "auto_shop_strip":
            return self.make_shop_category_strip_block(section)
        if ct == "auto_event_strip":
            return self.make_event_strip_block(section)
        return None

    def make_shop_category_strip_block(self, section: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "type": "shop_category_strip",
            "value": {"category_id": section["shop_category_id"]},
        }

    def make_event_strip_block(self, section: Dict[str, Any]) -> Dict[str, Any]:
        venue_slug = section.get("gallery_group", "").replace("_", "-")
        venue_ids = []
        if venue_slug:
            if venue_slug == "tate-st-ives":
                venue_ids.extend(
                    EventVenuePage.objects.filter(
                        slug__in=[
                            "tate-st-ives",
                            "barbara-hepworth-museum-and-sculpture-garden",
                        ]
                    ).values_list("id", flat=True)
                )
            else:
                venue = (
                    EventVenuePage.objects.filter(slug=venue_slug).values("id").first()
                )
                if venue:
                    venue_ids.append(venue["id"])
        return {
            "type": "event_strip",
            "value": {
                "max_items": None,
                "columns": StripColumnChoices.THREE,
                "layout": None,
                "audiences": section.get("audience_type", []),
                "venues": venue_ids,
                "types": [],
            },
        }

    def heading_block_from_title(self, title: str, show_in_inpage_nav=True):
        return {
            "type": "heading",
            "value": {
                "text": title,
                "show_in_inpage_nav": show_in_inpage_nav,
                "html_id": slugify(title),
            },
        }

    def clean_content_sections(self):
        self.add_accordion_blocks()
        self.group_content_into_sections()
        self.clean_section_blocks()

    def clean_article_footers(self):
        for section in self.value:
            if section["type"] == "article_footer":
                section["type"] == "main_content_footer"
                section["value"] = None

    def add_accordion_blocks(self):
        """
        'collapsed' page sections with simple heading text and content are to be
        migrated to the new 'Accordion' block type, which groups together several such
        sections. This method finds series of suitable 'page_section' blocks and nests
        them within an 'accordion' block value.
        """
        value_new = []
        accordion_items = []
        for section in self.value:
            if (
                section["type"] == "page_section"
                and not section.get("classes")
                and section.get("style", "") == "collapsed"
            ):
                accordion_items.append(
                    {
                        "heading": section.get("heading", ""),
                        "collapsed": True,
                        "content": self.extract_content_blocks_from_paragraph_text(
                            section.get("text", ""),
                            allow_h2_in_richtext=False,
                            heading_blocks_supported=False,
                            cta_row_blocks_supported=True,
                            table_html_blocks_supported=True,
                            embed_html_blocks_supported=True,
                        ),
                    }
                )
            else:
                self._add_accordion_block(accordion_items, to=value_new)
                value_new.append(section)

        # In case there are remaining 'accordion_items'
        self._add_accordion_block(accordion_items, to=value_new)
        self.value = value_new

    def _add_accordion_block(self, items: Sequence[Dict[str, Any]], to: List[Dict]):
        if not items:
            return
        to.append({"type": "accordion", "value": {"children": list(items)}})
        items.clear()

    def group_content_into_sections(self):
        """
        Only banners, headings and some types of strip will be represented at the
        root level in StreamField content. The rest is grouped into 'section' blocks.
        This method identifies anything that doesn't belong at the top-level and nests
        them under fake 'section' blocks to better resemble the target structure.
        """
        value_new = []
        section_contents = []
        for section in self.value:
            if section["type"] in ["accordion", "page_section"]:
                section_contents.append(section)
            else:
                self._add_section_block(section_contents, to=value_new)
                value_new.append(section)

        # In case there are remaining 'section_contents'
        self._add_section_block(section_contents, to=value_new)
        self.value = value_new

    def _add_section_block(
        self,
        contents: Sequence[Dict[str, Any]],
        to: List[Dict[str, Any]],
        background: str = None,
    ):
        if not contents:
            return

        block = {
            "type": "section",
            "value": {
                "content": list(contents),
            },
        }
        if background:
            block["value"]["background"] = background

        to.append(block)
        contents.clear()

    def clean_section_blocks(self):
        for section in self.value:
            if section["type"] != "section":
                continue
            section["value"]["content"] = self._clean_section_block_contents(
                section["value"]["content"]
            )

    def _clean_section_block_contents(self, value: Sequence[Dict[str, Any]]):
        blocks = []
        for block in value:
            if block["type"] == "page_section":

                if heading := block.pop("heading", "").strip():
                    blocks.append({"type": "heading", "value": {"text": heading}})

                media_blocks = []
                if block.get("embeds"):
                    media_blocks.extend(
                        self._extract_media_blocks_from_page_section(block)
                    )

                text_blocks = []
                if text := block.get("text"):
                    text_blocks.extend(
                        self.extract_content_blocks_from_paragraph_text(
                            text,
                            allow_h2_in_richtext=False,
                            heading_blocks_supported=True,
                            cta_row_blocks_supported=True,
                            table_html_blocks_supported=True,
                            embed_html_blocks_supported=True,
                        )
                    )

                if block.get("text_first", False):
                    blocks.extend(text_blocks)
                    blocks.extend(media_blocks)
                else:
                    blocks.extend(media_blocks)
                    blocks.extend(text_blocks)

            else:
                blocks.append(block)
        return blocks

    def _extract_media_blocks_from_page_section(self, value):
        embeds = value.get("embeds")
        used_embed_types = set(item["type"] for item in embeds)
        block_classnames = value.get("classes") or []
        media_style = self.media_style_from_classes(block_classnames)
        if used_embed_types in ({"image"}, {"artwork"}, {"artwork", "image"}):
            image_blocks = []
            for embed in embeds:
                if embed["type"] == "image":
                    block = self.image_block_from_api_image_data(embed)
                    if block is not None:
                        image_blocks.append(block)
                else:
                    block = self.image_block_from_api_artwork_data(embed)
                    if block is not None:
                        image_blocks.append(block)

            if len(image_blocks) == 1:
                # Single images become a `SingleImageBlock` value
                block = image_blocks[0]
                block["value"]["wrap_style"] = self.wrap_style_from_classes(
                    block_classnames
                )
                yield block
            else:
                # Multiple images become an `ImageGalleryBlock` value
                yield {
                    "type": "image_gallery",
                    "value": {"images": image_blocks, "style": media_style, "text": ""},
                }
        elif any(
            bool(et in used_embed_types) for et in API_EMBED_TYPE_TO_FIGURE_TYPE.keys()
        ):
            # A series of 'Figure' blocks become a `FiguresBlock` value
            yield {
                "type": "figures",
                "value": {
                    "figures": self.get_figure_blocks_from_api_data(value["embeds"]),
                    "style": media_style,
                },
            }

        elif any(
            bool(et in used_embed_types) for et in API_EMBED_TYPE_TO_CARD_TYPE.keys()
        ):
            # A series of 'Card' blocks become a `CardsBlock` value
            yield {
                "type": "cards",
                "value": {
                    "cards": self.get_card_blocks_from_api_data(value["embeds"]),
                    "style": media_style,
                },
            }
        else:
            raise ValidationError(
                f"Couldn't interpret 'embeds' value for page_section:\n\n {dump(value)}"
            )

    @staticmethod
    def media_style_from_classes(classes):
        val = " ".join(classes)
        if "2-col" in val:
            if "full-width" in val:
                return MediaBlockStyleChoices.TWO_COL_FULL_WIDTH
            return MediaBlockStyleChoices.TWO_COL

        if "3-col" in val:
            if "masonry" in val:
                if "full-width" in val:
                    return MediaBlockStyleChoices.THREE_COL_MASONRY_FULL_WIDTH
                return MediaBlockStyleChoices.THREE_COL_MASONRY
            if "full-width" in val:
                return MediaBlockStyleChoices.THREE_COL_FULL_WIDTH
            return MediaBlockStyleChoices.THREE_COL

        if "4-col" in val:
            if "masonry" in val:
                if "full-width" in val:
                    return MediaBlockStyleChoices.FOUR_COL_MASONRY_FULL_WIDTH
                return MediaBlockStyleChoices.FOUR_COL_MASONRY
            if "full-width" in val:
                return MediaBlockStyleChoices.FOUR_COL_FULL_WIDTH
            return MediaBlockStyleChoices.FOUR_COL

        if "slideshow" in val:
            if "full-width" in val:
                return MediaBlockStyleChoices.ONE_COL_SLIDESHOW_FULL_WIDTH
            return MediaBlockStyleChoices.ONE_COL_SLIDESHOW

        if "left" in val:
            if "pull" in val:
                return MediaBlockStyleChoices.ONE_COL_LEFT_PULL
            return MediaBlockStyleChoices.ONE_COL_LEFT_FLUSH

        if "right" in val:
            if "pull" in val:
                return MediaBlockStyleChoices.ONE_COL_RIGHT_PULL
            return MediaBlockStyleChoices.ONE_COL_RIGHT_FLUSH

        if "full-width" in val:
            return MediaBlockStyleChoices.ONE_COL_FULL_WIDTH
        return MediaBlockStyleChoices.ONE_COL

    @staticmethod
    def wrap_style_from_classes(classes):
        if "aside--left" in classes:
            if "aside--pull" in classes:
                return WrapStyleChoices.LEFT
            return WrapStyleChoices.FLUSH_LEFT
        if "aside--right" in classes:
            if "aside--pull" in classes:
                return WrapStyleChoices.RIGHT
            return WrapStyleChoices.FLUSH_RIGHT
        return WrapStyleChoices.CENTRE

    # -------------------------------------------------------------------------
    # Drupal paragraph text -> Streamfield blocks
    # -------------------------------------------------------------------------

    def extract_content_blocks_from_paragraph_text(
        self,
        value,
        allow_h2_in_richtext=False,
        heading_blocks_supported=True,
        cta_row_blocks_supported=True,
        table_html_blocks_supported=True,
        embed_html_blocks_supported=True,
    ):
        """
        In the new site, some things that used to be added into 'paragraph text'
        in Drupal are implemented in Wagtail using dedicated blocks. This
        method attempts to extract this content from blocks that look destined
        for richtext, and re-adds that data as fake 'sections' at the same
        level.
        """
        blocks = []
        # Temporary store for series of plain text and inline tag strings that aren't
        # parented by a block element. Each series will be added as a new <p> tag
        inline_elements = []

        # Temporary store for series of block-level html elements strings that should
        # make up the contents of a 'richtext' value. `inline_elements` may be combined
        # into a <p> and added to this list.
        richtext_segments = []

        richtext_block_names = RICHTEXT_BLOCK_ELEMENT_NAMES
        if allow_h2_in_richtext:
            richtext_block_names = richtext_block_names + ("h2",)

        soup = self.get_soup(value)

        # Strip and log removal of <style>, <script> and <link> tags
        for tag_name in ("style", "script", "link"):
            for tag in soup.find_all(tag_name):
                self.messages.append(f"<{tag_name}> tag removed from content: {tag}")
                tag.extract()

        for elem in soup.contents:
            if (
                isinstance(elem, NavigableString)
                or getattr(elem, "name", "") in RICHTEXT_INLINE_ELEMENT_NAMES
            ):
                # Add this item to the current series
                inline_elements.append(str(elem))
            elif elem.name == "br":
                prev = elem.previous_sibling
                if (
                    isinstance(prev, NavigableString)
                    or getattr(prev, "name", "") in RICHTEXT_INLINE_ELEMENT_NAMES
                ):
                    inline_elements.append("<br>")
                else:
                    self._add_paragraph_from_inline_elements(
                        inline_elements, to=richtext_segments
                    )
                    richtext_segments.append("<br>")

            elif buttons := elem.find_all("a", class_="btn"):

                if cta_row_blocks_supported:
                    # Close the current series of inline elements
                    self._add_paragraph_from_inline_elements(
                        inline_elements, to=richtext_segments
                    )

                    # Close the current richtext block
                    self._add_richtext_block(richtext_segments, to=blocks)

                    # Add button(s) as `CTARowBlock` value
                    blocks.append(
                        {
                            "type": "cta_row",
                            "value": {
                                "items": [
                                    self.cta_block_from_button_tag(b) for b in buttons
                                ],
                                "alignment": CTARowAlignmentChoices.LEFT,
                            },
                        }
                    )
                else:
                    self.messages.append(f"CTA buttons dropped from content: {buttons}")

            elif elem.name in richtext_block_names:
                # Close the current series of inline elements
                self._add_paragraph_from_inline_elements(
                    inline_elements, to=richtext_segments
                )
                # Add this element to the current richtext block
                richtext_segments.append(str(elem))
            elif elem.name == "h2" and not heading_blocks_supported:
                # Close the current series of inline elements
                self._add_paragraph_from_inline_elements(
                    inline_elements, to=richtext_segments
                )

                # Add h2 as a h3 to avoid losing the content
                elem.name = "h3"
                richtext_segments.append(str(elem))
            else:
                # Close the current series of inline elements
                self._add_paragraph_from_inline_elements(
                    inline_elements, to=richtext_segments
                )
                # Close the current richtext block
                self._add_richtext_block(richtext_segments, to=blocks)

                # Add h2s as `HeadingBlock` values if supported
                if elem.name == "h2":
                    text = elem.get_text().strip()
                    if text and heading_blocks_supported:
                        blocks.append(
                            {
                                "type": "heading",
                                "value": {
                                    "text": text,
                                    "show_in_inpage_nav": True,
                                },
                            }
                        )

                # Add blockquotes as `QuoteBlock` values
                elif elem.name == "blockquote" and "instagram-media" not in elem.get(
                    "class", ""
                ):
                    text = "".join(str(e) for e in elem.contents)
                    attribution = ""
                    for separator in ("<br />", "&ndash;"):
                        if separator in text:
                            text_split = text.split(separator)
                            attribution = "<p>" + text_split.pop().strip() + "</p>"
                            text = separator.join(text_split) + "</p>"
                            break
                    blocks.append(
                        {
                            "type": "quote",
                            "value": {
                                "quote": self.parse_richtext(text),
                                "attribution": attribution,
                            },
                        }
                    )

                # Add table HTML as `TableBlock` value
                elif elem.name == "table" or elem.find("table"):

                    if len(elem.find_all("td")) == 1:
                        # Extract content from single-column tables
                        content = self.parse_richtext(str(elem))
                        richtext_segments.append(content)
                        self.messages.append(
                            "Single-column <table> dropped from content. The following "
                            f"content was extracted: {content}"
                        )

                    elif table_html_blocks_supported:
                        blocks.append(
                            {
                                "type": "table_html",
                                # Use a block instance to sanitize the value
                                "value": TableBlock().value_from_form(str(elem)),
                            }
                        )
                    else:
                        self.messages.append(f"<table> dropped from content: {elem}")

                # Add iframe HTML a EmbedHTMLBlock values
                elif elem.name == "iframe" or elem.find(["iframe"]):

                    if embed_html_blocks_supported:
                        blocks.append(
                            {
                                "type": "embed_html",
                                # Use block instance to sanitize the value
                                "value": EmbedHTMLBlock().value_from_form(str(elem)),
                            }
                        )
                    else:
                        self.messages.append(f"<iframe> dropped from content: {elem}")

                elif elem.name == "div":
                    # Close the current series of inline elements
                    self._add_paragraph_from_inline_elements(
                        inline_elements, to=richtext_segments
                    )
                    if not elem.find("p"):
                        elem.name = "p"
                    # Add this element to the current richtext block
                    richtext_segments.append(str(elem))

                else:
                    self.messages.append(f"<{elem.name}> removed from content: {elem}")

        # In case there are remaining 'inline_elements'
        self._add_paragraph_from_inline_elements(inline_elements, to=richtext_segments)
        # In case there are remaining 'richtext_segments'
        self._add_richtext_block(richtext_segments, to=blocks)
        return blocks

    def _add_paragraph_from_inline_elements(
        self, inline_elements: Sequence[str], to: List[str]
    ):
        if not inline_elements:
            return
        to.append("<p>" + " ".join(inline_elements) + "</p>")
        inline_elements.clear()

    def _add_richtext_block(self, segments: Sequence[str], to: List[Dict[str, Any]]):
        if not segments:
            return
        to.append(
            {"type": "rich_text", "value": self.parse_richtext("".join(segments))}
        )
        segments.clear()

    def cta_block_from_button_tag(self, button: Tag):
        if span := button.find("span"):
            label = span.get_text()
        else:
            label = button.get_text()

        url = button["href"]

        # derive 'style' value from classnames
        style = ""
        for c in button.get_attribute_list("class"):
            if "btn--type" in c:
                style = c.replace("btn--type__", "")

        # derive 'icon' value from icon classnames
        icon = ""
        icon_element = button.find("i")
        if icon_element:
            for c in icon_element.get_attribute_list("class"):
                if "icon--" in c:
                    icon = c.replace("icon--", "")

        return self.make_cta_block(label, url, style, icon)

    def make_cta_block(self, label: str, url: str, style: str = "", icon: str = ""):
        document = None
        page = None
        parsed_url = urlparse(url)

        if self.document_finder.looks_like_document_url(parsed_url):
            try:
                document = self.find_document(url).pk
            except ObjectDoesNotExist:
                pass

        elif self.page_finder.looks_like_page_url(parsed_url):
            try:
                page = self.find_page(url).pk
            except ObjectDoesNotExist:
                pass

        if document:
            return {
                "type": "document",
                "value": {
                    "document": document,
                    "label": label,
                    "style": style,
                },
            }

        if page:
            fragment = ""
            if "#" in url:
                fragment = url.split("#").pop()
            return {
                "type": "page",
                "value": {
                    "page": page,
                    "fragment": fragment,
                    "label": label,
                    "style": style,
                    "icon": icon,
                },
            }

        return {
            "type": "url",
            "value": {
                "url": url,
                "label": label,
                "style": style,
                "icon": icon,
            },
        }

    # -------------------------------------------------------------------------
    # Image blocks
    # -------------------------------------------------------------------------
    def image_block_from_api_image_data(self, data):
        block_id = self.generate_id()
        try:
            image_id = self.find_or_download_image_for_block(
                data["uri"],
                "ImageBlock",
                block_id,
                alt=data.get("alt", ""),
                caption=data.get("caption", ""),
            ).pk
            legacy_id = None
        except (ObjectDoesNotExist, ValidationError):
            image_id = self.fallback_image.pk
            legacy_id = data["uri"]
        return {
            "type": "image",
            "value": {
                "image": image_id,
                "legacy_id": legacy_id,
                "caption": self.parse_richtext(data.get("caption")),
                "alt": "",
            },
            "id": block_id,
        }

    def image_block_from_api_artwork_data(self, data):
        block_id = self.generate_id()
        try:
            image_id = self.find_collection_page_for_block(
                data["id"], "ImageBlock", block_id
            ).master_image_id
            legacy_id = None
        except ObjectDoesNotExist:
            image_id = self.fallback_image.pk
            legacy_id = data["id"]
        return {
            "type": "image",
            "value": {
                "image": image_id,
                "legacy_id": legacy_id,
                "caption": "",
                "alt": "",
            },
        }

    def get_image_id_from_thumbnail(self, data, block_type: str, block_id: uuid.UUID):
        if not data:
            return None
        if data.get("type", "") == "artwork":
            try:
                return self.find_artwork_for_block(
                    data["id"], block_type, block_id
                ).master_image_id
            except ObjectDoesNotExist:
                pass
        else:
            try:
                return self.find_or_download_image_for_block(
                    data["uri"],
                    block_type,
                    block_id,
                    alt=data.get("alt", ""),
                    caption=data.get("caption", ""),
                ).pk
            except (ObjectDoesNotExist, ValidationError):
                pass

        return self.fallback_image.pk

    # -------------------------------------------------------------------------
    # Card blocks
    # -------------------------------------------------------------------------

    def get_card_blocks_from_api_data(self, data):
        blocks = []
        for item in data:
            block = self.card_block_from_api_data(item)
            if block is not None:
                blocks.append(block)
        return blocks

    def card_block_from_api_data(self, data):
        block_id = self.generate_id()
        block_type = API_EMBED_TYPE_TO_CARD_TYPE.get(data["type"])

        if block_type == "page":
            return self.make_page_card_block(data, block_id)

        if block_type == "event":
            return self.make_event_card_block(data, block_id)

        if block_type == "artist":
            return self.make_artist_card_block(data, block_id)

        if block_type == "external":
            return self.make_external_card_block(data, block_id)

        if block_type == "shop_product":
            return self.make_shop_product_card_block(data, block_id)

        if block_type == "promo":
            return self.make_promo_card_block(data, block_id)

        if block_type == "artwork":
            return self.make_artwork_card_block(data, block_id)

        raise ValidationError(
            f"Card type '{block_type}' not recognised. Cannot convert:\n\n {dump(data)}"
        )

    def make_page_card_block(self, data, block_id):
        legacy_id = data.get("id") or data.get("noderef", {}).get("nid")
        if legacy_id is None:
            return None
        try:
            page = self.find_page_for_block(legacy_id, "PageCardBlock", block_id).pk
            legacy_id = ""
        except ObjectDoesNotExist:
            page = None

        return {
            "type": "page",
            "value": {
                "page": page,
                "legacy_id": legacy_id,
                "image": self.get_image_id_from_thumbnail(
                    data.get("thumbnail"), "PageCardBlock", block_id
                ),
                "description": self.parse_richtext(data.get("desc", "")),
            },
            "id": block_id,
        }

    def make_event_card_block(self, data, block_id):
        try:
            legacy_id = data["id"]
        except KeyError:
            return None
        try:
            event_id = self.find_event_for_block(
                data["id"], "EventCardBlock", block_id
            ).pk
            legacy_id = ""
        except ObjectDoesNotExist:
            event_id = None
        return {
            "type": "event",
            "value": {"event": event_id, "legacy_id": legacy_id},
            "id": block_id,
        }

    def make_artist_card_block(self, data, block_id):
        # Assemble `ArtistCardBlock` value
        try:
            artist_id = self.find_artist_for_block(
                data["id"], "ArtistCardBlock", block_id
            ).pk
            legacy_id = ""
        except ObjectDoesNotExist:
            artist_id = None
            legacy_id = data["id"]
        return {
            "type": "artist",
            "value": {
                "artist": artist_id,
                "legacy_id": legacy_id,
                "description": "",
            },
            "id": block_id,
        }

    def make_external_card_block(self, data, block_id):
        # Assemble `ExternalCardBlock` value
        return {
            "type": "external",
            "value": {
                "title": data["title"],
                "url": data["url"],
                "image": self.get_image_id_from_thumbnail(
                    data.get("thumbnail"), "ExternalCardBlock", block_id
                ),
                "description": self.parse_richtext(data.get("desc", "")),
            },
            "id": block_id,
        }

    def make_shop_product_card_block(self, data, block_id):
        # Assemble `ShopProductCardBlock` value
        product_id = None

        if "id" in data:
            # This was a 'shop_ref' card: Use product ID as procvided
            product_id = data["id"]
        elif url := data.get("url"):
            # This was a 'shop' card: Extract product ID from URL
            if product_id_result := re.search(r"([a-z0-9]{4,10})[^-/]+$", url):
                product_id = product_id_result.group(1)
            else:
                raise ValidationError(f"Could not extract product ID from: '{url}'.")

        if product_id:
            return {
                "type": "shop_product",
                "value": {"product_id": product_id},
                "id": block_id,
            }
        return None

    def make_promo_card_block(self, data, block_id):
        # Assemble to `PromoCardBlock` value
        card_style = PromoCardStyleChoices.DEFAULT
        for classname in data.get("classes") or []:
            if "kids" in classname:
                card_style = PromoCardStyleChoices.KIDS
            elif "blue" in classname:
                card_style = PromoCardStyleChoices.COLLECTIVE_BLUE
            elif "collective" in classname:
                card_style = PromoCardStyleChoices.COLLECTIVE_GREEN

        cta_value = []
        if url := data.get("url"):
            cta_value.append(
                self.make_cta_block(
                    data.get("action_label", "Placeholder"),
                    url,
                    icon=CTAIconChoices.RIGHT_ARROW,
                )
            )

        return {
            "type": "promo",
            "value": {
                "text": self.parse_richtext(data.get("title")),
                "cta": cta_value,
                "style": card_style,
            },
            "id": block_id,
        }

    def make_artwork_card_block(self, data, block_id):
        # Assemble `ArtworkCardBlock` value
        try:
            artwork_id = self.find_collection_page_for_block(
                data["id"], "ArtworkCardBlock", block_id
            ).pk
            legacy_id = ""
        except ObjectDoesNotExist:
            artwork_id = None
            legacy_id = data["id"]
        return {
            "type": "artwork",
            "value": {"artwork": artwork_id, "legacy_id": legacy_id},
            "id": block_id,
        }

    # -------------------------------------------------------------------------
    # Figure blocks
    # -------------------------------------------------------------------------

    def get_figure_blocks_from_api_data(self, data):
        blocks = []
        for item in data:
            block = self.figure_block_from_api_data(item)
            if block is not None:
                blocks.append(block)
        return blocks

    def figure_block_from_api_data(self, data):
        block_id = self.generate_id()
        block_type = API_EMBED_TYPE_TO_FIGURE_TYPE.get(data["type"])

        if block_type == "column":
            return self.make_column_figure_block(data, block_id)

        if block_type == "quote":
            return self.make_quote_figure_block(data, block_id)

        if block_type == "legacy_embed":
            return self.make_legacy_embed_figure_block(data, block_id)

        if block_type == "image":
            return self.make_image_figure_block(data, block_id)

        raise ValidationError(
            f"Figure type '{block_type}' not recognised. Cannot convert:\n\n {dump(data)}"
        )

    def make_column_figure_block(self, data, block_id):
        # Assemble `ColumnFigureBlock` value
        return {
            "type": "column",
            "value": {
                "text": self.parse_richtext(data.get("text")),
                "image": self.get_image_id_from_thumbnail(
                    data.get("thumbnail"), "ColumnFigureBlock", block_id
                ),
            },
            "id": block_id,
        }

    def make_quote_figure_block(self, data, block_id):
        # Assemble `QuoteFigureBlock` value
        return {
            "type": "quote",
            "value": {
                "quote": self.parse_richtext(data["quote_text"]),
                "attribution": self.parse_richtext(data.get("quote_author", "")),
            },
            "id": block_id,
        }

    def make_legacy_embed_figure_block(self, data, block_id):
        # Assemble `LegacyEmbedFigureBlock` value
        if data.get("type", "") == "brightcove" or "code" not in data:
            return None

        return {
            "type": "legacy_embed",
            "value": {
                "embed_code": EmbedHTMLBlock().value_from_form(
                    str(self.get_soup(data["code"]))
                ),
                "caption": self.parse_richtext(data.get("caption")),
            },
            "id": block_id,
        }

    def make_image_figure_block(self, data, block_id):
        # Assemble `ImageFigureBlock` value
        if data["type"] == "artwork":
            try:
                image_id = self.find_artwork_for_block(
                    data["id"], "ImageFigureBlock", block_id
                ).master_image_id
            except ObjectDoesNotExist:
                image_id = self.fallback_image.pk
        else:
            try:
                image_id = self.find_or_download_image_for_block(
                    data["uri"],
                    "ImageFigureBlock",
                    block_id,
                    alt=data.get("alt", ""),
                    caption=data.get("caption", ""),
                ).pk
            except (ObjectDoesNotExist, ValidationError):
                image_id = self.fallback_image.pk
        return {
            "type": "image",
            "value": {
                "image": image_id,
                "caption": self.parse_richtext(data.get("caption", "")),
                "alt": "",
            },
            "id": block_id,
        }
