import argparse
import json
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from django.apps import apps
from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.db.models import Model
from django.utils.functional import cached_property
from wagtail.contrib.redirects.models import Redirect
from wagtail.core.fields import RichTextField, StreamField
from wagtail.core.models import Collection, Page, Site

from importo.commands.base import (
    BaseImportCommand,
    BaseQuerySetProcessingCommand,
    FindersMixin,
)
from importo.readers import BasePaginatedReaderException
from importo.wagtail.finders import DocumentFinder, ImageFinder, PageFinder
from importo.wagtail.parsers.rich_text import RichTextParser
from importo.wagtail.utils import get_unique_slug


class BasePageImportCommand(BaseImportCommand):
    parent_page_type = None
    move_existing_pages = False

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--parent-id", type=int)
        if apps.is_installed("wagtail.contrib.redirects"):
            parser.add_argument(
                "--no-redirects",
                action="store_true",
                help=(
                    "By default, permanent redirects are created for all new pages to help "
                    "preserve their SEO ranking as they are moved around the site. Use this "
                    "option to prevent creation of redirects"
                ),
            )

    def process_options(self, options: Mapping[str, Any]) -> None:
        super().process_options(options)
        self.parent_id = options.get("parent_id")
        self.create_redirects = apps.is_installed(
            "wagtail.contrib.redirects"
        ) and not options.get("no_redirects")

    def validate_object(self, obj: Page, is_new: bool) -> None:
        if is_new:
            exclude = ["depth", "path"]
        else:
            exclude = None
        obj.full_clean(exclude=exclude)

    def get_or_initialise_object(
        self, legacy_id: Union[int, str, uuid.UUID], data: Any
    ) -> Tuple[Model, bool]:
        """
        Overrides BaseImportCommand.get_or_initialise_object() to set an
        '_original_slug' attribute on each object to help `save_existing_page()`
        detect changes to slugs (which requires special treatment).
        """
        obj, is_new = super().get_or_initialise_object(legacy_id, data)
        obj._original_slug = getattr(obj, "slug", "")
        return obj, is_new

    def save_object(self, obj: Page, is_new: bool) -> None:
        """
        Overrides BaseImportCommand.save_object() to create revisions, publish,
        unpublish and move pages to the correct part of the page tree.
        """
        if legacy_path := getattr(obj, "legacy_path", None):
            try:
                path_segments = [seg for seg in legacy_path.split("/") if seg]
                obj.slug = path_segments.pop()
            except IndexError:
                pass

        if is_new:
            return self.save_new_page(obj)

        return self.save_existing_page(obj)

    def save_new_page(self, page: Page) -> Page:
        parent = self.get_parent_page(page)
        # repair parent (if damaged by previous failure)
        parent.numchild = parent.get_children().count()
        # ensure slug is unique amongst it's intended siblings
        page.slug = get_unique_slug(page, parent)

        with transaction.atomic():
            parent.add_child(instance=page)

        if getattr(page, "legacy_path", None) and self.create_redirects:
            site = parent.get_site() or Site.find_for_request(self.dummy_request)
            old_path = Redirect.normalise_path(page.legacy_path)
            try:
                redirect = Redirect.objects.get(site=site, old_path=old_path)
            except Redirect.DoesNotExist:
                redirect = Redirect(site=site, old_path=old_path, is_permanent=True)
            if not redirect.redirect_link:
                redirect.redirect_page = page
                redirect.save()
        return page

    def save_existing_page(self, page: Page) -> Page:
        reparented = False
        if self.move_existing_pages:
            parent = self.get_parent_page(page)
            if not (
                # avoid unnecessary moves
                page.path.startswith(parent.path)
                and page.depth == parent.depth + 1
            ):
                # repair parent (if damaged by previous failure)
                parent.numchild = parent.get_children().count()

                # ensure slug is unique amongst it's new siblings
                page.slug = get_unique_slug(page, parent)

                # move the page
                page.move(parent, "last-child")
                reparented = True

        if not reparented:
            # NOTE: `_original_slug` is set in get_or_initialise_object(), but
            # might not be set if the method is overridden
            if page.slug != getattr(page, "_original_slug", ""):
                # ensure uniqueness of new slugs
                page.slug = get_unique_slug(page, page.get_parent())

        with transaction.atomic():
            revision = page.save_revision(changed=False)

        if page.live:
            revision.publish()

        return page

    def get_parent_page(self, obj: Page):
        try:
            ideal_path = obj.get_ideal_parent_path()
        except AttributeError:
            return self.default_parent_page
        try:
            return self.finders["pages"].find(ideal_path)
        except Page.DoesNotExist:
            return self.default_parent_page

    @cached_property
    def default_parent_page(self):
        """
        Return a page to use as the parent for pages created by this
        import. Imports can override ``get_parent_page()`` to select a
        different parent depending on the page, but most will set
        `parent_page_type` and add pages to the same place in the tree.
        """
        if self.parent_page_type:
            qs = self.parent_page_type.objects.all()
            if self.parent_id:
                return qs.get(id=self.parent_id)
            parent = qs.first()
            if parent is not None:
                return parent
        return Site.objects.get(is_default_site=True).root_page.specific


class BaseCollectionMemberImportCommand(BaseImportCommand):
    target_collection_name = None

    @cached_property
    def target_collection(self):
        root_collection = Collection.get_first_root_node()
        if not self.target_collection_name:
            return root_collection
        try:
            return Collection.objects.get(name__iexact=self.target_collection_name)
        except Collection.DoesNotExist:
            collection = Collection(name=self.target_collection_name)
            root_collection.add_child(instance=collection)
            return collection

    def update_object(self, obj: Model, raw_data: Any, is_new: bool) -> None:
        if is_new:
            obj.collection = self.target_collection
        super().update_object(obj, raw_data, is_new)


class BaseWagtailQuerysetProcessingCommand(BaseQuerySetProcessingCommand):
    def process_row(
        self,
        row_number: int,
        data: Any,
        max_page_size: int = None,
        current_page_size: int = None,
        current_page_row_number: int = None,
    ):
        # Replace generic pages with specific ones
        if isinstance(data, Page):
            data = data.specific
            if type(data) is Page:
                self.logger.info(self.get_object_description(data))
                self.logger.info("The 'specific' page is unavailable, so skipping.")
                return None
        return super().process_row(
            self,
            row_number,
            data,
            max_page_size=max_page_size,
            current_page_size=current_page_size,
            current_page_row_number=current_page_row_number,
        )


class FixupError:
    __slots__ = [
        "object_desc",
        "field_name",
        "msg",
        "exception",
    ]

    def __init__(
        self,
        object_desc: str,
        field_name: str,
        msg: str,
        exception: Exception = None,
    ):
        self.object_desc = object_desc
        self.field_name = field_name
        self.msg = msg
        self.exception = exception

    def __repr__(self):
        lines = [
            self.object_desc,
            f"Field: {self.field_name}",
            f"Message: {self.msg}",
        ]
        if self.exception:
            lines.append(f"Exception: {type(self.exception)} | {str(self.exception)}")
        return "\n".join(lines)


class BaseInformationArchitectureFixupCommand(
    FindersMixin, BaseWagtailQuerysetProcessingCommand
):
    source_queryset = Page.objects.filter(depth__gt=1)

    def setup(self, options: Dict[str, Any]) -> None:
        super().setup(options)
        # Stores details of parents that couldn't be found for pages
        # - The key is the path of the page that couldn't be found
        # - The value is a list of ids of pages that want to be moved to below the path
        self.find_parent_errors = defaultdict(list)

        # Stores details of pages for which the slug couldn't be updated
        # - The key is the id of the page
        # - The value is a two-tuple, where the first value is a the ideal slug, and
        #   the second is a boolean, indicating whether the page has the ideal parent page
        self.slug_change_errors = {}

    @cached_property
    def root_page(self):
        return Page.objects.filter(depth=1).first()

    def handle(self, *args: Any, **options: Any) -> Optional[str]:
        super().handle(*args, **options)
        if self.find_parent_errors:
            self.logger.info(
                "================================================================================\n"
                "The following parents could not be found:"
                "\n================================================================================"
            )
            for key, page_ids in self.find_parent_errors.items():
                if page_ids:
                    page_descriptions = "\n * ".join(
                        self.get_object_description(obj)
                        for obj in Page.objects.filter(id__in=page_ids).specific(
                            defer=True
                        )
                    )
                    self.logger.warning(
                        f"Path: {key} | Required for:\n * {page_descriptions}"
                    )

        if self.slug_change_errors:
            self.logger.info(
                "================================================================================\n"
                "Slugs could not be corrected for the following pages:"
                "\n================================================================================"
            )
            pages = Page.objects.filter(id__in=self.slug_change_errors.keys()).in_bulk()
            for page_id, values in self.slug_change_errors.items():
                desc = self.get_object_description(pages[page_id].specific_deferred)
                self.logger.warning(
                    f"{desc}\nIdeal slug: '{values[0]}'\nIdeally parented? {values[1]}"
                )

    def skip_update(self, obj: Page):
        return obj.has_ideal_path(self.dummy_request)

    def get_ideal_parent_page(self, ideal_path: str, page: Page) -> Page:
        parent = self.finders["pages"].find(ideal_path)
        if len(self.finders["pages"]._other_lookup_cache) > 250:
            self.finders["pages"].clear_caches()
        return parent

    def get_possible_slug(self, ideal_slug: str, page: Page, parent_page: Page) -> str:
        # Store the original value to allow restoration
        original_value = page.slug

        # Temporarily change slug to allow get_unique_slug() to work
        page.slug = ideal_slug

        try:
            # Make any necessary adjustments to ensure the slug is unique
            return_value = get_unique_slug(page, parent_page)
        except Exception:
            # Something weird happened... abandon ship!
            return_value = original_value
            self.logger.exception("Unique slug generation failed")
        finally:
            # Ensure slug is always reset
            page.slug = original_value

        return return_value

    def update_object(self, obj: Page, new_parent: Page = None) -> None:
        # save_object() will look for these attributes to figure out
        # what to change (if anything)
        obj._new_slug = None
        obj._new_parent = None

        # Figure out where we want to be...
        ideal_slug = obj.get_ideal_slug(self.dummy_request)
        ideal_parent_path = obj.get_ideal_parent_path(self.dummy_request)

        if not obj.has_ideal_parent(self.dummy_request):
            # Update obj._new_parent if the page needs to move
            try:
                ideal_parent = new_parent or self.get_ideal_parent_page(
                    ideal_parent_path, page=obj
                )
                if ideal_parent.specific != obj.specific_parent_page:
                    self.logger.debug(f"😊 Page CAN be moved to '{ideal_parent_path}'.")
                    obj._new_parent = ideal_parent
                    try:
                        self.find_parent_errors[ideal_parent_path].remove(obj.id)
                    except ValueError:
                        pass
            except Page.DoesNotExist:
                self.logger.debug(f"😞 Page CANNOT be moved to '{ideal_parent_path}'.")
                self.find_parent_errors[ideal_parent_path].append(obj.id)

        # Update obj._new_slug if the slug can be changed to an ideal value
        original_slug = obj.slug
        new_slug = self.get_possible_slug(
            ideal_slug,
            page=obj,
            parent_page=obj._new_parent or obj.specific_parent_page or self.root_page,
        )
        if new_slug != original_slug:
            obj._new_slug = new_slug

            if new_slug == ideal_slug:
                self.logger.debug(f"😊 Page slug CAN be changed to '{ideal_slug}'.")
                try:
                    del self.slug_change_errors[obj.id]
                except KeyError:
                    pass
            else:
                self.logger.debug(f"😞 Page slug CANNOT be changed to '{ideal_slug}'.")
                self.slug_change_errors[obj.id] = (
                    ideal_slug,
                    obj._new_parent is not None,
                )

    def skip_save(self, obj):
        return bool(obj._new_parent is None and obj._new_slug is None)

    def save_object(self, obj):
        if obj._new_parent:
            target_slug = obj._new_slug or obj.slug

            # Change slug temporarily to avoid clashes in new location
            obj.slug = str(uuid.uuid4())
            with transaction.atomic():
                obj.save()

            # Move the page
            with transaction.atomic():
                obj.move(obj._new_parent, "last-child")

            # Change / restore the slug
            # NOTE: move() doesn't update the in-memory instance, so refecth obj from DB
            obj = Page.objects.get(id=obj.id).specific_deferred
            obj.slug = target_slug
            with transaction.atomic():
                obj.save()

        elif obj._new_slug:
            obj.slug = obj._new_slug
            with transaction.atomic():
                obj.save()

        # Reprocess pages unblocked by this change!
        new_path = obj.get_url(self.dummy_request).rstrip("/")

        # NOTE: Using pop() to simultaneously get and remove
        if unblocked_page_ids := self.find_parent_errors.pop(new_path, ()):
            self.logger.debug(
                f"✨ Reprocessing {len(unblocked_page_ids)} pages unblocked by this change ✨"
            )
            for unblocked_page in (
                self.get_queryset().filter(id__in=unblocked_page_ids).iterator()
            ):
                unblocked_page = unblocked_page.specific
                self.update_object(unblocked_page, new_parent=obj)
                if not self.skip_save(unblocked_page):
                    self.save_object(unblocked_page)


class BaseContentFixupCommand(FindersMixin, BaseWagtailQuerysetProcessingCommand):
    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--remove",
            action="store_true",
            help=(
                "Remove block values / objects that cannot be matched to a real entity."
            ),
        )
        parser.add_argument(
            "--remove-only",
            action="store_true",
            help=(
                "Just remove block values / objects that aren't linke to a real entity, and "
                "do not try to match them up to one."
            ),
        )
        super().add_arguments(parser)

    def process_options(self, options: Dict[str, Any]) -> None:
        super().process_options(options)
        self.remove = options.get("remove") or False
        self.remove_only = options.get("remove_only") or False

    def log_fixup_error(self, msg: str, exception: Exception = None):
        self.fixup_errors.append(
            FixupError(
                object_desc=self.get_object_description(self.current_object),
                field_name=self.current_field_name,
                msg=msg,
                exception=exception,
            )
        )

    @cached_property
    def image_finder(self):
        return self.get_or_create_finder("images", ImageFinder)

    @cached_property
    def document_finder(self):
        return self.get_or_create_finder("documents", DocumentFinder)

    @cached_property
    def page_finder(self):
        return self.get_or_create_finder("pages", PageFinder)

    def on_page_started(self, page_number: int) -> None:
        super().on_page_started(page_number)
        self.fixup_errors = []

    def update_object(self, obj):
        self.changed_fields = []
        # Update RichTextField and StreamField values
        for field in obj._meta.concrete_fields:
            if isinstance(field, RichTextField):
                if self.fixup_richtextfield_value(obj, field.name):
                    self.changed_fields.append(field.name)
            if isinstance(field, StreamField):
                if self.fixup_streamfield_value(obj, field.name):
                    self.changed_fields.append(field.name)

    def on_page_completed(
        self, page_number: int, reason: BasePaginatedReaderException = None
    ) -> None:
        super().on_page_completed(page_number, reason=reason)
        if self.fixup_errors:
            self.logger.warning(
                "--------------------------------------------------------------\n"
                "The following fixup errors occurred on this page"
                "\n--------------------------------------------------------------"
            )
            for e in self.fixup_errors:
                self.logger.warning(e)

    def skip_save(self, obj):
        if not self.changed_fields:
            self.logger.debug("No changes were made.")
            return True
        return False

    def fixup_richtextfield_value(self, obj: Model, field_name: str) -> bool:
        self.logger.debug(f"Checking '{field_name}' RichTextField value.")
        if self.remove_only:
            # Avoid match attempts and stick with the current value
            return False
        self.current_field_name = field_name
        current_value = getattr(obj, field_name)
        new_value = self.clean_richtext(current_value)
        if new_value == current_value:
            return False
        setattr(obj, field_name, new_value)
        return True

    def fixup_streamfield_value(self, page, field_name: str) -> bool:
        self.logger.debug(f"Checking '{field_name}' StreamField value.")
        self.current_field_name = field_name
        current_data = getattr(page, field_name)._raw_data
        current_value = json.dumps(current_data, cls=DjangoJSONEncoder)
        new_value = json.dumps(
            self.clean_streamblock_value(current_data), cls=DjangoJSONEncoder
        )
        if current_value == new_value:
            return False
        setattr(page, field_name, new_value)
        return True

    def clean_richtext(self, value) -> str:
        if self.remove_only or not value or "<a " not in value:
            return value
        parser = RichTextParser(command=self)
        new_value = parser.parse(value, link_replacement_only=True)
        for error in parser.link_match_errors:
            self.log_fixup_error(error.msg, error.exception)
        return new_value

    def clean_streamblock_value(
        self, blocks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        return_value = []

        for block in blocks:

            if block.get("value") and isinstance(block["value"], list):
                # If this is a value for a StreamBlock or ListBlock,
                # ensure any sub-block values are processed
                block["value"] = self.clean_list_value(block["value"])

            if block.get("value") and isinstance(block["value"], dict):
                block = self.clean_structblock(block)
                if block is None:
                    continue

            if block.get("type") == "rich_text":
                block["value"] = self.clean_richtext(block["value"])

            # Add the updated block to the return value
            # This may have been bypassed using 'continue' further up
            return_value.append(block)

        return return_value

    def clean_list_value(self, value: List[Any]) -> List[Any]:
        if not value or not isinstance(value[0], dict):
            return value
        first_item = value[0]
        if "type" in first_item and "value" in first_item:
            return self.clean_streamblock_value(value)
        return self.clean_listblock_value(value)

    def clean_listblock_value(self, value: List[Dict[str, Any]]):
        for row in value:
            for key, subblock_value in row.items():
                if subblock_value and isinstance(subblock_value, list):
                    row[key] = self.clean_list_value(subblock_value)
        return value

    def clean_structblock(self, block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for key, value in block["value"].items():
            if value and isinstance(value, list):
                block["value"][key] = self.clean_list_value(value)
        return block
