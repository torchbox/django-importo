import argparse
import json
import logging
import uuid
import warnings
from collections import defaultdict
from copy import copy
from operator import attrgetter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import urlencode

from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import (
    NON_FIELD_ERRORS,
    ImproperlyConfigured,
    ValidationError,
)
from django.core.management.base import BaseCommand as DjangoBaseCommand
from django.core.paginator import Paginator
from django.core.serializers.json import DjangoJSONEncoder
from django.db import IntegrityError, transaction
from django.db.models import Model, QuerySet
from django.forms.utils import ErrorDict, ErrorList
from django.utils import timezone
from django.utils.functional import cached_property
from tate.legacy.readers.base import BaseReader
from wagtail.contrib.redirects.models import Redirect
from wagtail.core.fields import RichTextField, StreamField
from wagtail.core.models import Collection, Page, Site

from . import constants, fields, finders, models, readers
from .errors import BasePaginatedReaderException, CommandOptionError, SkipField, SkipRow
from .parsers.richtext import RichTextParser
from .utils import get_dummy_request, get_unique_slug
from .utils.classes import LoggingShortcutsMixin
from .utils.datetime import humanize_timedelta
from .utils.values import extract_row_value


class RowError:
    __slots__ = ["page", "row", "legacy_id", "msg"]

    def __init__(
        self, page: int, row: int, legacy_id: Any, msg: str, exception: Exception = None
    ):
        self.page = page
        self.row = row
        self.legacy_id = legacy_id
        if exception is not None:
            self.msg = f"{msg}: {exception}"
        else:
            self.msg = msg

    def __repr__(self):
        return f"Page: {self.page} | Row: {self.row} | Source ID: {self.legacy_id}\n\n{self.msg}"


class FindersMixin:
    finder_classes = {
        "users": finders.UserFinder,
        "images": finders.ImageFinder,
        "documents": finders.DocumentFinder,
        "pages": finders.PageFinder,
        "sites": finders.SiteFinder,
    }

    @cached_property
    def finders(self) -> Sequence[finders.BaseFinder]:
        finder_instances = {}
        for key, value in self.finder_classes.items():
            if isinstance(value, finders.BaseFinder):
                finder_instances[key] = value
            else:
                finder_instances[key] = value(command=self)
        return finder_instances

    def get_or_create_finder(self, key: str, finder_class: type):
        try:
            return self.finders[key]
        except KeyError:
            warnings.warn(
                f"The command {self.__class__.__module__}{self.__name__} does "
                f"not have finder matching the key '{key}', so the is adding "
                f"it's own {finder_class} instance."
            )
            finder = finder_class()
            # Allow the finder (and its caches) to be shared
            self.finders[key] = finder
            return finder


class BaseCommand(DjangoBaseCommand):
    def execute(self, *args, **options) -> None:
        self.process_options(options)
        self.setup(options)
        self.on_command_started(options)
        super().execute(*args, **options)
        self.on_command_completed(options)

    def process_options(self, options: Dict[str, Any]) -> None:
        """
        Interpret/validate and store option values for reference by other methods.
        If any modification are made to the ``options`` dict, those changes will
        be carried through to ``setup()`` and ``handle()``
        """
        pass

    def setup(self, options: Dict[str, Any]) -> None:
        """
        Hook to allow any 'initial setup' to be made before
        on_command_started() is called.
        """
        pass

    def on_command_started(self, options: Dict[str, Any]) -> None:
        """
        A hook that is called immediately BEFORE the handle() method,
        and after setup().
        """
        self.command_started_at = timezone.now()

    def on_command_completed(self, options: Dict[str, Any]) -> None:
        """
        A hook that is called immediately AFTER the handle() method.
        """
        pass


class LoggingCommand(LoggingShortcutsMixin, BaseCommand):
    def process_options(self, options: Dict[str, Any]) -> None:
        super().process_options(options)
        self.verbosity = options["verbosity"]

    def setup(self, options: Dict[str, Any]) -> None:
        super().setup(options)
        # Used by get_object_description() to generate page URLs more efficiently
        self.dummy_request = get_dummy_request()

    @cached_property
    def logger(self):
        # Log formatting (set here to keep the base settings tidy)
        logger = logging.getLogger(self.__class__.__module__)
        logger.propagate = False
        verbosity = getattr(self, "verbosity", 2)
        level = constants.VERBOSITY_TO_LOGGING_LEVEL.get(verbosity) or logging.INFO
        logger.setLevel(level)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s\n"))
        logger.handlers = [handler]
        return logger

    def get_object_description(self, obj):
        if isinstance(obj, models.LegacyPageMixin):
            return (
                f"<{type(obj).__name__} id='{obj.id}' legacy_id='{getattr(obj, obj.LEGACY_ID_FIELD, '')}' "
                f"url='{obj.get_url(self.dummy_request)}' legacy_path='{obj.legacy_path or ''}'>"
            )
        if isinstance(obj, Page):
            return f"<{type(obj).__name__} id='{obj.id}' url='{obj.get_url(self.dummy_request)}'>"
        return f"<{type(obj).__name__} id={obj.id}>"


class ReadingCommand(LoggingCommand):
    reader_class = readers.SimpleCSVReader

    default_source_db = None
    allow_source_db_override = True

    default_sql_query = None
    allow_sql_query_override = True

    source_model = None
    source_queryset = None

    # If not set on the command class, the command will look for an attribute
    # of the same name on the reader class
    default_page_size = None
    allow_page_size_override = True

    # Determines the contents of resume cache keys
    data_impacting_options: Sequence[str] = ["source_db", "query", "row"]

    resilient = False

    @classmethod
    def resume_enabled(cls) -> bool:
        return not cls.reader_class.requires_source_file

    def add_arguments(self, parser: argparse.ArgumentParser):
        # Allow parent classes to modify arguments
        super().add_arguments(parser)

        if self.reader_class.requires_source_file:
            parser.add_argument(
                "file",
                type=argparse.FileType("r"),
                help=self.reader_class.source_file_help,
            )

        if self.reader_class.requires_source_db:
            common_kwargs = {
                "choices": list(settings.DATABASES.keys()),
                "help": self.reader_class.source_db_help,
            }
            if self.default_source_db is None:
                # The source db must be specified by the caller
                parser.add_argument("source-db", **common_kwargs)
            elif self.allow_source_db_override:
                # The source db can be overridden by the caller
                parser.add_argument("--source-db", **common_kwargs)

        if self.reader_class.requires_sql_query:
            if not self.default_sql_query:
                # The SQL query must be specified by the caller
                parser.add_argument("query", help=self.reader_class.sql_query_help)
            elif self.allow_sql_query_override:
                # The SQL query can be overridden by the caller
                parser.add_argument(
                    "-q", "--query", help=self.reader_class.sql_query_help
                )

        if self.reader_class.supports_pagination:
            parser.add_argument(
                "-p",
                "--page",
                type=int,
                help=(
                    "Optionally specify a single page of source data to operate "
                    "on. Useful during development to test the process "
                    "on a smaller dataset. When provided, overrides both "
                    "the 'start-page' and 'stop-page' option values."
                ),
            )
            parser.add_argument(
                "--start-page",
                type=int,
                help=(
                    "Optionally specify a page of source data to start operating on, "
                    "skipping over any preceeding pages."
                ),
            )
            parser.add_argument(
                "--stop-page",
                type=int,
                help=(
                    "Optionally specify the last page of source data to operate on. "
                    "The import will stop once this page has been processed."
                ),
            )
            if not self.default_page_size and not self.reader_class.default_page_size:
                # The page_size must be specified by the caller
                parser.add_argument(
                    "page-size",
                    help=(
                        "The number of rows that should be fetched from the data "
                        "source for each page. NOTE: Larger pages use a larger "
                        "amount of memory."
                    ),
                )
            elif self.allow_page_size_override:
                default = self.default_page_size or self.reader_class.default_page_size
                parser.add_argument(
                    "--page-size",
                    type=int,
                    help=(
                        "Optionally override the number of rows that should be "
                        "fetched from the data source for each page. A value "
                        f"of {default} will be used by default. NOTE: Larger "
                        "pages use a larger amount of memory."
                    ),
                )

        row_option_help_extra = ""
        if self.reader_class.supports_pagination:
            row_option_help_extra = (
                " NOTE: The value is automatically adjusted for pagination. "
                "For example, if the page size is 100, and you provide a value "
                "of 250, that will automatically be interpreted as 'row 50 of "
                "the third page'."
            )

        parser.add_argument(
            "-r",
            "--row",
            type=int,
            help=(
                "Optionally specify a single row number to process. Useful "
                "for re-running the process for a single item. Overrides both "
                "'start-row' and 'stop-row' option values when provided."
                + row_option_help_extra
            ),
        )

        parser.add_argument(
            "-s",
            "--start-row",
            type=int,
            help=(
                "Optionally specify the row number to start processing on, "
                "skipping over any preceeding rows." + row_option_help_extra
            ),
        )

        parser.add_argument(
            "--stop-row",
            type=int,
            help=(
                "Optionally specify the row number to stop processing on, "
                "skipping over any succeeding rows." + row_option_help_extra
            ),
        )

        parser.add_argument(
            "--resilient",
            action="store_true",
            help=("Continue processing errors to hault processing."),
        )

        if self.resume_enabled():
            parser.add_argument(
                "--resume",
                action="store_true",
                help=(
                    "Start the import from where it got to the last time it "
                    "was run (with the same result-impacting options)."
                ),
            )

    def process_options(self, options: Dict[str, Any]) -> None:
        self.resilient_mode = options["resilient"]
        self.resume_requested = options.get("resume", False)
        self.resume_key = None
        if self.resume_enabled():
            self.resume_key = self.get_resume_key(options)
            if self.resume_requested:
                start_row = self.get_resume_progress()
                if start_row is None:
                    raise CommandOptionError(
                        "Resume progress could not be found. Try running again "
                        "without the '--resume' option to start the import "
                        "from scratch."
                    )
                self.log_info(f"Resuming from row: {start_row}")
                options.update(start_row=start_row, start_page=None)

        # Allow parent classes to process options
        super().process_options(options)

    @classmethod
    def get_resume_key_values(cls, options) -> Dict[str, Any]:
        values = {"command": str(cls)}
        for key in cls.data_impacting_options:
            option_value = options.get(key)
            if option_value is not None:
                values[key] = option_value
        return values

    @classmethod
    def get_resume_key(cls, options) -> Optional[str]:
        values = cls.get_resume_key_values(options)
        return urlencode(dict(sorted(values.items())))

    def setup(self, options: Dict[str, Any]):
        super().setup(options)
        self.reader = self.get_reader(options)

    def get_reader(self, options: Mapping[str, Any]) -> BaseReader:
        reader_kwargs = self.get_reader_kwargs(options)
        return self.reader_class(**reader_kwargs)

    def get_reader_kwargs(self, options: Mapping[str, Any]) -> Mapping[str, Any]:
        kwargs = {}
        if self.reader_class.requires_source_file:
            kwargs["file"] = options.get("file")
        if self.reader_class.requires_source_db:
            kwargs["source_db"] = options.get("source_db") or self.default_source_db
        if self.reader_class.requires_sql_query:
            kwargs["query"] = options.get("query") or self.default_sql_query
        if self.reader_class.requires_queryset:
            kwargs["queryset"] = self.get_source_queryset(options)

        # Validate row, start-row and stop-row options
        specific_row = options.get("row")
        start_row = specific_row or options.get("start_row")
        stop_row = specific_row or options.get("stop_row")
        kwargs.update(
            start_row=start_row,
            stop_row=stop_row,
        )
        if start_row and stop_row and stop_row < start_row:
            raise CommandOptionError("'stop-row' cannot be less than 'start-row'.")

        # Validate page, start-page and stop-page options
        if self.reader_class.supports_pagination:
            specific_page = options.get("page")
            start_page = specific_page or options.get("start_page")
            stop_page = specific_page or options.get("stop_page")
            if start_row is not None or stop_row is not None:
                if specific_page is not None:
                    raise CommandOptionError(
                        "'page' cannot be used in conjunction with row, start-row or stop-row options."
                    )
                if start_page is not None:
                    raise CommandOptionError(
                        "'start_page' cannot be used in conjunction with row, start-row or stop-row options."
                    )
                if stop_page is not None:
                    raise CommandOptionError(
                        "'stop_page' cannot be used in conjunction with row, start-row or stop-row options."
                    )
            if stop_page and start_page and stop_page < start_page:
                raise CommandOptionError(
                    "'stop-page' cannot be less than 'start-page'."
                )
            kwargs.update(
                start_page=start_page,
                stop_page=stop_page,
                page_size=options.get("page_size") or self.default_page_size,
            )

        # Bind reader to command
        kwargs["command"] = self
        return kwargs

    def get_source_queryset(self, options) -> Optional[QuerySet]:
        """
        If the reader class for the command requires a queryset as a
        data source, this method is used to generate one.
        """
        model = self.source_model
        queryset = self.source_queryset
        if queryset is None and model is None:
            raise ImproperlyConfigured(
                f"{self.reader_class} expects the command to provide 'queryset' a data source. Please "
                "set the 'source_model' or 'source_querset' attributes on your command to allow this."
            )
        if queryset:
            return queryset.all()
        return model.objects.all()

    def handle(self, *args, **options) -> None:
        self.max_page_size = getattr(self.reader, "page_size", None)
        for row in self.reader:

            # These values are unlikley to change between rows, but readers
            # might not know them until results have been requested
            self.total_rows = getattr(self.reader, "total_rows", None)
            self.total_pages = getattr(self.reader, "total_pages", None)

            self._handle_row(
                self.reader.current_row_number,
                data=row,
                page_number=getattr(self.reader, "current_page_number", None),
                page_size=getattr(self.reader, "current_page_size", None),
                page_specific_row_number=getattr(
                    self.reader, "current_page_row_number", None
                ),
            )

    def _handle_row(
        self,
        row_number: int,
        data: Any,
        page_number: int = None,
        page_size: int = None,
        page_specific_row_number: int = None,
    ) -> None:
        self.row_number = row_number
        self.row_data = data
        self.page_number = page_number
        self.page_specific_row_number = page_specific_row_number

        self.on_row_started(
            row_number,
            data,
            page_number,
            page_size,
            page_specific_row_number,
        )

        error = None
        try:
            self.process_row(
                row_number,
                data,
                page_number,
                page_size,
                page_specific_row_number,
            )
        except Exception as e:
            error = e
            if self.resilient_mode:
                self.log_error("Error occured while processing row", exc_info=e)
            else:
                raise
        finally:
            self.on_row_completed(
                row_number,
                data,
                error is None,
                page_number,
                page_size,
                page_specific_row_number,
            )

    def on_row_started(
        self,
        row_number: int,
        data: Any,
        page_number: int = None,
        page_size: int = None,
        page_specific_row_number: int = None,
    ) -> None:
        """
        A hook that is called immediately before a new row is processed.

        NOTE: `page_number`, `page_size` and `page_specific_row_number`
        will only be supplied by readers that support pagination.
        """
        self.render_row_header(
            row_number, data, page_number, page_size, page_specific_row_number
        )
        self.set_resume_progress(row_number)

    def render_row_header(
        self,
        row_number: int,
        data: Any,
        page_number: int = None,
        page_size: int = None,
        page_specific_row_number: int = None,
    ):
        msg_parts = []

        msg = f"Row: {row_number}"
        if self.total_rows is not None:
            msg += f" of {self.total_rows}"

        msg_parts.append(msg)

        if page_number is not None:
            if self.total_pages is not None:
                msg_parts.append(f"Page: {page_number} of {self.total_pages}")
            else:
                msg_parts.append(f"Page: {page_number}")

        if page_specific_row_number is not None:
            msg_parts.append(f"Page row: {page_specific_row_number} of {page_size}")

        self.log_info("  |  ".join(msg_parts), overline="=", underline="=")

    def process_row(
        self,
        row_number: int,
        data: Any,
        page_number: int = None,
        page_size: int = None,
        page_specific_row_number: int = None,
    ) -> None:
        """
        A hook that is responsible for processing row data. Override this
        method to customisze how row data is processed.

        NOTE: `page_number`, `page_size` and `page_specific_row_number`
        will only be supplied by readers that support pagination.
        """
        pass

    def on_row_completed(
        self,
        row_number: int,
        data: Any,
        successful: bool,
        page_number: int = None,
        page_size: int = None,
        page_specific_row_number: int = None,
    ) -> None:
        """
        A hook that is called immediately after a row has been processed, even
        if the process errored for some reason (in which case, `was_successful`
        will be `False`).

        NOTE: `page_number`, `page_size` and `page_specific_row_number`
        will only be supplied by readers that support pagination.
        """
        self.render_row_footer(
            row_number,
            data,
            successful,
            page_number,
            page_size,
            page_specific_row_number,
        )

    def render_row_footer(
        self,
        row_number: int,
        data: Any,
        successful: bool,
        page_number: int = None,
        page_size: int = None,
        page_specific_row_number: int = None,
    ) -> None:
        msg_lines = []

        start_row = self.reader.start_row or 1
        stop_row = self.reader.stop_row or self.total_rows
        if stop_row is not None:
            rows_to_process = stop_row - (start_row - 1)
            row_number_in_series = row_number - (start_row - 1)
            progress = "{:.1f}".format((100 / rows_to_process) * row_number_in_series)
            msg_lines.append(f"🌱 Progress: {progress}%")

            elapsed_time = timezone.now() - self.command_started_at
            per_row_avg = elapsed_time / row_number_in_series
            remaining_rows = rows_to_process - row_number_in_series
            time_remaining = humanize_timedelta(per_row_avg * remaining_rows)
            msg_lines.append(f"🔮 Time remaining: {time_remaining}")

        if msg_lines:
            self.log_info("---")
            self.log_info("\n".join(msg_lines))

    def on_page_started(self, page_number: int) -> None:
        """
        A hook that is called immediately before processing a page
        of row results from the data source.

        NOTE: This will only be called if the reader used by a
        command support pagaination.
        """
        pass

    def on_page_completed(
        self, page_number: int, reason: BasePaginatedReaderException = None
    ) -> None:
        """
        A hook that is called immediately before processing a page
        of row results from the data source.

        NOTE: This will only be called if the reader used by a
        command support pagaination.
        """
        if reason is not None:
            self.log_debug(reason.message, overline="~", underline="~")

    def on_command_completed(self, options: Dict[str, Any]) -> None:
        self.log_info("🎉 That's all folks! 🎉")
        self.clear_resume_progress()
        super().on_command_completed(options)

    def get_resume_progress(self) -> Optional[int]:
        if self.resume_key:
            return cache.get(self.resume_key)

    def set_resume_progress(self, row_number: int) -> None:
        if self.resume_key:
            return cache.set(self.resume_key, row_number, timeout=86400)

    def clear_resume_progress(self):
        if self.resume_key:
            return cache.delete(self.resume_key)


class DeclarativeFieldsMetaclass(type):
    """Collect Fields declared on the base classes."""

    def __new__(mcs, name, bases, attrs):
        # Collect fields from current class and remove them from attrs.
        attrs["declared_fields"] = {
            key: attrs.pop(key)
            for key, value in list(attrs.items())
            if isinstance(value, fields.Field)
        }

        new_class = super().__new__(mcs, name, bases, attrs)

        # Walk through the MRO.
        declared_fields = {}
        for base in reversed(new_class.__mro__):
            # Collect fields from base class.
            if hasattr(base, "declared_fields"):
                declared_fields.update(base.declared_fields)

            # Field shadowing.
            for attr, value in base.__dict__.items():
                if value is None and attr in declared_fields:
                    declared_fields.pop(attr)

        new_class.base_fields = declared_fields
        new_class.declared_fields = declared_fields

        return new_class


class BaseImportCommand(
    FindersMixin, ReadingCommand, metaclass=DeclarativeFieldsMetaclass
):
    target_model = None

    # TODO: Figure out a way to do this with fields
    source_id_field = ""
    source_ids_to_ignore = ()
    target_model_id_field = ""

    def add_arguments(self, parser: argparse.ArgumentParser):
        # Allow parent classes to add their arguments first
        super().add_arguments(parser)

        parser.add_argument(
            "-d",
            "--dryrun",
            action="store_true",
            help=("Run the import without saving any changes to the database."),
        )

        parser.add_argument(
            "-u",
            "--force-update",
            action="store_true",
            help=(
                "Process all rows, even if it looks like the existing object "
                "does not need updating."
            ),
        )

        parser.add_argument(
            "-n",
            "--no-update",
            action="store_true",
            help="Process new rows only (do not update existing objects).",
        )

        parser.add_argument(
            "-m",
            "--mock-downloads",
            action="store_true",
            help="Use placeholders for file downloads instead of downloading from source (used for testing)",
        )

    def process_options(self, options):
        self.dryrun = options.get("dryrun", False)
        self.no_update = options.get("no_update", False)
        self.force_update = options.get("force_update", False)
        self.mock_downloads = options.get("mock_downloads", False)
        # Allow parent classes to process options
        super().process_options(options)

    def setup(self, options: Dict[str, Any]) -> None:
        # Allow parent classes to do setup first
        super().setup(options)
        self.errors = []

    @cached_property
    def fields(self):
        # Order fields by 'clean cost' so that 'cheap' field errors prevent unncessary
        # cleaning of more 'expensive' fields
        fields = {
            k: v
            for k, v in sorted(
                self.declared_fields.items(), key=lambda x: x[1].clean_cost
            )
        }
        for name, field in fields.items():
            # set name on each field and bind to this command instance
            field.name = name
            field.bind_to_command(self)
        return fields

    def log_error(self, msg: str, *args, exc_info=None, **kwargs):
        super().log_error(msg, *args, exc_info=exc_info, **kwargs)
        self.errors.append(
            RowError(
                page=self.page_number,
                row=self.row_number,
                legacy_id=self.row_source_id,
                msg=msg,
                exception=exc_info,
            )
        )

    def get_target_model(self, data: Any) -> type:
        """
        Return the model class that the command should turn `data` into an instance of.
        """
        return self.target_model

    def get_source_id(self, data: Any):
        """
        Return a ID value from the supplied row data, that can be used to
        look for an existing model instance to update.
        """
        value = extract_row_value(self.source_id_field, data)
        if self.source_id_field in self.fields:
            return self.fields[self.source_id_field].clean(value)
        return value

    def get_queryset(self) -> QuerySet:
        """
        Return a QuerySet of instances of the target model. The value is used
        by 'source_ids_from_db' (below).
        """
        return self.target_model.objects.all()

    @cached_property
    def source_ids_from_db(self):
        """
        Return a set of 'cross-system ID' values that already exist in the
        database. This value is used by get_or_initialise_object() to check
        whether an encountered row already has a local representation,
        without needing to make a database query.
        """
        return set(
            self.get_queryset().values_list(self.target_model_id_field, flat=True)
        )

    def process_row(
        self,
        row_number: int,
        data: Any,
        max_page_size: int = None,
        current_page_size: int = None,
        current_page_row_number: int = None,
    ):
        """
        This method is responsible for deciding what to do with the data for a
        given row. It's unlikely that you'll need to override this method.
        """
        # Set for methods, fields and other objects to reference
        self.row_source_id = source_id = self.get_source_id(data)
        self.row_data = data
        self.row_errors = ErrorDict()
        self.current_object = None
        self.cleaned_data = {}

        self.log_info(f"Source ID: {source_id}")

        if source_id in self.source_ids_to_ignore:
            self.log_info("Skipping update.")
            return None

        if self.no_update and source_id in self.source_ids_from_db:
            self.log_info("Skipping update.")
            return None

        obj, is_new = self.get_or_initialise_object(source_id, data)
        self.current_object = obj

        skip_update = self.skip_update(obj, data, is_new)
        if not skip_update:
            try:
                self.clean(data, is_new)
            except SkipRow:
                skip_update = True

        if skip_update:
            self.log_info("Skipping update.")
            return None

        if is_new:
            self.log_info(f"Creating new {type(obj).__name__}")
        else:
            self.log_info(f"Updating existing {type(obj).__name__} (PK: {obj.pk})")

        self.update_object(obj, data, is_new)
        if self.is_valid(obj, is_new):
            self.conditionally_save_object(obj, is_new)

    def get_or_initialise_object(self, legacy_id: Any, data: Any) -> Tuple[Model, bool]:
        """
        Returns an instance of `self.target_model` with an ID matching that
        found in the source data. If a match cannot be found in the database,
        a new/unsaved instance will be returned, with the relevant ID field
        value set.
        """
        target_model = self.get_target_model(data)
        if legacy_id in self.source_ids_from_db:
            try:
                obj = self.get_object(legacy_id, target_model)
                return obj, False
            except target_model.DoesNotExist:
                pass
        return self.initialise_object(legacy_id, data, target_model), True

    def get_object(self, legacy_id: Any, target_model: type) -> Model:
        """
        Returns an instance of ``self.target_model`` matching the provided
        ``legacy_id``. Should raise `self.target_model.DoesNotExist` if no
        matching instance can be found.
        """
        lookups = {self.target_model_id_field: legacy_id}
        return target_model.objects.all().get(**lookups)

    def get_init_kwargs(self, legacy_id, data: Any, target_model: type) -> dict:
        """
        Returns a ``dict`` of values for initialise_object()
        to use when initialising a new object.
        """
        return {self.target_model_id_field: legacy_id}

    def initialise_object(self, legacy_id, data: Any, target_model: type) -> Model:
        """
        Returns a new, unsaved instance of ``self.target_model``
        ready to be updated by update_object().
        """
        return target_model(**self.get_init_kwargs(legacy_id, data, target_model))

    def skip_update(self, obj: Model, row_data: Any, is_new: bool) -> bool:
        """
        Override to skip updating or saving of changes to the supplied
        ``obj``. For example, if it doesn't look like things have changed
        since the last update.
        """
        if self.force_update or is_new:
            return False

        return not self.is_stale(obj, row_data)

    def is_stale(obj: Model, row_data: Any) -> bool:
        """
        Return a boolean indicating whether `obj` (a pre-existing model instance)
        needs updating to reflect changes in `row_data`. By default, pre-existing
        objects are always updated, but if rows include a 'last updated' timestamp
        or other indicator, you might want to override this method to take that
        into account.
        """
        return True

    def clean(self, row_data: Any, is_new: bool):
        """
        Use the command's fields to populate ``self.cleaned_data`` from
        ``row_data`` (the raw data as returned by the reader).
        """
        for name, field in self.fields.items():
            try:
                field.contribute_to_cleaned_data(self.cleaned_data, row_data)
            except SkipField:
                continue
            except ValidationError as e:
                self.add_row_error(name, e)

        if self.row_errors:
            if self.resilient_mode:
                self.log_error("Row data is invalid:\n" + self.row_errors.as_text())
                raise SkipRow
            else:
                raise ValidationError(self.row_errors.get_json_data())

        return self.cleaned_data

    def update_object(self, obj: Model, is_new: bool) -> None:
        """
        Use the command's fields to update the supplied ``obj`` according to
        the values in ``self.cleaned_data``. In case it contains anything useful,
        the full, raw row data is also provided as ``raw_data``.
        """
        obj.last_imported_at = timezone.now()
        for field in self.fields.values():
            field.update_object(obj, self.cleaned_data, is_new)

    def add_row_error(self, field: str, error: Union[ValidationError, list, dict]):
        """
        Update the content of `self.row_errors` for the current row.
        The `field` argument is the name of the field to which the errors
        should be added. If it's None, treat the errors as NON_FIELD_ERRORS.

        The `error` argument can be a single error, a list of errors, or a
        dictionary that maps field names to lists of errors. An "error" can be
        either a simple string or an instance of ValidationError with its
        message attribute set and a "list or dictionary" can be an actual
        `list` or `dict` or an instance of ValidationError with its
        `error_list` or `error_dict` attribute set.

        If `error` is a dictionary, the `field` argument *must* be None and
        errors will be added to the fields that correspond to the keys of the
        dictionary.
        """
        if not isinstance(error, ValidationError):
            # Normalize to ValidationError and let its constructor
            # do the hard work of making sense of the input.
            error = ValidationError(error)

        if hasattr(error, "error_dict"):
            if field is not None:
                raise TypeError(
                    "The argument `field` must be `None` when the `error` "
                    "argument contains errors for multiple fields."
                )
            else:
                error = error.error_dict
        else:
            error = {field or NON_FIELD_ERRORS: error.error_list}

        for field, error_list in error.items():
            if field not in self.row_errors:
                if field == NON_FIELD_ERRORS:
                    self.row_errors[field] = ErrorList(error_class="nonfield")
                else:
                    self.row_errors[field] = ErrorList()
            self.row_errors[field].extend(error_list)
            if field in self.cleaned_data:
                del self.cleaned_data[field]

    def row_has_error(self, field: str, code: Optional[str] = None) -> bool:
        return field in self.row_errors and (
            code is None
            or any(error.code == code for error in self.row_errors.as_data()[field])
        )

    def is_valid(self, obj: Model, is_new: bool) -> bool:
        self.logger.info("Validating updated object.")
        try:
            self.validate_object(obj, is_new)
            return True
        except ValidationError as e:
            if self.resilient_mode:
                self.log_error("Validation failure", e)
                return False
            else:
                raise

    def validate_object(self, obj: Model, is_new: bool) -> None:
        obj.full_clean()

    def conditionally_save_object(self, obj: Model, is_new: bool) -> None:
        if self.dryrun:
            return

        self.logger.info(f"Saving object")
        try:
            self.save_object(obj, is_new)
            # Avoid re-processing if the source data includes duplicates...
            self.source_ids_from_db.add(getattr(obj, self.target_model_id_field))
            return
        except (IntegrityError, ValidationError, IOError) as e:
            if self.resilient_mode:
                self.log_error("Failed to save object", e)
            else:
                raise

    def save_object(self, obj: Model, is_new: bool) -> None:
        obj.save()

    def on_command_completed(self, options: Dict[str, Any]) -> None:
        super().on_command_completed(options)
        if self.errors:
            self.log_info("The following errors were encountered during this import:")
            for e in self.errors:
                self.logger.info(e)


class BasePageImportCommand(BaseImportCommand):
    parent_page_type = None
    move_existing_pages = False

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--parent-id", type=int)

    def process_options(self, options: Mapping[str, Any]) -> None:
        super().process_options(options)
        self.parent_id = options.get("parent_id")

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
        if getattr(obj, "legacy_path", None):
            try:
                path_segments = [seg for seg in obj.legacy_path.split("/") if seg]
                obj.slug = path_segments.pop()
            except IndexError:
                pass

        if is_new:
            return self.save_new_page(obj)
        return self.save_existing_page(obj)

    def save_new_page(self, page: models.LegacyPageMixin) -> Page:
        parent = self.get_parent_page(page)
        # repair parent (if damaged by previous failure)
        parent.numchild = parent.get_children().count()
        # ensure slug is unique amongst it's intended siblings
        page.slug = get_unique_slug(page, parent)

        with transaction.atomic():
            parent.add_child(instance=page)

        return page

    def save_existing_page(self, page: models.LegacyPageMixin) -> Page:
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

    def get_parent_page(self, obj: models.LegacyPageMixin):
        try:
            ideal_path = obj.get_ideal_parent_path()
        except AttributeError:
            return self.default_parent_page
        try:
            return self.finders["pages"].find(ideal_path)
        except Page.DoesNotExist:
            return self.default_parent_page


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


class BaseQuerySetProcessingCommand(ReadingCommand):
    reader = readers.QuerySetReader

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--dryrun",
            "-d",
            action="store_true",
            help=("Run the command without saving any changes to the database."),
        )
        # Allow parent classes to modify arguments too
        super().add_arguments(parser)

    def process_options(self, options: Dict[str, Any]) -> None:
        super().process_options(options)
        self.dryrun = options.get("dryrun") or False

    def process_row(
        self,
        row_number: int,
        data: Any,
        max_page_size: int = None,
        current_page_size: int = None,
        current_page_row_number: int = None,
    ):
        self.current_object = obj = data
        self.logger.info(self.get_object_description(obj))

        if self.skip_update(obj):
            self.logger.info("Skipping update.")
            return None

        self.update_object(obj)

        if self.dryrun or self.skip_save(obj):
            self.logger.debug("Skipping save.")
            return None

        self.logger.debug("Saving.")
        self.save_object(obj)

    def skip_update(self, obj):
        return False

    def update_object(self, obj):
        raise NotImplementedError

    def skip_save(self, obj):
        return False

    def save_object(self, obj):
        obj.save()


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

    @property
    def image_finder(self):
        from .finders import ImageFinder

        return self.get_or_create_finder("images", ImageFinder)

    @property
    def document_finder(self):
        from .finders import DocumentFinder

        return self.get_or_create_finder("documents", DocumentFinder)

    @property
    def page_finder(self):
        from .finders import PageFinder

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
