import logging
from typing import Any, Iterable, Mapping, Sequence

from importo.constants import LOGGING_DIVIDER_LENGTH
from importo.utils.logging import within_dividers

from .errors import (
    BasePaginatedReaderException,
    EmptyPageRecieved,
    LastPageReached,
    ShortPageRecieved,
    StopPageReached,
)


class BaseReader:
    supports_pagination = False
    supports_throttling = False

    requires_file_input = False
    file_input_help = ""

    requires_db_connection = False
    db_connection_help = "The database to read data from (from settings.DATABASES)"

    def __init__(
        self,
        start_row: int = None,
        stop_row: int = None,
    ):
        self.start_row = start_row or 1
        if stop_row and stop_row < self.start_row:
            raise ValueError(
                "The 'stop-row' option value must be greater than or equal to 'start-row'."
            )
        self.stop_row = stop_row
        self.current_row_number = None
        self.current_row_data = None
        self._logger = None

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            raise RuntimeError(
                f"{self.__class__.__name__} instance does not have a logger set. Did you forget to call super().setup(options) in your command's setup() method?"
            )
        return self._logger

    @logger.setter
    def logger(self, logger: logging.Logger):
        self._logger = logger

    def __iter__(self) -> Iterable:
        fetch_kwargs = self.get_fetch_kwargs()
        for i, row in enumerate(self.fetch(**fetch_kwargs), self.start_row):
            self.logger.info(within_dividers(f"Processing row: {i}"))
            self.current_row_number = i
            self.sanitize_row(row)
            self.current_row_data = row
            yield row

    def get_fetch_kwargs(self) -> Mapping[str, Any]:
        """
        Return keyword arguments for calling fetch() from __iter__().
        """
        kwargs = {
            "start_row": self.start_row,
            "stop_row": self.stop_row,
        }
        return kwargs

    def fetch(self, start_row: int, stop_row: int = None) -> Iterable:
        """
        Return a sequence of results from the original data source.

        `start_row` is a non zero-indexed number (starting from 1) indicating the first row of interest.

        `stop_row` is an optional non-zero-idexed number (starting from 1) indicating the last row of interest.
        """
        raise NotImplementedError(
            "Subclasses of BaseReader must define their own fetch() method."
        )

    def sanitize_row(self, row: Any) -> None:
        """
        A hook to make any reader-specific changes to a row value before it
        is handed back to the import command for processing. By default, no
        changes are made.

        NOTE: If data requires adjustment regardless of the reader class used,
        You may want to update the sanitize_row() method on your command class
        instead.
        """
        return row


class BasePaginatedReader(BaseReader):
    supports_pagination = True

    # Default number of rows to include in each page fetched from the data source
    default_page_size = 500

    def __init__(
        self,
        page_size: int = None,
        start_page: int = None,
        stop_page: int = None,
        start_row: int = None,
        stop_row: int = None,
    ) -> None:
        self.page_size = page_size or self.default_page_size
        self.start_page = start_page or 1
        self.stop_page = stop_page

        if stop_row is not None and not self.single_page_requested:
            raise ValueError(
                "The 'stop-row' option is only supported when fetching a single page of data."
            )

        self.current_page_number = None
        self.current_page_data = None

        super().__init__(start_row, stop_row)

    @property
    def single_page_requested(self) -> bool:
        return self.stop_page is not None and self.stop_page == self.start_page

    def is_first_page(self, page_number: int) -> bool:
        return page_number == self.start_page

    def is_stop_page(self, page_number: int) -> bool:
        return self.stop_page and page_number == self.stop_page

    def is_last_page(self, page_number: int) -> bool:
        """
        Return a boolean indicating whether the provided `page_number` is
        the last that will be returned from the data source. By default,
        this always returns `False`, but subclasses can implement this
        where relevant.
        """
        return False

    def __iter__(self) -> Iterable:
        """
        Keep returning results until an exception is encountered.
        """
        while True:
            page_number = self.get_next_page_number()
            for result in self.get_results(page_number):

                # Handle 'yielded exceptions' from get_rows()
                if isinstance(result, BasePaginatedReaderException):
                    self.logger.info("-" * LOGGING_DIVIDER_LENGTH)
                    self.logger.info(result.message)
                    return None

                iter_start = self.start_row if self.is_first_page(page_number) else 1
                for i, item in enumerate(result, iter_start):
                    contextual_i = ((page_number - 1) * self.page_size) + i
                    self.logger.info(
                        within_dividers(
                            f"Processing row: {i} of page {self.current_page_number} (Item #{contextual_i})"
                        )
                    )
                    self.current_row_number = i
                    self.current_row_data = self.sanitize_row(item)
                    yield self.current_row_data

    def get_next_page_number(self) -> int:
        if self.current_page_number is None:
            return self.start_page
        return self.current_page_number + 1

    def get_results(self, page_number: int = None) -> Iterable:
        fetch_kwargs = self.get_fetch_kwargs(page_number)
        self.before_fetch(page_number, fetch_kwargs)
        self.logger.info(within_dividers(f"Processing page: {page_number}", "="))
        result = self.fetch(**fetch_kwargs)
        self.after_fetch(page_number, fetch_kwargs, result)
        result_size = len(result)

        self.current_page_data = result
        self.current_page_number = page_number
        self.current_row_data = None
        self.current_row_number = None

        if not result_size:
            yield EmptyPageRecieved(page_number)

        yield result

        if self.is_last_page(page_number):
            yield LastPageReached(page_number)

        if self.is_stop_page(page_number):
            yield StopPageReached(page_number)

        if result_size < self.page_size:
            yield ShortPageRecieved(page_number, result_size, self.page_size)

    def get_fetch_kwargs(self, page_number: int) -> Mapping:
        """
        Return keyword arguments for calling fetch() from __iter__().
        """
        kwargs = {
            "page_number": page_number,
            "start_row": 1,
            "stop_row": None,
        }

        if self.is_first_page(page_number):
            kwargs["start_row"] = self.start_row

        if self.stop_row and (
            self.is_stop_page(page_number) or self.is_last_page(page_number)
        ):
            kwargs["stop_row"] = self.stop_row

        return kwargs

    def before_fetch(self, page_number: int, fetch_kwargs: Mapping) -> None:
        """
        Hook to allow subclasses to invoke custom code before fetch() is called.

        `page_number` is a non zero-indexed number indicating the page to be fetched.

        `fetch_kwargs` is a mapping of keyword argument values prepared by
        `get_fetch_kwargs()`, that will be used to call fetch().
        """
        pass

    def fetch(self, page_number: int, start_row: int, stop_row: int = None) -> Sequence:
        """
        Return a sequence of results from the original data source.

        `page_number` is a non zero-indexed number (starting at 1) indicating
        the page of interest.

        `start_row` is a non zero-indexed number (starting at 1) indicating the
        first row of interest for the page.

        `stop_row` is an optional non-zero-idexed number (starting at 1)
        indicating the last row of interest for the page.
        """
        raise NotImplementedError(
            "Subclasses of BasePaginatedReader must define their own fetch() method."
        )
