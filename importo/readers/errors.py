from django.utils.translation import gettext_lazy as _


class BaseReaderException(Exception):
    pass


class BasePaginatedReaderException(BaseReaderException):
    MESSAGE_FMT = ""

    def __init__(self, page_number: int):
        self.page_number = page_number

    @property
    def message(self):
        return self.MESSAGE_FMT.format(**self.__dict__)


class LastPageReached(BasePaginatedReaderException):
    MESSAGE_FMT = _("Page {page_number} is the final page.")


class StopPageReached(BasePaginatedReaderException):
    MESSAGE_FMT = _("Page {page_number} is the 'stop-page'.")


class EmptyPageRecieved(BasePaginatedReaderException):
    MESSAGE_FMT = _("Page {page_number} contained zero rows.")


class ShortPageRecieved(BasePaginatedReaderException):
    MESSAGE_FMT = _(
        "Page {page_number} contained {page_size} rows, which falls short of the usual {full_page_size}."
    )

    def __init__(self, current_page_number: int, page_size: int, full_page_size: int):
        super().__init__(current_page_number)
        self.page_size = page_size
        self.full_page_size = full_page_size
