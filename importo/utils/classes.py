import logging
import warnings
from typing import TYPE_CHECKING, Union

from django.utils.functional import cached_property

from importo.constants import LOGGING_LINE_LENGTH

if TYPE_CHECKING:
    from importo.commands import BaseCommand


class LoggingShortcutsMixin:
    @cached_property
    def logger(self):
        return self.get_logger()

    def get_logger(self):
        raise NotImplementedError

    def log(
        self, msg: str, *args, level: int = logging.INFO, exc_info=None, **kwargs
    ) -> None:
        if args or kwargs:
            msg = msg.format(*args, **kwargs)
        self.logger.log(level, msg, stacklevel=3, exc_info=exc_info)

    def log_error(self, msg: str, *args, exc_info=None, **kwargs):
        return self.log(msg, *args, exc_info=exc_info, level=logging.ERROR, **kwargs)

    def log_warning(self, msg: str, *args, exc_info=None, **kwargs):
        return self.log(msg, *args, exc_info=exc_info, level=logging.WARNING, **kwargs)

    def log_info(
        self,
        msg: str,
        *args,
        underline: Union[bool, str] = False,
        overline: Union[bool, str] = False,
        **kwargs,
    ):
        msg_new = ""
        if overline:
            char = "-" if isinstance(overline, bool) else str(overline)
            msg_new += char * (LOGGING_LINE_LENGTH // len(char))
            msg_new += "\n"
        msg_new += msg
        if underline:
            char = "-" if isinstance(underline, bool) else str(underline)
            msg_new += "\n"
            msg_new += char * (LOGGING_LINE_LENGTH // len(char))
        return self.log(msg_new, *args, level=logging.INFO, **kwargs)

    def log_debug(self, msg: str, *args, exc_info=None, **kwargs):
        return self.log(msg, *args, exc_info=exc_info, level=logging.DEBUG, **kwargs)


class CommandBoundMixin:
    def __init__(self, *args, command: "BaseCommand" = None, **kwargs):
        if command is None:
            self.command = command
        else:
            self.bind_to_command(command)
        super().__init__(*args, **kwargs)

    def bind_to_command(self, command: "BaseCommand") -> None:
        self.command = command

    def check_bound_to_command(self):
        if not isinstance(self.command, "BaseCommand"):
            raise RuntimeError(
                f"{self} is not bound to a command. Did you forget to run bind_to_command()?"
            )


class CommandBoundObject(CommandBoundMixin, LoggingShortcutsMixin):
    def __init__(self, command: "BaseCommand" = None):
        super().__init__(command=command)

    def get_logger(self):
        try:
            return self.command.logger
        except AttributeError:
            warnings.warn(
                f"{type(self).__name__} should be bound to a command with a logger "
                "available as `self.logger`, but isn't. Did you forget to run "
                "bind_to_command()?"
            )
            logger = logging.getLogger(self.__class__.__module__)
            logger.setLevel(logging.INFO)
            logger.propagate = False
            return logger


class CopyableMixin:
    def __copy__(self):
        return type(self)(**self.get_copy_kwargs())

    def get_copy_kwargs(self):
        return {
            key.lstrip("_"): val
            for key, val in self.__dict__.items()
            if not isinstance(getattr(self.__class__, key, None), cached_property)
        }
