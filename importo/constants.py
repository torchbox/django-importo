import logging

LOGGING_LINE_LENGTH = 75

VERBOSITY_TO_LOGGING_LEVEL = {
    0: logging.ERROR,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
}

# Used as a default kwarg value where the distinction
# between None and 'not provided' is important
NOT_SPECIFIED = "__NS__"
