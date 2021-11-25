from importo.fields import constants
from importo.fields.base import BaseParsedField
from importo.wagtail.parsers.richtext import RichTextParser


class RichTextField(BaseParsedField):
    default_parser = RichTextParser
    clean_cost = constants.CLEAN_COST_HIGH
    default_fallback = ""
