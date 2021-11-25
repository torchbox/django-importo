import re

EMPTY_PARAGRAPH_REGEX = r"<p[^>]*>(\s|&nbsp;|</?\s?br\s?/?>)*</?p>"
EXCESS_WHITESPACE_REGEX = r"\n\s*\n"
LINEBREAKS_REGEX = r"\n"
BR_REGEX = r"<br/>"
MULTI_BR_REGEX = r"<br/?>(\s?<br/?>)*"


def tidy_html(
    value: str,
    remove_empty_paragraphs: bool = True,
    remove_excess_whitespace: bool = True,
    remove_linebreaks: bool = True,
    for_richtext: bool = False,
):
    # strip empty <p> tags
    if remove_empty_paragraphs:
        value = re.sub(EMPTY_PARAGRAPH_REGEX, "", value, flags=re.DOTALL)
    # strip excessive whitespace
    if remove_excess_whitespace:
        value = re.sub(EXCESS_WHITESPACE_REGEX, "\n", value, flags=re.DOTALL)
    # strip all linebreaks
    if remove_linebreaks:
        value = re.sub(LINEBREAKS_REGEX, "", value)
    # make value suitable for use as a Wagtail richtext value
    if for_richtext:
        value = re.sub(BR_REGEX, "<br>", value)
    return value
