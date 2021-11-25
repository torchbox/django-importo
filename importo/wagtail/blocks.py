from django.core.exceptions import ValidationError
from django.forms.utils import ErrorList
from tate.utils.blocks import OrderedSubBlocksStructBlock
from wagtail.core import blocks
from wagtail.core.blocks.struct_block import StructBlockValidationError


class LegacyIDBlock(blocks.CharBlock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field.widget.attrs["readonly"] = "readonly"


class LegacyReferenceStructBlock(OrderedSubBlocksStructBlock):
    """
    A base block for StructBlocks that reference objects that might not
    exits at the time of creating, but should exist once the migration is
    complete.
    """

    # Set this to the name of the ChooserBlock referencing
    # the page that might not yet exist at time of migration
    REAL_REFERENCE_BLOCK: str = ""

    # NOTE: This should always be optional
    legacy_id = LegacyIDBlock(
        label="Legacy ID",
        help_text="Please ignore this field. It exists to facilitate migration only and will be removed soon.",
        required=False,
    )

    def clean(self, value):
        """
        Overrides StructBlock.clean() to silence validation errors for REAL_REFERENCE_BLOCK
        when 'legacy_id' is set, allowing the page to be saved.
        """
        result = []
        # build up a list of (name, value) tuples to be passed to the StructValue constructor
        errors = {}
        for name, val in value.items():
            try:
                result.append((name, self.child_blocks[name].clean(val)))
            except ValidationError as e:
                # It's just these couple of lines here that are new!
                if (
                    name == self.REAL_REFERENCE_BLOCK
                    and e.code == "required"
                    and value.get("legacy_id")
                ):
                    result.append((name, None))
                else:
                    errors[name] = ErrorList([e])

        if errors:
            raise StructBlockValidationError(errors)

        return self._to_struct_value(result)
