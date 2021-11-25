from typing import Any, Dict

from django.db.models import Model
from django.utils.translation import gettext_lazy as _
from wagtail.documents import get_document_model
from wagtail.images import get_image_model

from importo.fields.file import FileField as RegularFileField
from importo.fields.file import ImageFileField as RegularImageFileField
from importo.utils.io import get_bytesio_hash

Image = get_image_model()
Document = get_document_model()


class SetHashMixin:
    def update_object(self, obj: Model, cleaned_data: Dict[str, Any], is_new: bool):
        target_attr = self.target_field or self.name
        original_value = getattr(obj, target_attr, None)
        new_value = cleaned_data.get(target_attr)
        new_file_is_dummy = getattr(new_value, "is_dummy", False)

        # avoid replacing existing files with dummies
        if new_file_is_dummy and original_value:
            return

        # if updating the main 'file' field of a Wagtail image or document object,
        # compare and update 'file_hash'
        if (
            new_value is not None
            and not new_file_is_dummy
            and target_attr == "file"
            and isinstance(obj, (Document, Image))
        ):
            # Only update 'file' and 'file_hash' if the new file is different
            # to the current value
            new_file_hash = get_bytesio_hash(new_value)
            new_value.seek(0)
            if not obj.file_hash:
                # this a new obj, so set file_hash without shouting about it
                obj.file_hash = new_file_hash
            elif obj.file_hash != new_file_hash:
                self.log_debug("File has changed. Updating 'file_hash'.")
                # the 'file' value itself will be set by super().update_object()
                obj.file_hash = new_file_hash
            else:
                self.log_debug("File has NOT changed. Ignoring.")
                # we don't want the 'file' value to be set, so exit here
                return
        super().update_object(obj, cleaned_data, is_new)


class FileField(SetHashMixin, RegularFileField):
    pass


class ImageFileField(SetHashMixin, RegularImageFileField):
    def set_object_value(self, obj, attribute_name, value):
        """
        When updating the 'file' field of an Image instance, while we have the
        file contents in-memory, set the height and width field values using
        pillow. If errors happen when saving the file, this will prevent
        ImageField.update_dimension_fields() from reading the non-existent file
        in order to work them out.
        """
        if attribute_name == "file" and isinstance(obj, Image) and value:
            pillow_image = self.get_pil_image(value)
            width, height = pillow_image.size
            value.seek(0)
            obj.height = height
            obj.width = width
        super().set_object_value(obj, attribute_name, value)
