from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Tuple, Union

from django.core.exceptions import ValidationError
from django.db.models.base import ModelBase
from django.utils.translation import gettext_lazy as _
from wagtail.core.models import Page
from wagtail.documents import get_document_model
from wagtail.images import get_image_model

from importo.constants import NOT_SPECIFIED
from importo.fields import error_codes, strategy_codes
from importo.fields.file import ImageFileField
from importo.fields.related import BaseFinderField

if TYPE_CHECKING:
    from importo.commands import BaseImportCommand

Document = get_document_model()
Image = get_image_model()


class ImageReferenceField(BaseFinderField):
    """
    A field that converts legacy image path or ID values to Wagtail Image
    instances, with the help of the bound command instance's `ImageFinder`.
    """

    on_not_found_choices = [
        NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.SKIP_FIELD,
        strategy_codes.USE_FALLBACK,
        strategy_codes.ATTEMPT_DOWNLOAD,
    ]

    finder_name = "images"

    def __init__(
        self,
        *,
        source: str = None,
        target_field: str = None,
        fallback: Optional[Any] = NOT_SPECIFIED,
        required: bool = True,
        on_missing_value: Optional[str] = NOT_SPECIFIED,
        on_empty_value: Optional[str] = NOT_SPECIFIED,
        on_not_found: Optional[Union[str, None]] = NOT_SPECIFIED,
        file_path_replace: Optional[Sequence[Tuple[str, str]]] = None,
        title_source: Optional[str] = None,
        alt_source: Optional[str] = None,
        error_messages: Optional[Mapping[str, str]] = None,
        validators: Optional[Sequence[callable]] = (),
        command: Optional["BaseImportCommand"] = None,
    ):
        self.file_path_replace = file_path_replace or ()
        self.title_source = title_source
        self.alt_source = alt_source
        super().__init__(
            source=source,
            target_field=target_field,
            fallback=fallback,
            required=required,
            on_missing_value=on_missing_value,
            on_empty_value=on_empty_value,
            on_not_found=on_not_found,
            error_messages=error_messages,
            validators=validators,
            command=command,
        )

    def handle_not_found(self, value):
        strategy = self.on_not_found
        if strategy == strategy_codes.ATTEMPT_DOWNLOAD:

            file_path = value
            for f, r in self.file_path_replace:
                file_path = file_path.replace(f, r)

            image_field = ImageFileField(
                on_download_error=strategy_codes.RAISE_ERROR,
                on_file_invalid=strategy_codes.RAISE_ERROR,
                max_width=4000,
                max_height=4000,
                on_max_dimensions_exceeded=strategy_codes.SHRINK_IMAGE,
                command=self.command,
            )
            image_field.name = "file"
            image_file = image_field.clean(file_path)

            kwargs = {"title": "", "alt": "", "legacy_path": file_path}

            if self.title_source is not None:
                try:
                    kwargs["title"] = self.extract_row_value(
                        self.title_source, self.command.row_data
                    )
                except (AttributeError, KeyError):
                    pass

            if self.alt_source is not None:
                try:
                    kwargs["alt"] = self.extract_row_value(
                        self.alt_source, self.command.row_data
                    )
                except (AttributeError, KeyError):
                    pass

            if not kwargs["title"]:
                for_obj = self.command.current_object
                kwargs["title"] = (
                    f"Downloaded {self.target_field or self.name} image for {type(for_obj).__name__} "
                    f"{for_obj.pk or '(NEW)'}"
                )

            error_to_reraise = ValidationError(
                f"Error creating new image with details: {kwargs}."
            )

            obj = Image(**kwargs)
            try:
                # ImageFileField.update_object() does some useful additional stuff,
                # like setting the file_hash, width and height fields
                image_field.update_object(
                    obj, cleaned_data={"file": image_file}, is_new=True
                )
            except Exception as e:
                raise error_to_reraise from e
            try:
                # save the fully updated image
                obj.save()
            except Exception as e:
                raise error_to_reraise from e

            # Add to the finder cache for faster repeat lookups
            self.finder.add_to_cache(obj, file_path)
            return obj

        return super().handle_not_found(value)


class DocumentReferenceField(BaseFinderField):
    """
    A field that converts legacy document path values to Wagtail Document
    instances, with the help of the bound command instance's
    `DocumentFinder` (which takes care of caching).
    """

    finder_name = "documents"


class PageReferenceField(BaseFinderField):
    finder_name = "pages"

    default_error_messages = {
        error_codes.INCORRECT_PAGE_TYPE: _("%(value)s is not a %(return_type)s."),
    }

    def __init__(
        self,
        *,
        source: str = None,
        target_field: str = None,
        fallback: Optional[Any] = NOT_SPECIFIED,
        page_type: ModelBase = None,
        on_not_found: Optional[Union[str, None]] = NOT_SPECIFIED,
        on_missing_value: Optional[str] = NOT_SPECIFIED,
        on_empty_value: Optional[str] = NOT_SPECIFIED,
        required: bool = True,
        error_messages: Optional[Mapping[str, str]] = None,
        validators: Optional[Sequence[callable]] = (),
        command: Optional["BaseImportCommand"] = None,
    ):
        self.page_type = page_type
        super().__init__(
            source=source,
            target_field=target_field,
            fallback=fallback,
            on_not_found=on_not_found,
            on_missing_value=on_missing_value,
            on_empty_value=on_empty_value,
            required=required,
            error_messages=error_messages,
            validators=validators,
            command=command,
        )

    def validate(self, value):
        if isinstance(value, Page):
            # check the page is of the correct type
            if self.page_type and not issubclass(value.specific_class, self.page_type):
                msg = self.error_messages[error_codes.OBJECT_NOT_FOUND] % {
                    "value": f"<{value.specific_class.__name__} id={value.id} title='{value.title}'>",
                    "return_type": self.page_type,
                }
                raise ValidationError(msg, code=error_codes.OBJECT_NOT_FOUND)
        super().validate(value)
