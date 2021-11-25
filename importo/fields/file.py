import os
import sys
from io import BytesIO
from typing import Any, Dict, Optional, Sequence, Tuple

import PIL
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile, UploadedFile
from django.db.models import Model
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from importo.constants import NOT_SPECIFIED
from importo.errors import SkipField, SkipRow
from importo.utils.io import fetch_file, filename_from_url, static_file_to_bytesio

from . import base, constants, error_codes, strategy_codes

DUMMY_IMAGE_PATH = "importo/dummy_files/dummy.png"
DUMMY_DOCUMENT_PATH = "importo/dummy_files/dummy.pdf"
MAX_FILESIZE = 1024 * 1024 * 30  # 30MB


class FileField(base.Field):
    dummy_file_path = DUMMY_DOCUMENT_PATH
    on_download_error_choices = [
        NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.SKIP_FIELD,
        strategy_codes.SKIP_ROW,
        strategy_codes.USE_FALLBACK,
        strategy_codes.USE_DUMMY,
    ]
    on_file_invalid_choices = [
        NOT_SPECIFIED,
        strategy_codes.RAISE_ERROR,
        strategy_codes.SKIP_FIELD,
        strategy_codes.SKIP_ROW,
        strategy_codes.USE_FALLBACK,
        strategy_codes.USE_DUMMY,
    ]
    on_file_invalid_default = strategy_codes.RAISE_ERROR
    on_extension_invalid_choices = list(on_file_invalid_choices)

    default_error_messages = {
        error_codes.DOWNLOAD_ERROR: _("Failed to download file: %(error)s"),
        error_codes.MAX_FILESIZE_EXCEEDED: _(
            "The file is %(mb)sMB in size, which exceeds the 30MB limit",
        ),
        error_codes.INVALID_FILE_EXTENSION: _(
            "'%(extension)s' is not valid extension for this field."
        ),
    }

    # Downloading and cleaning files requires expensive I/O operations,
    # so these fields should be cleaned as late as possible
    clean_cost = constants.CLEAN_COST_HIGH

    def __init__(
        self,
        *args,
        file_path_replace: Optional[Sequence[Tuple[str, str]]] = None,
        allowed_extensions: Sequence[str] = None,
        on_download_error: Optional[str] = NOT_SPECIFIED,
        on_extension_invalid: Optional[str] = NOT_SPECIFIED,
        on_file_invalid: Optional[str] = NOT_SPECIFIED,
        max_retries: int = 3,
        max_filesize: int = MAX_FILESIZE,
        **kwargs,
    ):
        self.file_path_replace = file_path_replace or ()
        self.allowed_extensions = allowed_extensions or ()
        self.on_extension_invalid = on_extension_invalid
        self.on_download_error = on_download_error
        self.on_file_invalid = on_file_invalid
        self.max_retries = max_retries
        self.max_filesize = max_filesize
        super().__init__(*args, **kwargs)

    @property
    def on_download_error(self):
        """
        Return the preferred strategy for handling download errors for this field.
        """
        if self._on_download_error != NOT_SPECIFIED:
            return self._on_download_error
        try:
            return self.command.on_download_error
        except AttributeError:
            return strategy_codes.RAISE_ERROR

    @on_download_error.setter
    def on_download_error(self, value):
        """
        Validate and set the preferred strategy for handling download errors for this field.
        """
        valid_choices = self.on_download_error_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_download_error' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_download_error = value

    @property
    def on_file_invalid(self):
        """
        Return the preferred strategy for handling invalid files for this field.
        """
        if self._on_file_invalid != NOT_SPECIFIED:
            return self._on_file_invalid
        try:
            return self.command._on_file_invalid
        except AttributeError:
            return strategy_codes.RAISE_ERROR

    @on_file_invalid.setter
    def on_file_invalid(self, value):
        """
        Validate and set the preferred strategy for handling invalid files for this field.
        """
        valid_choices = self.on_file_invalid_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_file_invalid' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_file_invalid = value

    @property
    def on_extension_invalid(self):
        """
        Return the preferred strategy for handling invalid extensions encountered by
        this field.
        """
        if self._on_extension_invalid != NOT_SPECIFIED:
            return self._on_extension_invalid
        try:
            return self.command.on_extension_invalid
        except AttributeError:
            return self.on_file_invalid

    @on_extension_invalid.setter
    def on_extension_invalid(self, value):
        """
        Validate and set the preferred strategy for handling invalid extensions encountered by
        this field.
        """
        valid_choices = self.on_extension_invalid_choices
        if not callable(value) and value not in valid_choices:
            raise TypeError(
                "'on_extension_invalid' must be a callable or one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_extension_invalid = value

    @cached_property
    def dummy_file(self):
        file = static_file_to_bytesio(self.dummy_file_path)
        return file

    def get_dummy_uploadedfile(self, url: str) -> UploadedFile:
        self.dummy_file.seek(0)
        value = SimpleUploadedFile(filename_from_url(url), self.dummy_file.getvalue())
        value.is_dummy = True
        return value

    @property
    def use_dummy_file(self):
        return getattr(self.command, "mock_downloads", False)

    def to_python(self, value: Any) -> UploadedFile:
        if isinstance(value, UploadedFile):
            return value

        # Ensure value is a string, and make replacements
        value = str(value)
        for _find, _replace in self.file_path_replace:
            value = value.replace(_find, _replace)

        if self.use_dummy_file:
            self.log_debug(f"Using dummy file for '{self.target_field}'")
            return self.get_dummy_uploadedfile(value)

        try:
            self.log_debug(f"Downloading: {value}")
            file = fetch_file(value, add_hash=True, max_retries=self.max_retries)
        except Exception as e:
            self.log_debug(f"Encountered error while downloading: {e}")
            strategy = self.on_download_error
            if strategy == strategy_codes.USE_FALLBACK:
                return self.to_python(self.get_fallback())
            elif strategy == strategy_codes.USE_DUMMY:
                return self.get_dummy_uploadedfile(value)
            elif callable(strategy):
                return self.to_python(strategy(value))
            elif strategy == strategy_codes.SKIP_FIELD:
                raise SkipField
            elif strategy == strategy_codes.SKIP_ROW:
                raise SkipRow

            # Assume strategy == RAISE_ERROR
            msg = self.error_messages[error_codes.DOWNLOAD_ERROR] % {"error": e}
            raise ValidationError(msg, code=error_codes.DOWNLOAD_ERROR)

        # Convert fetched file to UploadedFile
        return_value = SimpleUploadedFile(filename_from_url(value), file.getvalue())
        # Prevent closing of file during validation
        return_value.close = lambda: None
        return return_value

    def clean(self, value: str):
        value = self.to_python(value)
        try:
            self.validate(value)
            self.run_validators(value)
        except ValidationError as e:
            return self.handle_invalid_file(
                value, e, getattr(e, "code", error_codes.INVALID)
            )
        return value

    def handle_invalid_file(
        self, value: SimpleUploadedFile, error: ValidationError, code: str
    ):
        """
        A hook that allows subclasses to take some kind of action when validation
        results in a ValidationError. This is needed to support strategies that
        involve replacing the return value entirely, because validate() and other
        methods invoked by validate() do not have that power.
        """
        if code == error_codes.INVALID_FILE_EXTENSION:
            strategy = self.on_extension_invalid
        else:
            strategy = self.on_file_invalid

        # Take appropriate action
        if strategy == strategy_codes.SKIP_FIELD:
            raise SkipField
        if strategy == strategy_codes.SKIP_ROW:
            raise SkipRow
        if strategy == strategy_codes.USE_DUMMY:
            return self.get_dummy_uploadedfile(value.name)
        if strategy == strategy_codes.USE_FALLBACK:
            return self.get_fallback()
        if callable(strategy):
            return strategy(value)
        # Assume strategy == RAISE_ERROR
        raise error

    def validate(self, value: SimpleUploadedFile):
        """
        Overrides Field.validate() to check the file size and extension
        """
        self.check_filesize(value)
        self.check_format(value)

    def check_filesize(self, value: SimpleUploadedFile):
        size = sys.getsizeof(value)
        value.seek(0)
        if self.max_filesize is not None and size > self.max_filesize:
            code = error_codes.MAX_FILESIZE_EXCEEDED
            msg = self.error_messages[code] % {
                "mb": (size / 1024) / 1024,
            }
            raise ValidationError(msg, code=code)

    def check_format(self, value: SimpleUploadedFile):
        extension = os.path.splitext(value.name)[1].lower()[1:]
        if self.allowed_extensions and extension not in self.allowed_extensions:
            code = error_codes.INVALID_FILE_EXTENSION
            msg = self.error_messages[code] % {"extension": extension}
            raise ValidationError(msg, code=code)

    def update_object(self, obj: Model, cleaned_data: Dict[str, Any], is_new: bool):
        target_attr = self.target_field or self.name
        original_value = getattr(obj, target_attr, None)
        new_value = cleaned_data.get(target_attr)

        # avoid replacing existing files with dummies
        if original_value and getattr(new_value, "is_dummy", False):
            return

        super().update_object(obj, cleaned_data, is_new)


class ImageFileField(FileField):
    dummy_file_path = DUMMY_IMAGE_PATH
    default_error_messages = {
        error_codes.INVALID: _(
            "The downloaded file '%(filename)s' is not a valid image."
        ),
        error_codes.MAX_FILESIZE_EXCEEDED: _(
            "The file is %(mb)sMB in size, which exceeds the 30MB limit.",
        ),
        error_codes.MAX_WIDTH_EXCEEDED: _(
            "The image has a width of %(width)s pixels, which exceeds the %(max)s limit."
        ),
        error_codes.MAX_HEIGHT_EXCEEDED: _(
            "The image has a height of %(height)s pixels, which exceeds the %(max)s limit."
        ),
    }
    on_max_dimensions_exceeded_choices = FileField.on_file_invalid_choices + [
        strategy_codes.SHRINK_IMAGE
    ]
    on_max_dimensions_exceeded_default = strategy_codes.RAISE_ERROR
    on_extension_invalid_choices = FileField.on_file_invalid_choices + [
        strategy_codes.CONVERT_TO_WEBP
    ]

    def __init__(
        self,
        *args,
        optimize: bool = False,
        convert_to_webp: bool = False,
        retain_metadata: bool = True,
        max_width: int = None,
        max_height: int = None,
        on_max_dimensions_exceeded: Optional[str] = NOT_SPECIFIED,
        allowed_extensions: Sequence[str] = ("jpg", "jpeg", "gif", "png", "webp"),
        **kwargs,
    ):
        self.optimize = optimize
        self.convert_to_webp = convert_to_webp
        self.retain_metadata = retain_metadata
        self.max_width = max_width
        self.max_height = max_height
        self.on_max_dimensions_exceeded = on_max_dimensions_exceeded
        self.allowed_extensions = allowed_extensions or ()
        super().__init__(*args, **kwargs)

    @property
    def on_max_dimensions_exceeded(self):
        """
        Return the preferred strategy for handling images that are wider or taller than desired.
        """
        if self._on_max_dimensions_exceeded != NOT_SPECIFIED:
            return self._on_max_dimensions_exceeded
        return self.on_max_dimensions_exceeded_default

    @on_max_dimensions_exceeded.setter
    def on_max_dimensions_exceeded(self, value):
        """
        Validate and set the preferred strategy for handling invalid images that are wider or taller than desired.
        """
        valid_choices = self.on_max_dimensions_exceeded_choices
        if value not in valid_choices:
            raise TypeError(
                "'on_max_dimensions_exceeded' must be one of the following "
                f"values (not '{value}'): {valid_choices}."
            )
        self._on_max_dimensions_exceeded = value

    @staticmethod
    def get_pil_image(file: SimpleUploadedFile) -> PIL.Image:
        try:
            image = PIL.Image.open(file)
        except OSError as e:
            if "Truncated File Read" in str(e):
                pass  # truncated images are generally okay
            else:
                raise e
        return image

    def validate(self, value: SimpleUploadedFile):
        super().validate(value)
        try:
            pil_image = self.get_pil_image(value)
        except Exception as e:
            raise ValidationError(
                f"Image could not be interpretted by Pillow. The error was: {e}"
            )
        self.check_image_validity(value, pil_image)
        self.check_image_dimensions(value, pil_image)

    def check_format(self, value: SimpleUploadedFile):
        """
        Overriding FileField.check_format() to support conversion
        to WebP for unsupported image file extensions.
        """
        try:
            super().check_format(value)
        except ValidationError:
            if self.on_extension_invalid == strategy_codes.CONVERT_TO_WEBP:
                value._convert_to_webp = True
            else:
                raise

    def check_image_validity(self, value, pil_image):
        try:
            # Ensure we're working with a valid image
            pil_image.verify()
        except Exception:
            msg = self.error_messages[error_codes.INVALID] % {"filename": value.name}
            raise ValidationError(msg, code=error_codes.INVALID)

    def check_image_dimensions(self, value, pil_image):
        width, height = pil_image.size
        too_wide = self.max_width is not None and width > self.max_width
        if too_wide:
            code = error_codes.MAX_WIDTH_EXCEEDED
            msg = self.error_messages[code] % {
                "width": width,
                "max": self.max_width,
            }
            if self.on_max_dimensions_exceeded == strategy_codes.RAISE_ERROR:
                raise ValidationError(msg, code=code)
            else:
                self.log_debug(msg)
                value._shrink_me = True

        too_tall = self.max_height is not None and height > self.max_height
        if too_tall:
            code = error_codes.MAX_HEIGHT_EXCEEDED
            msg = self.error_messages[code] % {
                "height": height,
                "max": self.max_height,
            }
            if self.on_max_dimensions_exceeded == strategy_codes.RAISE_ERROR:
                raise ValidationError(msg, code=code)
            else:
                self.log_debug(msg)
                value._shrink_me = True

    def clean(self, value):
        """
        Extends Field.clean() to optionally optimize or resize the image after
        it has been validated.
        """
        value = super().clean(value)

        if self.is_empty(value):
            return value

        # Reset file pointer
        value.seek(0)

        # Exit early if no changes are required
        if (
            not self.optimize
            and not self.convert_to_webp
            and not getattr(value, "_shrink_me", False)
            and not getattr(value, "_convert_to_webp", False)
        ):
            return value

        # Create (another) in-memory image
        pil_image = self.get_pil_image(value)
        value.seek(0)
        target_format = pil_image.format
        converting_to_webp = bool(
            (self.convert_to_webp or getattr(value, "_convert_to_webp", False))
            and pil_image.format != "WebP"
        )
        if converting_to_webp:
            self.log_debug("Converting image to WebP")
            target_format = "WebP"
        elif pil_image.format in ("TIFF", "BMP", "JPEG 2000"):
            target_format = "JPEG"

        width, height = pil_image.size
        if getattr(value, "_shrink_me", False):
            self.log_debug("Shrinking image")
            try:
                pil_image.thumbnail(
                    size=(self.max_width or width, self.max_height or height)
                )
            except OSError as e:
                if "image file is truncated" in str(e):
                    pass  # truncated images are generally okay
                else:
                    raise

        elif self.optimize:
            self.log_debug("Optimizing image")

        # Tailor save kwargs depending on the image format
        kwargs = {"format": target_format}
        if target_format in "JPEG":
            kwargs.update(optimize=True, quality="web_very_high")
        if target_format == "PNG":
            kwargs.update(optimize=True)
        if target_format == "WebP":
            kwargs.update(method=5, quality=90)
        if self.retain_metadata and target_format in ("JPEG", "PNG", "WebP"):
            if exif := pil_image.info.get("exif"):
                kwargs.update(exif=exif)

        # Save to a new BytesIO object
        new_file = BytesIO()
        # TODO: Document that setting PIL.ImageFile.LOAD_TRUNCATED_IMAGES = True is a good idea!
        pil_image.save(new_file, **kwargs)

        # Return a new SimpleUploadedFile
        return SimpleUploadedFile(value.name, new_file.getvalue())
