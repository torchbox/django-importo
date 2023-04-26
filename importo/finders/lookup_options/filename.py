import os
import re
from typing import Sequence

from django.core.files.storage import DefaultStorage
from django.db.models import Case, IntegerField, Model, When
from django.db.models.query import QuerySet

from importo.finders.lookup_value import LookupValue
from importo.utils.io import filename_from_url
from importo.utils.uri import is_media_uri

from .base import LookupValueError, ValueTypeIncompatible
from .modelfield import ModelFieldLookupOption
from .path import DomainSpecificValuesMixin, ValueDomainInvalid

__all__ = [
    "FilePathLookupOption",
    "FileExtensionInvalid",
]


class FileExtensionInvalid(LookupValueError):
    pass


class FilePathLookupOption(DomainSpecificValuesMixin, ModelFieldLookupOption):
    """
    A lookup option used to find imported objects from legacy filename values.

    Imagine you're importing data from a "Download" content type in Drupal to
    a "Document" model in Django (where a ``django.db.models.FileField`` is
    used to store the file), and the content you're importing contains a lot of
    links with the full file path of the "Download".

    When provided with one of those 'legacy file path' values (which may or may
    not also include a domain), this lookup option would be used to find the
    relevant "Document" object from it's new ``File`` field value - taking into
    account changes that may have happened as part of the migration, such as:

    *   Changes to storage directories
    *   Django adding random strings to filenames to ensure uniqueness
    *   The file being converted to a different file type with another
        extension, for example: PNG images being converted to WebP.
    """

    def __init__(
        self,
        *,
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
    ):
        """
        Overriding to remove the case_sensitive option, as filename lookups
        are always case insensitive.
        """
        super().__init__(
            case_sensitive=False,
            valid_patterns=valid_patterns,
            invalid_patterns=invalid_patterns,
        )

    def value_matches_pattern(self, value: LookupValue, pattern: re.Pattern) -> bool:
        """
        Overrides ``BaseLookupOption.value_matches_pattern()`` to check the extracted
        ``path`` value against patterns instead of the full raw value.
        """
        if self.case_sensitive:
            return bool(pattern.match(value.urlparsed.path))
        return bool(pattern.match(value.urlparsed.path, re.IGNORECASE))

    def validate_lookup_value(self, value: LookupValue) -> None:
        if not isinstance(value.raw, str):
            raise ValueTypeIncompatible
        if value.raw.is_digit():
            raise LookupValueError
        if not is_media_uri(value.urlparsed):
            raise ValueDomainInvalid
        # Avoid lookups for filenames without a 2-5 char extension, which should
        # be the case documents, images, audio and video
        if not re.search(r"\.[a-zA-Z0-9]{2,5}$", value.urlparsed.path):
            raise FileExtensionInvalid
        return super().validate_lookup_value(value)

    def extract_filename(self, value: LookupValue):
        # Extract filename, and treat the value as it would have been
        # treated if cleaned during import
        filename = filename_from_url(os.path.basename(value.urlparsed.path))

        # Treat the value as it would have been if saved by Django
        try:
            storage = self.model_field.storage
        except AttributeError:
            storage = DefaultStorage()
        return storage.generate_filename(filename)

    def find(self, value: LookupValue, queryset: QuerySet) -> Model:
        not_found_msg = f"{self.model.__name__} matching '{value.raw}' does not exist."

        # full paths are rarely preserved when migrating files to Django,
        # so we extract the filename to use in lookup queries
        filename = self.extract_filename(value)

        # Use regex to identify matches, including those with a different
        # extension, or with a 7 character 'unique' string appended to the
        # name (the default behaviour for Django file storages when a filename
        # is not unique at the time of upload)
        name, extension = os.path.splitext(filename)
        queryset = queryset.filter(
            **{
                f"{self.field_name}__regex": (
                    r"" + re.escape(name) + r"(_[a-zA-Z0-9]{7})?\.([a-zA-Z0-9]{2,5})$"
                )
            }
        )

        best_match = (
            queryset.annotate(
                match_quality=Case(
                    # full path matches are best, but it's uncommon for a field's
                    # `upload_to` value to be set to match the directory structure
                    # used on the legacy site
                    When(
                        **{f"{self.field_name}__iexact": value.normalized_path}, then=0
                    ),
                    # filname and extension matches come next
                    When(**{f"{self.field_name}__iendswith": f"/{filename}"}, then=1),
                    # next come filename and extension matches where a 'unique' string
                    # has been added to the filename by Django
                    When(
                        **{
                            f"{self.field_name}__icontains": f"/{name}_",
                            f"{self.field_name}__iendswith": f".{extension}",
                        },
                        then=2,
                    ),
                    # and finally, filename matches with a different extension
                    When(**{f"{self.field_name}__icontains": f"/{name}."}, then=3),
                    default=4,
                    output_field=IntegerField(),
                )
            )
            .order_by("match_quality")
            .first()
        )

        if best_match is None:
            raise self.model.DoesNotExist(not_found_msg)

        return best_match
