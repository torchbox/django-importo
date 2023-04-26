import uuid

from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _

from importo.utils.uri import extract_host_and_path


class BaseImportedEntity(models.Model):
    """
    An abstract base model for storing details about entities that have been
    imported and converted to Django model instances.
    """

    original_id = models.CharField(
        verbose_name=_("original ID"),
        max_length=255,
        db_index=True,
    )
    content_cleanup_required = models.BooleanField(
        verbose_name=_("content cleanup required"),
        default=False,
        help_text=_(
            "This is set to True to indicate when newly imported 'raw' content needs "
            "to go through a cleanup/conversion process before the import can be classed "
            "as 'complete' for this object. Once completed, this flag is set to 'False'."
        ),
    )
    last_imported = models.DateTimeField(
        verbose_name=_("last imported at"), null=True, editable=False
    )
    last_cleanup = models.DateTimeField(
        verbose_name=_("last cleanup applied at"), null=True, editable=False
    )
    disable_updates_to_object = models.BooleanField(
        verbose_name=_("disable updates to target object"),
        default=False,
        help_text=_(
            "Set this to True to ignore this item when encountered in future "
            "imports. This can be set in cases where an object initially created "
            "via import has become the 'canonical' version, and should no "
            "longer be updated to reflect the original."
        ),
    )

    created = models.DateTimeField(verbose_name=_("created"), auto_now_add=True)
    last_updated = models.DateTimeField(verbose_name=_("last updated"), auto_now=True)

    class Meta:
        abstract = True


class BaseImportedURIEntity(BaseImportedEntity):
    """
    An extension of `BaseImportedEntity` for storing details about
    imported entities that are often (or exclusively) referred to via a
    URI instead of ID (e.g. pages, documents, images).
    """

    # The ID might not be available (e.g. if importing a document linked to
    # from HTML content), so needs to be optional
    original_id = models.CharField(
        verbose_name=_("original ID"),
        max_length=150,
        db_index=True,
        null=True,
    )

    # The host and path are stored separately for improved lookup performance
    original_host = models.CharField(verbose_name=_("original host"), max_length=255, db_index=True)
    original_path = models.CharField(verbose_name=_("original path"), max_length=255, db_index=True)

    # Full URIs are too long to reliably apply unique restraints to, so
    # we create a hash in full_clean() and apply the constraint to that
    original_uri_hash = models.UUIDField(unique=True)

    class Meta:
        abstract = True

    def __init__(self, *args, original_uri: str = None, **kwargs):
        """
        Overrides Model.__init__() to support initialization with
        an 'original_uri' value.
        """
        super().__init__(*args, **kwargs)
        if original_uri:
            self.original_uri = original_uri

    @property
    def original_uri(self) -> str:
        return self.origin_host + self.origin_path

    @original_uri.setter
    def original_uri(self, uri: str) -> None:
        """
        When set, split the URI string into separate `host` and `path`
        values for storage.
        """
        host, path = extract_host_and_path(uri.strip())
        self.original_host = host
        self.original_path = path

    @staticmethod
    def _generate_uri_hash(uri: str) -> uuid.UUID:
        return uuid.uuid5(uuid.UUID("4ccf35b6-0e63-49ec-a878-50e680f3ecfc"), uri)

    def save(self, *args, clean: bool = True, **kwargs) -> None:
        if clean:
            self.full_clean()
        super().save(*args, **kwargs)

    def full_clean(self, *args, **kwargs) -> None:
        # Convert host to lowercase
        if self.original_host:
            self.original_host = self.original_host.lower()

        # Generate hash from host and path values
        if self.original_host and self.original_path:
            self.original_uri_hash = self._generate_uri_hash(self.original_uri)

        # Continue with typical validation
        super().full_clean(*args, **kwargs)


class ImportedUser(BaseImportedEntity):
    """
    A concrete model for storing details about imported users.

    Lookups on
    """
    object = models.OneToOneField(settings.AUTH_USER_MODEL, related_name="import_record")
