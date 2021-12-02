from django.contrib.auth import get_user_model

from importo.models import LegacyModelMixin

from .base import BaseFinder
from .lookup_options import ModelFieldLookupOption

User = get_user_model()


class UserFinder(BaseFinder):
    """
    A finder class that can be used to find Django users
    by their LEGACY_ID_FIELD value (if using `LegacyModelMixin`)
    or username.
    """
    model = User
    cache_lookup_failures = True

    lookup_options = []
    if issubclass(User, LegacyModelMixin):
        lookup_options.append(
            ModelFieldLookupOption(User.LEGACY_ID_FIELD),
        )
    lookup_options.append(
        ModelFieldLookupOption(User, User.USERNAME_FIELD),
    )
