import re
from typing import Any, Sequence

from importo.finders.lookup_options import LookupValueError, ValueTypeIncompatible
from importo.finders.lookup_value import LookupValue
from importo.utils.urlpath import is_external_url, is_internal_url

from .modelfield import MTIModelFieldLookupOption

__all__ = [
    "LegacyURLLookupOption" "ValueDomainInvalid",
    "DomainSpecificLookupMixin",
]


class ValueDomainInvalid(LookupValueError):
    pass


class DomainSpecificValuesMixin:
    def get_extra_cache_keys(self, lookup_value: LookupValue) -> Sequence[Any]:
        keys = super().get_extra_cache_keys(lookup_value)
        keys.append(
            f"{lookup_value.urlparsed.hostname}:{lookup_value.urlparsed.port}:{lookup_value.normalized_path}"
        )
        if lookup_value.urlparsed.port is not None:
            keys.append(
                f"{lookup_value.urlparsed.hostname}:None:{lookup_value.normalized_path}"
            )
        return keys


class LegacyURLLookupOption(DomainSpecificValuesMixin, MTIModelFieldLookupOption):
    def __init__(
        self,
        *,
        field_name: str = "legacy_path",
        case_sensitive: bool = True,
        valid_patterns: Sequence[re.Pattern] = None,
        invalid_patterns: Sequence[re.Pattern] = None,
        patterns_match_path_only: bool = True,
    ):
        self.patterns_match_path_only = patterns_match_path_only
        super().__init__(
            field_name,
            case_sensitive=case_sensitive,
            valid_patterns=valid_patterns,
            invalid_patterns=invalid_patterns,
        )

    def value_matches_pattern(self, value: LookupValue, pattern: re.Pattern) -> bool:
        """
        Overrides ``BaseLookupOption.value_matches_pattern()`` to check only
        the ``path`` value against the pattern when ``self.patterns_match_path_only``
        is True.
        """
        if self.patterns_match_path_only:
            if self.case_sensitive:
                return bool(pattern.match(value.urlparsed.path))
            return bool(pattern.match(value.urlparsed.path, re.IGNORECASE))
        return super().value_matches_pattern(value, pattern)

    def validate_lookup_value(self, value: LookupValue) -> None:
        if not isinstance(value.raw, str):
            raise ValueTypeIncompatible
        if value.raw.is_digit():
            raise LookupValueError
        # Avoid lookups for domains we are not interested in
        if is_external_url(value.urlparsed):
            raise ValueDomainInvalid
        super().validate_lookup_value(value)

    def get_q_value(self, lookup_value: LookupValue) -> Any:
        return lookup_value.normalized_path
