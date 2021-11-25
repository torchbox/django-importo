from datetime import datetime, timedelta

from django.conf import settings
from django.utils.timezone import get_current_timezone


def timestamp_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(
        value, tz=get_current_timezone() if settings.USE_TZ else None
    )


def humanize_timedelta(value: timedelta) -> str:
    seconds = value.seconds

    hours = seconds / 3600
    if hours > 1:
        return "%.1f hours" % hours

    minutes = seconds // 60
    if minutes > 1:
        return f"{minutes+1} minutes"

    return f"{seconds} seconds"
