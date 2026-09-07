"""Half-open local calendar windows with explicit UTC boundaries."""

from datetime import date,datetime,time
from zoneinfo import ZoneInfo,ZoneInfoNotFoundError
from tl.stream.events import UTC,ValidationError,iso


def window(start,end,timezone='UTC'):
    try:
        zone=ZoneInfo(timezone)
        first,last=date.fromisoformat(start),date.fromisoformat(end)
        if first>=last:
            raise ValueError()
        bounds=[datetime.combine(d,time(),zone) for d in (first,last)]
        if any(b.astimezone(UTC).astimezone(zone)!=b for b in bounds):
            raise ValueError()
    except (ValueError,TypeError,ZoneInfoNotFoundError):
        raise ValidationError('usage window requires ordered calendar dates and a valid timezone') from None
    return dict(start=start,end=end,timezone=timezone,start_utc=iso(bounds[0].astimezone(UTC)),
                end_utc=iso(bounds[1].astimezone(UTC)))
