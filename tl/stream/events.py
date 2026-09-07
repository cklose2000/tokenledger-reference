"""The sole typed business-event envelope. No warehouse-specific semantics."""

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import json
import math
import re
import hashlib
from pathlib import Path

import yaml

UTC = timezone.utc
CENT = Decimal("0.01")
MONEY_LIMIT = Decimal("1e16")


class ValidationError(ValueError):
    pass


def timestamp(value: str | datetime) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if not isinstance(parsed, datetime) or parsed.tzinfo is None:
            raise ValueError("timestamp requires an explicit timezone")
        return parsed if parsed.tzinfo is UTC else parsed.astimezone(UTC)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"invalid UTC timestamp: {value!r}") from exc


def iso(value: datetime) -> str:
    return timestamp(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def money(value) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValidationError(f"invalid money: {value!r}; use exact decimal text or integer")
    return _money(value)


@lru_cache(maxsize=16384)
def _money(value) -> Decimal:
    try:
        result = Decimal(value)
        if not result.is_finite() or result != result.quantize(CENT) or abs(result) >= MONEY_LIMIT:
            raise ValueError("money requires cents and fits DECIMAL(18,2)")
        return Decimal("0.00") if result == 0 else result
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationError(f"invalid money: {value!r}") from exc


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


@dataclass(frozen=True, slots=True)
class Activity:
    activity_id: str
    ts: str | datetime
    activity: str
    feature_json: dict = field(default_factory=dict)
    customer: str | None = None
    anonymous_customer_id: str | None = None
    revenue_impact: str | int | Decimal | None = None
    link: str | None = None


class Catalog:
    """Compile the closed YAML catalog once, then validate every emission."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.schemas = {}
        self.fields = {}
        digest = hashlib.sha256()
        for path in sorted(directory.glob("*.yaml")):
            digest.update(path.name.encode() + b"\0" + path.read_bytes().replace(b"\r\n", b"\n"))
            schema = yaml.safe_load(path.read_text(encoding="utf-8"))
            if schema["name"] != path.stem or schema["name"] in self.schemas:
                raise ValidationError(f"invalid activity schema name: {path}")
            self.schemas[schema["name"]] = schema
            self.fields[schema["name"]] = [(name, spec["type"], spec.get("required", True), spec.get("nullable", False),
                                            spec.get("enum"), spec.get("min"), spec.get("max"))
                                           for name, spec in schema["features"].items()]
        if not self.schemas:
            raise ValidationError(f"no activity schemas in {directory}")
        self.digest = digest.hexdigest()

    def validate(self, event: Activity) -> dict:
        if not isinstance(event.activity, str) or event.activity not in self.schemas:
            raise ValidationError(f"unknown activity: {event.activity}")
        for name, value in (("activity_id", event.activity_id), ("customer", event.customer),
                            ("anonymous_customer_id", event.anonymous_customer_id), ("link", event.link)):
            if (name == "activity_id" or value is not None) and (not isinstance(value, str) or not value.strip()):
                raise ValidationError(f"{name} must be a nonempty string")
        schema = self.schemas[event.activity]
        if schema.get("customer_required") and not event.customer:
            raise ValidationError(f"{event.activity} requires an identified customer")
        if event.customer is None and event.anonymous_customer_id is None:
            raise ValidationError("an identified or anonymous customer is required")
        if not isinstance(event.feature_json, dict):
            raise ValidationError("feature_json must be an object")
        if any(not isinstance(name, str) for name in event.feature_json):
            raise ValidationError("feature_json keys must be strings")
        features = dict(event.feature_json)
        fields = schema["features"]
        unknown = features.keys() - fields.keys()
        if unknown:
            raise ValidationError(f"{event.activity}: unknown features {sorted(unknown)}")
        for name, kind, required, nullable, choices, minimum, maximum in self.fields[event.activity]:
            if name not in features:
                if required:
                    raise ValidationError(f"{event.activity}: missing {name}")
                continue
            value = features[name]
            if value is None and nullable:
                continue
            valid = True
            if kind == "string":
                valid = isinstance(value, str) and bool(value.strip())
            elif kind == "integer":
                valid = type(value) is int
            elif kind == "number":
                valid = type(value) in (int, float) and math.isfinite(value)
            elif kind == "money":
                value = money(value)
                features[name] = format(value, ".2f")
            elif kind == "date":
                try:
                    valid = isinstance(value, str) and date.fromisoformat(value).isoformat() == value
                except ValueError:
                    valid = False
            elif kind == 'timestamp':
                value = timestamp(value)
                features[name] = iso(value)
            elif kind == 'sha256':
                valid = isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None
            elif kind == 'decimal':
                try:
                    if isinstance(value,bool) or not isinstance(value,(str,int,Decimal)):
                        raise ValueError()
                    value = Decimal(value)
                    valid = value.is_finite()
                    if valid:
                        features[name] = format(value,'f')
                except (InvalidOperation,ValueError):
                    valid = False
            else:
                raise ValidationError(f"unsupported catalog type: {kind}")
            if not valid:
                raise ValidationError(f"{event.activity}.{name}: expected {kind}")
            if choices is not None and value not in choices:
                raise ValidationError(f"{event.activity}.{name}: invalid choice {value!r}")
            if minimum is not None and value < minimum:
                raise ValidationError(f"{event.activity}.{name}: below minimum")
            if maximum is not None and value > maximum:
                raise ValidationError(f"{event.activity}.{name}: above maximum")
        if "period_start" in features and features["period_start"] > features["period_end"]:
            raise ValidationError("period_start is after period_end")
        if "start" in features and features["start"] > features["end"]:
            raise ValidationError("contract starts after end")
        if 'interval_start' in features and timestamp(features['interval_start']) > timestamp(features['interval_end']):
            raise ValidationError('usage interval starts after its end')
        if event.activity == "usage_invoiced":
            if money(features["gross_usd"]) - money(features["discount_usd"]) != money(features["net_usd"]):
                raise ValidationError("invoice gross minus discount must equal net")
            if money(features["channel_fee_usd"]) > money(features["net_usd"]):
                raise ValidationError("channel fee exceeds net invoice")
            if money(features["commit_applied_usd"]) + money(features["cash_due_usd"]) != money(features["net_usd"]):
                raise ValidationError("invoice commit applied plus cash due must equal net")
            if features["contract_id"] is None and money(features["commit_applied_usd"]) != 0:
                raise ValidationError("commit drawdown requires a contract")
        revenue = None if event.revenue_impact is None else format(money(event.revenue_impact), ".2f")
        if event.activity == "revenue_recognized" and revenue != features["net_usd"]:
            raise ValidationError("recognized revenue_impact must equal net_usd")
        if event.activity == "credit_issued" and (revenue is not None or money(features["amount_usd"]) <= 0):
            raise ValidationError("credits require a positive amount; recognition is a separate linked activity")
        if event.activity != "revenue_recognized" and revenue is not None:
            raise ValidationError("only revenue_recognized carries revenue_impact; other events are not revenue")
        return {"event_ts": timestamp(event.ts), "anonymous_customer_id": event.anonymous_customer_id,
                "feature_json": features, "revenue_impact": revenue, "link": event.link}
