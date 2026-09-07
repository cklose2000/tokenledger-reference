"""Deterministic source events, with dollars computed in integer cents.

No KPI is generated here. Commercial documents and separately linked recognition
events allow later independent accounting calculations and reconciliation.
"""

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
import hashlib
import math

import numpy as np

from tl.generate.allocation import pinned_coefficients
from tl.stream.events import Activity, UTC, ValidationError, canonical

SEGMENTS = ("enterprise", "mid_market_api", "startup_api", "marketplace",
            "consumer_subscription", "team_subscription", "claude_code_seats")
CHANNELS = ("aws_marketplace", "gcp_marketplace", "azure_marketplace")
TOKEN_TYPES = ("input", "output", "cache_read", "cache_write")
PRICES_MICRO = (3_000_000, 12_000_000, 300_000, 3_750_000)
ENERGY = (0.02, 0.08, 0.003, 0.025)  # Fictional MWh / million tokens.
PLANS = ("pro", "max", "team", "enterprise")
SEAT_PRICE_CENTS = (2000, 6000, 3500, 8000)
GENERATOR_VERSION = "synthetic-v1.1"


def month_add(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def dt(value: date) -> datetime:
    return datetime.combine(value, time(), UTC)


def dollars(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}{abs(cents) // 100}.{abs(cents) % 100:02d}"


def rounded(numerator: int, denominator: int) -> int:
    return (numerator + denominator // 2) // denominator


@dataclass(frozen=True)
class Config:
    seed: int = 42
    months: int = 30
    customers: int = 50000
    start: str = "2024-02-01"
    profile: str = "default"

    def __post_init__(self):
        if not (0 <= self.seed < 2**32 and 1 <= self.months <= 120 and 1 <= self.customers <= 1_000_000):
            raise ValidationError("seed, months or customers out of supported range")
        if date.fromisoformat(self.start).day != 1:
            raise ValidationError("start must be the first day of a month")
        if self.profile not in ("default", "anthropic-like"):
            raise ValidationError("unknown synthetic profile")

    @property
    def run_id(self):
        return hashlib.sha256(canonical({"generator": GENERATOR_VERSION, **asdict(self)}).encode()).hexdigest()[:20]


class SyntheticWorld:
    def __init__(self, config: Config):
        self.coefficients_nano = pinned_coefficients(energy=ENERGY, token_types=TOKEN_TYPES)
        self.config = config
        self.start = date.fromisoformat(config.start)
        self.months = [month_add(self.start, i) for i in range(config.months + 1)]
        self.period_labels = {(value.year, value.month): f"{value.year:04d}-{value.month:02d}" for value in self.months}
        self.prefix = "sim:" + config.run_id
        self.counts = Counter()
        self.amounts = defaultdict(int)
        self.period_counts = Counter()
        self.period_amounts = defaultdict(int)
        self.late_invoices = 0
        self.invoices = 0
        self.energy = np.zeros((config.months, 5), dtype=np.int64)  # nano-MWh, additive across workers
        self.created = {}
        self._consumed = False

    def emit(self, key, activity, when, features, customer="internal:synthetic-lab", *,
             arrived=None, revenue=None, link=None):
        event = Activity(self.prefix + ":" + key, when, activity, features, customer,
                         revenue_impact=None if revenue is None else dollars(revenue), link=link)
        self.counts[activity] += 1
        period = self.period_labels[(when.year, when.month)]
        self.period_counts[(period, activity)] += 1
        if revenue is not None:
            self.amounts[activity] += revenue
            self.period_amounts[(period, activity)] += revenue
        return event, arrived or when

    def recognition(self, key, when, customer, cents, source, document, kind, channel, *, arrived=None):
        return self.emit(key, "revenue_recognized", when,
                         dict(period=when.date().replace(day=1).isoformat(), net_usd=dollars(cents),
                              source=source, source_document=document, recognition_kind=kind, channel=channel),
                         customer, revenue=cents, arrived=arrived, link=document)

    def events(self, progress=None):
        if self._consumed:
            raise ValidationError("create a new world to reproduce a generation")
        self._consumed = True
        yield from self.model_events()
        for index in range(self.config.customers):
            yield from self.customer_events(index)
            if progress and (index + 1) % 1000 == 0:
                progress(index + 1)
        yield from self.mergers()
        yield from self.compute_events()

    def model_events(self):
        for gen in range(5):
            release = month_add(self.start, gen * 6)
            if release >= self.months[-1]:
                break
            model = f"synthetic-g{gen + 1}"
            yield self.emit(f"model:{gen}:release", "model_released", dt(release),
                            dict(model=model, generation=gen + 1, release_date=release.isoformat(),
                                 training_cost_usd_estimate=dollars(100_000_000 * 3**gen)))
            if gen:
                deprecated = release + timedelta(days=120)
                if deprecated < self.months[-1]:
                    yield self.emit(f"model:{gen-1}:deprecated", "model_deprecated", dt(deprecated),
                                    dict(model=f"synthetic-g{gen}", replacement_model=model))

    def customer_events(self, index):
        rng = np.random.default_rng(np.random.SeedSequence([self.config.seed, index]))
        weights = (0.08, 0.14, 0.18, 0.16, 0.22, 0.14, 0.08)
        if self.config.profile == "anthropic-like":
            weights = (0.15, 0.20, 0.20, 0.20, 0.10, 0.08, 0.07)
        segment = index if index < 7 else int(rng.choice(7, p=weights))
        cohort = 0 if index < 7 else int(rng.integers(0, self.config.months))
        customer = f"customer:{index:07d}"
        self.created[customer] = cohort
        channel = str(rng.choice(CHANNELS)) if segment == 3 else "direct"
        first = dt(self.months[cohort])
        yield self.emit(f"{index}:created", "customer_created", first,
                        dict(segment=SEGMENTS[segment], channel=channel, country=str(rng.choice(["US", "CA", "GB", "DE"])),
                             parent_customer=None), customer)
        if segment < 4:
            yield from self.usage_events(index, customer, segment, cohort, channel, rng)
        else:
            yield from self.subscription_events(index, customer, segment, cohort, rng)

    def usage_events(self, index, customer, segment, cohort, channel, rng):
        base_tokens = float(rng.lognormal((20.8, 18.5, 16.5, 18.0)[segment], 1.35))
        discount_bp = int(rng.integers(1000, 4001)) if segment == 0 else int(rng.integers(0, 1001))
        fee_bp = int(rng.integers(300, 801)) if segment == 3 else 0
        migration_days = int(rng.integers(60, 121))
        factor = 1.0
        remaining = 0
        contract_id = None
        workload = str(rng.choice(["api", "claude_code", "agent_platform"], p=[0.7, 0.15, 0.15]))
        for month in range(cohort, self.config.months):
            start, stop = self.months[month:month + 2]
            when = dt(stop) - timedelta(microseconds=1)
            tenure = month - cohort
            key = f"{index}:{month}"
            if tenure and rng.random() < (0.003, 0.008, 0.020, 0.012)[segment] / (1 + tenure / 24):
                yield self.emit(key + ":churn", "customer_churned", dt(start), dict(reason="synthetic_tenure_hazard"), customer)
                if contract_id:
                    yield self.emit(key + ":expired", "contract_expired", dt(start),
                                    dict(contract_id=contract_id, unconsumed_commit_usd=dollars(remaining), reason="closed_account"), customer)
                    if remaining:
                        yield self.recognition(key + ":breakage", dt(start), customer, remaining,
                                               "commit_drawdown", contract_id, "commit_expiry", channel)
                break
            if segment == 0 and tenure % 12 == 0:
                if contract_id:
                    yield self.emit(key + ":expired", "contract_expired", dt(start),
                                    dict(contract_id=contract_id, unconsumed_commit_usd=dollars(remaining), reason="term_end"), customer)
                    if remaining:
                        yield self.recognition(key + ":breakage", dt(start), customer, remaining,
                                               "commit_drawdown", contract_id, "commit_expiry", channel)
                contract_id = f"contract:{index}:{tenure // 12}"
                remaining = max(1, int(base_tokens / 1e6 * 4.75 * (1 - discount_bp / 10000) * 12 * 0.8 * 100))
                yield self.emit(key + ":contract", "contract_signed" if tenure == 0 else "contract_renewed", dt(start),
                                dict(contract_id=contract_id, term_months=12, commit_usd=dollars(remaining),
                                     discount_pct=discount_bp / 10000, channel=channel,
                                     start=start.isoformat(), end=month_add(start, 12).isoformat()), customer)
            generation = min(4, month // 6)
            if generation and when < dt(month_add(self.start, generation * 6) + timedelta(days=migration_days)):
                generation -= 1
            model = f"synthetic-g{generation + 1}"
            release = month_add(self.start, generation * 6).isoformat()
            if rng.random() < 0.06:
                factor *= float(rng.choice([0.4, 2.0]))
                factor = max(0.1, min(factor, 8.0))
            seasonal = 1 + 0.12 * math.sin(2 * math.pi * (start.month - 1) / 12)
            total = max(100, int(base_tokens * factor * math.exp((0.035, 0.025, 0.015, 0.03)[segment] * tenure)
                                 * seasonal * rng.lognormal(0, 0.35)))
            split = rng.multinomial(total, [0.50, 0.25, 0.20, 0.05])
            batch_id, invoice_id = f"usage:{index}:{month}", f"invoice:{index}:{month}"
            gross = net = 0
            for token_type, token_count, list_micro, coefficient_nano in zip(
                    TOKEN_TYPES, split, PRICES_MICRO, self.coefficients_nano[generation]):
                tokens = int(token_count)
                if not tokens:
                    continue
                price_micro = round(list_micro * 0.82**generation)
                gross_line = rounded(tokens * price_micro, 10_000_000_000)
                net_line = rounded(gross_line * (10000 - discount_bp), 10000)
                gross += gross_line
                net += net_line
                self.energy[month, generation] += rounded(tokens * coefficient_nano, 1_000_000)
                yield self.emit(key + ":tokens:" + token_type, "tokens_processed", when,
                                dict(usage_batch_id=batch_id, model=model, model_release_date=release, token_type=token_type,
                                     tokens=tokens, list_price_per_mtok=price_micro / 1e6,
                                     effective_price_per_mtok=round(price_micro / 1e6 * (1 - discount_bp / 10000), 9),
                                     channel=channel, workload=workload), customer)
            fee = rounded(net * fee_bp, 10000)
            drawdown = min(net, remaining)
            remaining -= drawdown
            late = rng.random() < 0.055
            arrival = when + timedelta(days=int(rng.integers(5, 41))) if late else when
            self.invoices += 1
            self.late_invoices += int(late)
            yield self.emit(key + ":invoice", "usage_invoiced", when,
                            dict(invoice_id=invoice_id, usage_batch_id=batch_id, contract_id=contract_id,
                                 period_start=start.isoformat(), period_end=stop.isoformat(),
                                 gross_usd=dollars(gross), discount_usd=dollars(gross - net), net_usd=dollars(net),
                                 channel_fee_usd=dollars(fee), commit_applied_usd=dollars(drawdown),
                                 cash_due_usd=dollars(net - drawdown), channel=channel), customer, arrived=arrival)
            if drawdown:
                yield self.recognition(key + ":drawdown", when, customer, drawdown,
                                       "commit_drawdown", invoice_id, "usage", channel, arrived=arrival)
            if net - fee - drawdown:
                yield self.recognition(key + ":revenue", when, customer, net - fee - drawdown,
                                       "usage", invoice_id, "usage", channel, arrived=arrival)
            if rng.random() < 0.015 and net - fee > 100:
                credit = max(1, rounded((net - fee) * 5, 100))
                credit_id = f"credit:{index}:{month}"
                credit_arrival = arrival + timedelta(days=10)
                yield self.emit(key + ":credit", "credit_issued", when,
                                dict(credit_id=credit_id, invoice_id=invoice_id, period=start.isoformat(),
                                     amount_usd=dollars(credit), reason="one_time_service_credit"), customer, arrived=credit_arrival)
                yield self.recognition(key + ":credit-revenue", when, customer, -credit,
                                       "usage", credit_id, "credit", channel, arrived=credit_arrival)

    def subscription_events(self, index, customer, segment, cohort, rng):
        plan_index = {4: 0, 5: 2, 6: 3}[segment]
        seats = 1 if segment == 4 else int(rng.integers(3, 25))
        trial_id, subscription_id = f"trial:{index}", f"subscription:{index}"
        conversion = self.months[cohort] + timedelta(days=14)
        yield self.emit(f"{index}:trial", "trial_started", dt(self.months[cohort]),
                        dict(trial_id=trial_id, plan=PLANS[plan_index], seats=seats, end=conversion.isoformat()), customer)
        if rng.random() < 0.25:
            yield self.emit(f"{index}:trial-expired", "trial_expired", dt(conversion),
                            dict(trial_id=trial_id, reason="not_converted"), customer)
            return
        yield self.emit(f"{index}:converted", "trial_converted", dt(conversion),
                        dict(trial_id=trial_id, subscription_id=subscription_id, plan=PLANS[plan_index], seats=seats), customer)
        for month in range(cohort, self.config.months):
            start, stop = self.months[month:month + 2]
            days = (stop - start).days
            tenure = month - cohort
            key = f"{index}:{month}"
            if tenure and rng.random() < (0.025 if segment == 4 else 0.015) / (1 + tenure / 24):
                yield self.emit(key + ":cancelled", "subscription_cancelled", dt(start),
                                dict(subscription_id=subscription_id, plan=PLANS[plan_index], seats=seats,
                                     reason="synthetic_tenure_hazard"), customer)
                yield self.emit(key + ":churn", "customer_churned", dt(start), dict(reason="subscription_cancelled"), customer)
                break
            effective = conversion if tenure == 0 else start
            features = dict(subscription_id=subscription_id, plan=PLANS[plan_index], seats=seats,
                            price_per_seat=dollars(SEAT_PRICE_CENTS[plan_index]), billing_period="monthly",
                            period_start=effective.isoformat(), period_end=stop.isoformat())
            yield self.emit(key + ":subscription", "subscription_started" if tenure == 0 else "subscription_renewed",
                            dt(effective), features, customer)
            old_seats, old_price = seats, SEAT_PRICE_CENTS[plan_index]
            change_day = start + timedelta(days=9) if tenure else effective
            if tenure and rng.random() < 0.08:
                # Consumer plans switch within consumer; team plans within team.
                lower, upper = (0, 1) if segment == 4 else (2, 3)
                previous = plan_index
                plan_index = upper if plan_index == lower else lower
                yield self.emit(key + ":plan", "subscription_upgraded" if plan_index > previous else "subscription_downgraded",
                                dt(change_day), {**features, "plan": PLANS[plan_index],
                                                "price_per_seat": dollars(SEAT_PRICE_CENTS[plan_index]),
                                                "period_start": change_day.isoformat()}, customer)
            if tenure and segment != 4 and rng.random() < 0.20:
                delta = int(rng.integers(1, 5)) * (1 if rng.random() < 0.7 else -1)
                delta = max(1 - seats, delta)
                if delta:
                    seats += delta
                    yield self.emit(key + ":seats", "seat_added" if delta > 0 else "seat_removed", dt(change_day),
                                    dict(subscription_id=subscription_id, plan=PLANS[plan_index], seats=abs(delta), seats_after=seats), customer)
            before_days = (change_day - effective).days
            after_days = (stop - change_day).days
            earned = rounded(old_price * old_seats * before_days + SEAT_PRICE_CENTS[plan_index] * seats * after_days, days)
            yield self.recognition(key + ":revenue", dt(stop) - timedelta(microseconds=1), customer, earned,
                                   "subscription", subscription_id, "subscription", "direct")

    def mergers(self):
        # Upper IDs always merge into lower IDs; no cycles or chains created here.
        for month in range(11, self.config.months, 6):
            eligible = sorted(customer for customer, cohort in self.created.items() if cohort <= month)
            if len(eligible) >= 14:
                child = eligible[-(month // 6 + 1)]
                yield self.emit(f"merger:{month}", "customer_merged", dt(self.months[month]) + timedelta(days=15),
                                dict(ultimate_parent=eligible[0], reason="synthetic_acquisition"), child)

    def compute_events(self):
        providers = (("aws_trainium", "us-east", 0.45, 6800),
                     ("google_tpu", "us-central", 0.35, 5700),
                     ("nvidia_cloud", "us-west", 0.20, 8100))
        for provider, region, share, cents_per_mwh in providers:
            for purpose in ("inference", "training"):
                amounts = self.energy.sum(axis=1) / 1e9 * share
                if purpose == "training":
                    amounts = amounts * 0.35 + 50
                capacity = max(0.1, float(max(amounts)) / (28 * 24) * 4)
                capacity_id = f"capacity:{provider}:{purpose}"
                yield self.emit(capacity_id, "capacity_contracted", dt(self.start),
                                dict(capacity_id=capacity_id, provider=provider, region=region, mw=round(capacity, 9),
                                     usd_per_mwh=dollars(cents_per_mwh), start=self.start.isoformat(),
                                     end=self.months[-1].isoformat(), purpose=purpose))
                for step, ramp in enumerate((0.35, 0.70, 1.0)):
                    if step < self.config.months:
                        yield self.emit(capacity_id + f":energized:{step}", "capacity_energized", dt(self.months[step]),
                                        dict(capacity_id=capacity_id, provider=provider, region=region, mw=round(capacity * ramp, 9)))
                for month in range(self.config.months):
                    start, stop = self.months[month:month + 2]
                    for generation in range(5):
                        mwh = self.energy[month, generation] / 1e9 * share
                        if purpose == "training":
                            if generation != min(4, month // 6):
                                continue
                            mwh = amounts[month]
                        if mwh <= 0:
                            continue
                        yield self.emit(capacity_id + f":{month}:{generation}", "capacity_consumed", dt(stop) - timedelta(microseconds=1),
                                        dict(capacity_id=capacity_id, provider=provider, region=region, mwh=round(float(mwh), 9),
                                             purpose=purpose, model=f"synthetic-g{generation + 1}",
                                             allocation_basis="energy_coefficients_v1" if purpose == "inference" else "training_meter",
                                             period_start=start.isoformat(), period_end=stop.isoformat()))

    def manifest(self):
        return {"schema_version": "synthetic-source-manifest/v1", "generator_version": GENERATOR_VERSION,
                "run_id": self.config.run_id, "synthetic": True, "config": asdict(self.config),
                "source": self.prefix, "activity_counts": dict(sorted(self.counts.items())),
                "revenue_cents": dict(sorted(self.amounts.items())),
                "invoice_count": self.invoices, "late_invoice_count": self.late_invoices,
                "periods": [{"period": period, "activity": activity, "count": count,
                             "revenue_cents": self.period_amounts[(period, activity)]}
                            for (period, activity), count in sorted(self.period_counts.items())]}
