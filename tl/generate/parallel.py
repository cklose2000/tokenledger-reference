"""Bounded, deterministic preparation workers. Workers never open a database."""

from collections import deque
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

from tl.generate.synthetic import SyntheticWorld
from tl.stream.events import Catalog, ValidationError
from tl.stream.store import prepare_row


def customer_block(config, start, stop, directory, expected_schema):
    catalog = Catalog(directory)
    if catalog.digest != expected_schema:
        raise ValidationError("activity registry changed during generation")
    world = SyntheticWorld(config)
    rows = []
    for index in range(start, stop):
        for event, arrival in world.customer_events(index):
            rows.append(prepare_row(event, arrival, catalog, source=world.prefix, actor="synthetic-generator", lane="sim"))
    return rows, world.counts, world.amounts, world.period_counts, world.period_amounts, world.invoices, world.late_invoices, world.energy, world.created


def prepared_world(world, catalog, *, workers, progress=None):
    if world._consumed:
        raise ValidationError("create a new world to reproduce a generation")
    if not 1 <= workers <= 8:
        raise ValidationError("workers must be between 1 and 8")
    world._consumed = True
    for event, arrival in world.model_events():
        yield prepare_row(event, arrival, catalog, source=world.prefix, actor="synthetic-generator", lane="sim")
    block_size = 128
    blocks = [(world.config, start, min(start + block_size, world.config.customers), catalog.directory.resolve(), catalog.digest)
              for start in range(0, world.config.customers, block_size)]

    def combine(result):
        rows, counts, amounts, period_counts, period_amounts, invoices, late, energy, created = result
        world.counts.update(counts)
        for key, value in amounts.items():
            world.amounts[key] += value
        world.period_counts.update(period_counts)
        for key, value in period_amounts.items():
            world.period_amounts[key] += value
        world.invoices += invoices
        world.late_invoices += late
        world.energy += energy
        world.created.update(created)
        return rows

    if workers == 1 or len(blocks) == 1:
        for block in blocks:
            yield from combine(customer_block(*block))
            if progress:
                progress(block[2])
    else:
        # Spawn on every OS so tests cover Windows semantics. At most 2*workers
        # blocks are in flight; eager Executor.map would retain an entire world.
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
            pending = deque()
            iterator = iter(blocks)
            for _ in range(workers * 2):
                block = next(iterator, None)
                if block is not None:
                    pending.append((block, pool.submit(customer_block, *block)))
            while pending:
                block, future = pending.popleft()
                yield from combine(future.result())
                if progress:
                    progress(block[2])
                following = next(iterator, None)
                if following is not None:
                    pending.append((following, pool.submit(customer_block, *following)))
    for event, arrival in world.mergers():
        yield prepare_row(event, arrival, catalog, source=world.prefix, actor="synthetic-generator", lane="sim")
    for event, arrival in world.compute_events():
        yield prepare_row(event, arrival, catalog, source=world.prefix, actor="synthetic-generator", lane="sim")
