# Performance Notes

## FDB Raw Write Throughput

Measured with `fdb-bench` (standalone binary, no gRPC/schema/trigger overhead).
Fixed 64-byte JSON value, keys packed as `("bench", idx)`, localhost single-node FDB.

| batch_size | puts/sec | p50 commit | p99 commit |
|------------|----------|------------|------------|
| 1          | ~200     | ~5ms       | ~10ms      |
| 1,000      | ~85K     | ~11ms      | ~18ms      |
| 10,000     | ~480K    | ~20ms      | ~24ms      |

Key observations:

- **~5ms is the per-transaction floor** even on localhost. Every FDB commit pays the
  full sequencer → commit proxy → transaction log round-trip.
- **p99 ≈ 2× p50 at small batch sizes** — the FDB proxy's internal batch window
  occasionally makes two consecutive commits land in the same window.
- **Commit latency grows sub-linearly with batch size** — 10× more keys costs ~2×
  more commit time, so throughput scales super-linearly up to the mutation-size limit.
- **Warmup effect at large batches** — at batch_size=10,000 (5 commits total), run 1
  is ~35% slower than run 5 as the GRV cache and proxy connections warm up.
- **Sweet spot: ~1,000–5,000 keys/transaction** — most of the throughput gain over
  batch_size=1 with more predictable latency than 10,000+.

Run the benchmark:

```bash
cargo run --release --bin fdb-bench --features bench/fdb
cargo run --release --bin fdb-bench --features bench/fdb -- --batch-size 1
cargo run --release --bin fdb-bench --features bench/fdb -- --batch-size 100
cargo run --release --bin fdb-bench --features bench/fdb -- --batch-size 1000 --tuples 50000
```

---

## Batch Writer Design and Bottlenecks

The `BatchWriter` in `crates/server/src/batch_writer.rs` coalesces tuple writes
into batches before sending to FDB. The run loop is:

```
1. wait up to BATCH_TIMEOUT for first item
2. greedily drain channel up to BATCH_SIZE (try_recv, no waiting)
3. fdb commit (put_batch)
4. acquire playbook lock → evaluate triggers for every tuple in batch
5. fire trigger events
6. notify guaranteed-write callers
7. goto 1
```

Current constants: `BATCH_SIZE = 10`, `BATCH_TIMEOUT = 2ms`.

### Channel capacity = BATCH_SIZE

The mpsc channel is created with `capacity = BATCH_SIZE`. This means no-guarantee
puts (`try_send`) return instantly while the queue has room, then block once full.
The effective client behaviour with larger batch sizes would be:

- Puts 1–N: instant (queue has room)
- Put N+1: blocks until batch_writer drains and commits (~11ms for N=1,000)
- Pattern repeats → average throughput ≈ N / commit_time

### The 2ms timeout at high load

At high throughput the queue fills and drains before the 2ms timer fires — the
timeout is effectively bypassed. It only matters at low/idle load, where it
ensures partial batches are flushed promptly rather than waiting indefinitely.

### Trigger matching is the hidden post-commit cost

Steps 4–5 above run **synchronously after the FDB commit**, adding to the dead
time before the next batch can start draining. Cost is O(batch_size × N_triggers).
At 50 triggers and batch_size=1,000 this is negligible (~0.5ms). At 500 triggers
or batch_size=10,000 it becomes meaningful.

---

## Pipelining Opportunity (deferred)

Predicate evaluation is pure CPU work with no ordering constraint relative to the
FDB commit. Only *firing* events must happen after a successful commit.

The restructured loop would be:

```
snapshot all_triggers once at batch-cycle start

receive items (up to BATCH_SIZE or BATCH_TIMEOUT):
  for each item as it arrives:
    evaluate triggers → store Vec<TriggerFiredEvent> on item
    advance high_water_mark

when buffer full / timeout fires:
  kick off fdb commit (async — suspends here)
  concurrently: match triggers for items above high_water_mark

await commit result
if ok: fire all pre-computed events
notify callers
```

Two pipelining opportunities:

1. **During buffer fill** — at moderate load, items trickle in over the 2ms window.
   Trigger evaluation on each arriving item is free, hidden behind the receive wait.

2. **During FDB commit** — remaining unmatched items (those that arrived just before
   flush) get evaluated while the ~11ms FDB commit is in flight. This is the larger
   structural win.

At 50 triggers × 1,000 tuples, evaluation finishes in <0.5ms and easily hides
inside the 11ms commit window. The optimisation becomes meaningful at ~500+ triggers
or when commit latency drops significantly (larger FDB cluster).

**Playbook lock strategy:** snapshot `all_triggers` once at the start of each batch
cycle rather than locking per-item. Correct because playbook changes are rare, and
avoids repeated lock contention inside the tight evaluation loop.

**Why deferred:** the added complexity (pipelining state, pre-computed event storage,
concurrent futures) is not justified at current trigger volumes. Revisit when
O(batch_size × N_triggers) shows up in profiling.
