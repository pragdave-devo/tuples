use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tonic::Status;

use proto::tuples::TriggerFiredEvent;
use tuples_core::playbook::ParamSource;
use tuples_core::tuple::Tuple;
use tuples_storage::{PlaybookStore, TupleStore};

// ── Public API ──────────────────────────────────────────────────────────────

pub struct BatchWriter {
    tx: mpsc::Sender<BatchItem>,
}

impl BatchWriter {
    pub fn new(
        store: Arc<dyn TupleStore>,
        playbooks: Arc<Mutex<dyn PlaybookStore>>,
        trigger_tx: broadcast::Sender<TriggerFiredEvent>,
    ) -> Self {
        let config = store.batch_config();
        let (tx, rx) = mpsc::channel(config.batch_size * 2);
        let (completion_tx, completion_rx) = mpsc::channel::<Completion>(4);

        // Persistent IO worker — receives flush jobs, writes to store.
        let (io_tx, io_rx) = mpsc::channel::<FlushJob>(2);
        tokio::spawn(io_worker(io_rx, Arc::clone(&store), completion_tx.clone()));

        // Persistent trigger worker — receives flush jobs, runs trigger matching.
        let (trig_tx, trig_rx) = mpsc::channel::<FlushJob>(2);
        tokio::spawn(trigger_worker(trig_rx, playbooks, trigger_tx, completion_tx));

        tokio::spawn(input_loop(rx, config, io_tx, trig_tx, completion_rx));
        Self { tx }
    }

    /// Submit a tuple for writing.
    ///
    /// - `guaranteed = false`: if the queue has room the call returns immediately before the
    ///   write commits; if the queue is full it blocks until the current batch flushes.
    /// - `guaranteed = true`: always blocks until the write is committed.
    pub async fn put(&self, tuple: Tuple, guaranteed: bool) -> Result<(), Status> {
        if guaranteed {
            let (notify_tx, notify_rx) = oneshot::channel();
            self.tx
                .send(BatchItem { tuple, notify: Some(notify_tx), flush_now: true })
                .await
                .map_err(|_| Status::internal("batch writer closed"))?;
            notify_rx
                .await
                .map_err(|_| Status::internal("batch writer dropped"))?
                .map_err(Status::internal)
        } else {
            let item = BatchItem { tuple, notify: None, flush_now: false };
            match self.tx.try_send(item) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(item)) => {
                    let (notify_tx, notify_rx) = oneshot::channel();
                    let item = BatchItem { notify: Some(notify_tx), ..item };
                    self.tx
                        .send(item)
                        .await
                        .map_err(|_| Status::internal("batch writer closed"))?;
                    notify_rx
                        .await
                        .map_err(|_| Status::internal("batch writer dropped"))?
                        .map_err(Status::internal)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Err(Status::internal("batch writer closed"))
                }
            }
        }
    }
}

// ── Internal types ──────────────────────────────────────────────────────────

struct BatchItem {
    tuple: Tuple,
    notify: Option<oneshot::Sender<Result<(), String>>>,
    flush_now: bool,
}

struct FlushJob {
    buffer_id: usize,
    tuples: Arc<Vec<Tuple>>,
}

/// Tracks a flushed buffer awaiting IO + trigger completion.
struct PendingBuffer {
    notifiers: Vec<Option<oneshot::Sender<Result<(), String>>>>,
    io_done: bool,
    triggers_done: bool,
    io_result: Option<Result<(), String>>,
}

impl PendingBuffer {
    fn try_complete(&mut self) -> bool {
        if self.io_done && self.triggers_done {
            let err_str = self.io_result.take().and_then(|r| r.err());
            for notify in self.notifiers.drain(..) {
                if let Some(tx) = notify {
                    let r = match &err_str {
                        None => Ok(()),
                        Some(e) => Err(e.clone()),
                    };
                    let _ = tx.send(r);
                }
            }
            true
        } else {
            false
        }
    }
}

enum Completion {
    Io { buffer_id: usize, result: Result<(), String> },
    Triggers { buffer_id: usize },
}

// ── Persistent worker tasks ─────────────────────────────────────────────────

async fn io_worker(
    mut rx: mpsc::Receiver<FlushJob>,
    store: Arc<dyn TupleStore>,
    completion_tx: mpsc::Sender<Completion>,
) {
    while let Some(job) = rx.recv().await {
        let result = store.put_batch(&job.tuples).await;
        let _ = completion_tx
            .send(Completion::Io {
                buffer_id: job.buffer_id,
                result: result.map_err(|e| e.to_string()),
            })
            .await;
    }
}

async fn trigger_worker(
    mut rx: mpsc::Receiver<FlushJob>,
    playbooks: Arc<Mutex<dyn PlaybookStore>>,
    trigger_tx: broadcast::Sender<TriggerFiredEvent>,
    completion_tx: mpsc::Sender<Completion>,
) {
    while let Some(job) = rx.recv().await {
        fire_triggers(&playbooks, &trigger_tx, &job.tuples).await;
        let _ = completion_tx
            .send(Completion::Triggers { buffer_id: job.buffer_id })
            .await;
    }
}

// ── Input loop (owns all buffer state) ──────────────────────────────────────

async fn input_loop(
    mut rx: mpsc::Receiver<BatchItem>,
    config: tuples_storage::BatchConfig,
    io_tx: mpsc::Sender<FlushJob>,
    trig_tx: mpsc::Sender<FlushJob>,
    mut completion_rx: mpsc::Receiver<Completion>,
) {
    let mut pending: [Option<PendingBuffer>; 2] = [None, None];
    let mut current: Vec<(Tuple, Option<oneshot::Sender<Result<(), String>>>)> =
        Vec::with_capacity(config.batch_size);
    let mut current_id: usize = 0;
    let mut timer_active = false;

    let sleep = tokio::time::sleep(config.batch_timeout);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            item = rx.recv() => {
                match item {
                    None => {
                        if !current.is_empty() {
                            flush_buffer(current_id, &mut current, &mut pending, &io_tx, &trig_tx);
                        }
                        drain_pending(&mut pending, &mut completion_rx).await;
                        break;
                    }
                    Some(item) => {
                        let flush_now = item.flush_now;
                        if current.is_empty() {
                            sleep.as_mut().reset(tokio::time::Instant::now() + config.batch_timeout);
                            timer_active = true;
                        }
                        current.push((item.tuple, item.notify));

                        if flush_now || current.len() >= config.batch_size {
                            flush_buffer(
                                current_id, &mut current, &mut pending, &io_tx, &trig_tx,
                            );
                            timer_active = false;
                            swap_buffer(
                                &mut current_id, &mut current, &mut pending,
                                &mut completion_rx, config.batch_size,
                            ).await;
                        }
                    }
                }
            }

            _ = &mut sleep, if timer_active && !current.is_empty() => {
                timer_active = false;
                flush_buffer(
                    current_id, &mut current, &mut pending, &io_tx, &trig_tx,
                );
                swap_buffer(
                    &mut current_id, &mut current, &mut pending,
                    &mut completion_rx, config.batch_size,
                ).await;
            }

            Some(c) = completion_rx.recv() => {
                handle_completion(c, &mut pending);
            }
        }
    }
}

// ── Flush + swap helpers ────────────────────────────────────────────────────

/// Move tuples out of the current buffer (zero-copy), wrap in Arc, send to workers.
fn flush_buffer(
    buffer_id: usize,
    current: &mut Vec<(Tuple, Option<oneshot::Sender<Result<(), String>>>)>,
    pending: &mut [Option<PendingBuffer>; 2],
    io_tx: &mpsc::Sender<FlushJob>,
    trig_tx: &mpsc::Sender<FlushJob>,
) {
    let slots = std::mem::take(current);
    let mut tuples = Vec::with_capacity(slots.len());
    let mut notifiers = Vec::with_capacity(slots.len());
    for (tuple, notify) in slots {
        tuples.push(tuple);
        notifiers.push(notify);
    }
    let tuples = Arc::new(tuples);

    pending[buffer_id] = Some(PendingBuffer {
        notifiers,
        io_done: false,
        triggers_done: false,
        io_result: None,
    });

    let _ = io_tx.try_send(FlushJob { buffer_id, tuples: Arc::clone(&tuples) });
    let _ = trig_tx.try_send(FlushJob { buffer_id, tuples });
}

async fn swap_buffer(
    current_id: &mut usize,
    current: &mut Vec<(Tuple, Option<oneshot::Sender<Result<(), String>>>)>,
    pending: &mut [Option<PendingBuffer>; 2],
    completion_rx: &mut mpsc::Receiver<Completion>,
    capacity: usize,
) {
    let alt_id = 1 - *current_id;
    while pending[alt_id].is_some() {
        if let Some(c) = completion_rx.recv().await {
            handle_completion(c, pending);
        }
    }
    *current_id = alt_id;
    *current = Vec::with_capacity(capacity);
}

fn handle_completion(completion: Completion, pending: &mut [Option<PendingBuffer>; 2]) {
    match completion {
        Completion::Io { buffer_id, result } => {
            if let Some(buf) = pending[buffer_id].as_mut() {
                buf.io_done = true;
                buf.io_result = Some(result);
                if buf.try_complete() {
                    pending[buffer_id] = None;
                }
            }
        }
        Completion::Triggers { buffer_id } => {
            if let Some(buf) = pending[buffer_id].as_mut() {
                buf.triggers_done = true;
                if buf.try_complete() {
                    pending[buffer_id] = None;
                }
            }
        }
    }
}

async fn drain_pending(
    pending: &mut [Option<PendingBuffer>; 2],
    completion_rx: &mut mpsc::Receiver<Completion>,
) {
    while pending[0].is_some() || pending[1].is_some() {
        if let Some(c) = completion_rx.recv().await {
            handle_completion(c, pending);
        } else {
            break;
        }
    }
}

// ── Trigger matching ────────────────────────────────────────────────────────

async fn fire_triggers(
    playbooks: &Arc<Mutex<dyn PlaybookStore>>,
    trigger_tx: &broadcast::Sender<TriggerFiredEvent>,
    tuples: &[Tuple],
) {
    if let Ok(all_triggers) = playbooks.lock().await.all_triggers().await {
        for tuple in tuples {
            let matchable = tuple.to_matchable();
            for (playbook_name, trigger) in &all_triggers {
                if tuple.tuple_type != trigger.match_.tuple_type {
                    continue;
                }
                if !trigger.match_.filter.matches(&matchable) {
                    continue;
                }
                let mapped_params = trigger
                    .execution
                    .params
                    .iter()
                    .filter_map(|(param, source)| match source {
                        ParamSource::Field(field) => matchable
                            .get(field)
                            .and_then(|v| v.as_str())
                            .map(|s| (param.clone(), s.to_string())),
                        ParamSource::Literal(v) => Some((param.clone(), v.to_string())),
                    })
                    .collect();
                let event = TriggerFiredEvent {
                    playbook: playbook_name.clone(),
                    agent: trigger.execution.agent.clone(),
                    tuple_uuid7: tuple.uuid7.clone(),
                    tuple_type: tuple.tuple_type.clone(),
                    tuple_data: tuple.data.to_string(),
                    mapped_params,
                    trace_id: tuple.trace_id.clone(),
                };
                let _ = trigger_tx.send(event);
            }
        }
    }
}
