use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tonic::Status;

use proto::tuples::TriggerFiredEvent;
use tuples_core::playbook::ParamSource;
use tuples_core::tuple::Tuple;
use tuples_storage::{PlaybookStore, TupleStore};

// TupleStore uses &self so no Mutex needed — Arc alone is sufficient.

const BATCH_SIZE: usize = 10;
const BATCH_TIMEOUT: Duration = Duration::from_millis(2);

struct BatchItem {
    tuple: Tuple,
    /// If set, the sender is notified once the batch containing this tuple is committed.
    notify: Option<oneshot::Sender<Result<(), String>>>,
    /// If true, flush the current batch immediately rather than waiting to fill it.
    flush_now: bool,
}

pub struct BatchWriter {
    tx: mpsc::Sender<BatchItem>,
}

impl BatchWriter {
    pub fn new(
        store: Arc<dyn TupleStore>,
        playbooks: Arc<Mutex<dyn PlaybookStore>>,
        trigger_tx: broadcast::Sender<TriggerFiredEvent>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(BATCH_SIZE);
        tokio::spawn(run(rx, store, playbooks, trigger_tx));
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
                    // Queue full — attach a notifier and wait for the next flush.
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

async fn run(
    mut rx: mpsc::Receiver<BatchItem>,
    store: Arc<dyn TupleStore>,
    playbooks: Arc<Mutex<dyn PlaybookStore>>,
    trigger_tx: broadcast::Sender<TriggerFiredEvent>,
) {
    loop {
        // Wait for the first item, then greedily drain the rest up to BATCH_SIZE.
        let first = match tokio::time::timeout(BATCH_TIMEOUT, rx.recv()).await {
            Ok(Some(item)) => item,
            Ok(None) => break, // channel closed — shut down
            Err(_) => continue, // timeout — nothing arrived, loop again
        };

        let flush_now = first.flush_now;
        let mut batch = vec![first];

        if !flush_now {
            while batch.len() < BATCH_SIZE {
                match rx.try_recv() {
                    Ok(item) => {
                        let f = item.flush_now;
                        batch.push(item);
                        if f {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        let tuples: Vec<Tuple> = batch.iter().map(|i| i.tuple.clone()).collect();
        let result = store.put_batch(&tuples).await;

        // Fire triggers — only after a successful commit, before notifying callers so
        // that guaranteed-write callers see events as soon as their put_tuple returns.
        if result.is_ok() {
            if let Ok(all_triggers) = playbooks.lock().await.all_triggers().await {
                for tuple in &tuples {
                    let matchable = tuple.to_matchable();
                    for (playbook_name, trigger) in &all_triggers {
                        // New format: check tuple_type first, then optional filter.
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
                                ParamSource::Literal(v) => {
                                    Some((param.clone(), v.to_string()))
                                }
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

        // Notify any callers that were waiting for this batch.
        let err_str = result.err().map(|e| e.to_string());
        for item in batch {
            if let Some(tx) = item.notify {
                let r = match &err_str {
                    None => Ok(()),
                    Some(e) => Err(e.clone()),
                };
                let _ = tx.send(r);
            }
        }
    }
}
