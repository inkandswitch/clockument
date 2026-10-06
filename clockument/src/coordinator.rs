use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use automerge::{Automerge, PatchLog, transaction::Transaction};
use sedimentree_core::id::SedimentreeId;
use thiserror::Error;
use tokio::{
    select,
    sync::{Mutex, broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;

use crate::{
    coordinator::worker::{DependencyTree, PersistenceWorker, Rebroadcast},
    dependency::DependencyResolver,
    document_ref::DocumentRef,
    heads::Heads,
    persistence::{PersistenceError, PersistenceTarget},
};

mod worker;

#[derive(Clone, Debug)]
struct Insertion {
    id: SedimentreeId,
    doc: Automerge,
    // If this cancels, we give up an insert, if it is pending.
    // Only used for pending from other sources, not transient.
    // Only used for root clockuments.
    token: CancellationToken,
    // A set of the original dependencies from the target's put.
    // Allows us to keep track of poisoned dependencies, so we don't end up
    // relying on dependencies that might never arrive.
    original_dependencies: DependencyTree,
}

#[derive(Debug)]
struct PutResult {
    doc_ref: DocumentRef,
    source: PersistenceId,
    dependencies: DependencyTree,
}

#[derive(Debug)]
struct InsertionFailure {
    error: PersistenceError,
    heads: Heads,
    id: SedimentreeId,
}

type WorkerPool = Arc<Mutex<HashMap<PersistenceId, Arc<PersistenceWorker>>>>;

#[derive(Clone, Debug)]
pub struct Clockument {
    id: SedimentreeId,
    /// Specifies a depth which to stop searching for dependencies.
    /// 0 means that only the root clockument is checked for dependencies. None has no depth, which means
    /// ALL documents are checked for dependencies, which may incur a performance penalty.
    dependency_search_depth: Option<usize>,
    dependencies: Arc<dyn DependencyResolver>,
}

// TODO: Add "identify clockument" feature to support adding arbitrary clockuments rather than just one
impl Clockument {
    pub fn new(
        id: SedimentreeId,
        dependencies: Arc<dyn DependencyResolver>,
        dependency_search_depth: Option<usize>,
    ) -> Self {
        Self {
            id,
            dependencies,
            dependency_search_depth,
        }
    }

    pub fn id(&self) -> SedimentreeId {
        self.id
    }

    fn get_dependencies(&self, doc: &mut Automerge, heads: &Heads) -> HashSet<DocumentRef> {
        let tx = doc
            .transaction_at(PatchLog::inactive(), heads.iter().as_slice())
            .unwrap();
        self.dependencies.get_dependencies(&tx)
    }
}

#[derive(Clone)]
enum ClockumentPutResult {
    Success,
    Failure {
        heads: Heads,
        error: Arc<PersistenceError>,
    },
}

#[derive(Debug)]
pub struct ClockumentCoordinator {
    last_persistence_id: AtomicU64,
    inner: Arc<ClockumentCoordinatorInner>,
}

#[derive(Clone, Debug)]
struct ClockumentCoordinatorInner {
    workers: WorkerPool,
    clockument: Clockument,
    // These are different channels because put failures come directly from the worker doing the put,
    // but puts_tx is informed by many things, like rebroadcasting or a put on the target
    puts_tx: mpsc::UnboundedSender<PutResult>,
    puts_failure_tx: mpsc::UnboundedSender<InsertionFailure>,
    clockument_put_tx: broadcast::Sender<ClockumentPutResult>,
    token: CancellationToken,
}

#[derive(Debug, Error)]
pub enum ClockumentError {
    #[error("the clockument didn't have an added persistence of ID {0:?}")]
    NoSuchTarget(PersistenceId),
    #[error("no document was found matching the ID {0}")]
    NoSuchDocument(SedimentreeId),
    #[error("no heads {0:?} were found on the document")]
    NoSuchHeads(Heads),
    #[error("the target was removed")]
    TargetRemoved,
    #[error("there was an error in persistence: {0}")]
    Persist(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct PersistenceId(u64);

impl ClockumentCoordinator {
    pub fn new(clockument: Clockument) -> Self {
        let (puts_tx, puts_rx) = mpsc::unbounded_channel();
        let (puts_failure_tx, puts_failure_rx) = mpsc::unbounded_channel();
        let (clockument_put_tx, _) = broadcast::channel(1024);
        let token = CancellationToken::new();
        let workers = WorkerPool::new(Default::default());

        let inner = Arc::new(ClockumentCoordinatorInner {
            clockument,
            workers,
            puts_tx,
            token: token.clone(),
            clockument_put_tx: clockument_put_tx.clone(),
            puts_failure_tx,
        });
        {
            let token = token.clone();
            let inner = inner.clone();
            {
                tokio::task::spawn(async move {
                    select! {
                        _ = token.cancelled() => {}
                        _ = inner.driver_loop(puts_rx, puts_failure_rx) => {}
                    }
                });
            }
        }
        return Self {
            last_persistence_id: Default::default(),
            inner,
        };
    }

    /// Add a new persistence target to the coordinator.
    /// Optionally, reconcile from an existing best target, ensuring that the target is
    /// fully synced up and ready for more events.
    pub async fn add_persistence(
        &self,
        target: Arc<dyn PersistenceTarget>,
    ) -> Result<PersistenceId, ClockumentError> {
        let mut workers = self.inner.workers.lock().await;

        let id = PersistenceId(self.last_persistence_id.fetch_add(1, Ordering::Relaxed));
        let worker = PersistenceWorker::new(
            id,
            self.inner.clockument.clone(),
            target,
            self.inner.puts_tx.clone(),
            self.inner.puts_failure_tx.clone(),
        );

        workers.insert(id, worker.clone());

        for (_, worker) in &*workers {
            // Ask every worker to re-announce its persisted data to everyone else.
            // This is intended to catch the new worker up to everyone, and also inform everyone
            // of the new worker's data.
            worker.rebroadcast(Rebroadcast);
        }

        Ok(id)
    }

    /// Remove a persistence target
    pub async fn remove_persistence(&self, id: PersistenceId) -> Result<(), ClockumentError> {
        let mut workers = self.inner.workers.lock().await;
        let worker = workers
            .remove(&id)
            .ok_or(ClockumentError::NoSuchTarget(id))?;

        // Shuts down all the inner stuff, AND notifies pending clockuments that are expecting stuff from the worker
        // to give up.
        worker.token.cancel();
        Ok(())
    }

    /// Insert new data into the coordinator. If a document with `id` already exists,
    /// it will be merged into the existing copy.
    /// This method will return when the insertion is queued, but not when any actual
    /// insertion is done.
    /// If `id` is that of the root clockument, it will not be persisted into any location until
    /// ALL dependencies have also been persisted to that location.
    /// As such, if `id` is a root clockument, the user MUST [Self::insert] or [Self::transact] ALL
    /// untracked dependencies (including heads) into the coordinator!
    /// If this is not performed, the user may instead cancel the returned token.
    pub async fn insert(
        &self,
        id: SedimentreeId,
        doc: Automerge,
    ) -> Result<CancellationToken, ClockumentError> {
        let workers = self.inner.workers.lock().await;
        let token = CancellationToken::new();
        for (_, worker) in &*workers {
            worker.insert(Insertion {
                token: token.clone(),
                id,
                doc: doc.clone(),
                // We're expecting users to ALWAYS insert/transact dependencies into the coordinator.
                // So, we don't need any poisonable dependencies.
                original_dependencies: Default::default(),
            })
        }
        Ok(token)
    }

    /// Transact over the clockument, scoped to the most recent valid heads on a persistence.
    /// If new dependencies are added, they MUST be added with [Self::insert] or [Self::transact].
    /// The transaction will occur at the most-recently persisted heads. If transactions occur on the same heads,
    /// they will both be persisted, and will both be merged.
    pub async fn transact<F, R>(
        &self,
        persistence: PersistenceId,
        f: F,
    ) -> Result<R, ClockumentError>
    where
        F: AsyncFnOnce(Transaction) -> R,
    {
        let workers = self.inner.workers.lock().await;
        let worker = workers
            .get(&persistence)
            .ok_or(ClockumentError::NoSuchTarget(persistence))?
            .clone();

        drop(workers);

        let mut doc = worker
            .target
            .get(self.inner.clockument.id)
            .await
            .map_err(|e| match e {
                PersistenceError::NotFound(sedimentree_id) => {
                    ClockumentError::NoSuchDocument(sedimentree_id)
                }
                PersistenceError::Other(error) => ClockumentError::Persist(error),
            })?;

        let persisted_heads = doc.get_heads();
        let res = {
            let tx = doc
                .transaction_at(
                    PatchLog::inactive(),
                    persisted_heads.clone().into_iter().as_slice(),
                )
                .unwrap();

            f(tx).await
        };

        // Nothing changed
        if doc.get_heads() == persisted_heads {
            return Ok(res);
        }

        // Persist to all sources. If something else persists in the meantime,
        // our changes will be merged in.
        self.insert(self.inner.clockument.id, doc).await?;

        Ok(res)
    }

    pub async fn transact_at<F, R>(
        &self,
        doc_ref: &DocumentRef,
        persistence: PersistenceId,
        f: F,
    ) -> Result<R, ClockumentError>
    where
        F: AsyncFnOnce(Transaction) -> R,
    {
        let workers = self.inner.workers.lock().await;
        let worker = workers
            .get(&persistence)
            .ok_or(ClockumentError::NoSuchTarget(persistence))?
            .clone();

        drop(workers);

        let mut doc = select! {
            _ = worker.token.cancelled() => {
                return Err(ClockumentError::TargetRemoved);
            }
            doc = worker.target.get(doc_ref.id()) => {
                doc
            }
        }
        .map_err(|e| match e {
            PersistenceError::NotFound(id) => ClockumentError::NoSuchDocument(id),
            PersistenceError::Other(error) => ClockumentError::Persist(error),
        })?;

        let persisted_heads = doc.get_heads();
        let res = {
            let tx = doc
                .transaction_at(
                    PatchLog::inactive(),
                    doc_ref.heads().clone().into_iter().as_slice(),
                )
                .unwrap();

            let heads = Heads::from(tx.get_heads());
            if &heads != doc_ref.heads() {
                return Err(ClockumentError::NoSuchHeads(heads));
            }

            f(tx).await
        };

        // Nothing changed
        if doc.get_heads() == persisted_heads {
            return Ok(res);
        }

        // Persist to all sources. If something else persists in the meantime,
        // our changes will be merged in.
        self.insert(doc_ref.id(), doc).await?;

        Ok(res)
    }

    /// After transacting, await this to ensure the transaction has persisted.
    pub async fn heads_ready(
        &self,
        heads: Heads,
        persistence: PersistenceId,
    ) -> Result<(), ClockumentError> {
        let mut rx = self.inner.clockument_put_tx.subscribe();

        let workers = self.inner.workers.lock().await;
        let worker = workers
            .get(&persistence)
            .ok_or(ClockumentError::NoSuchTarget(persistence))?
            .clone();

        drop(workers);

        // If we're already fully persisted at the desired heads, we're OK.
        let doc_ref = DocumentRef::new(self.inner.clockument.id, heads.clone());
        select! {
            _ = worker.token.cancelled() => {
                return Err(ClockumentError::TargetRemoved);
            }
            has = worker.target.has(&doc_ref) => {
                if has.map_err(|e|ClockumentError::Persist(Box::new(e)))? {
                    return Ok(());
                }
            }
        }

        loop {
            select! {
                _ = worker.token.cancelled() => {
                    return Err(ClockumentError::TargetRemoved);
                }
                // This is always emitted AFTER a put -- so if we must wait some time before this fires, that's OK.
                // This will occur on the next put, when we actually put the clockument.
                res = rx.recv() => {
                    match res {
                        Ok(res) => match res {
                            ClockumentPutResult::Success { .. } => {
                                if worker
                                    .target
                                    .has(&DocumentRef::new(self.inner.clockument.id(), heads.clone()))
                                    .await
                                    .map_err(|e| ClockumentError::Persist(Box::new(e)))?
                                {
                                    return Ok(());
                                }

                            },
                            ClockumentPutResult::Failure { heads: failed_heads, error } => {
                                if failed_heads.contains(&heads) {
                                    return Err(ClockumentError::Persist(error.into()));
                                }
                            },
                        },
                        Err(_) =>return Err(ClockumentError::TargetRemoved),
                    }
                }
            }
        }
    }
}

impl ClockumentCoordinatorInner {
    async fn driver_loop(
        &self,
        mut puts_rx: mpsc::UnboundedReceiver<PutResult>,
        mut puts_failure_rx: mpsc::UnboundedReceiver<InsertionFailure>,
    ) {
        // TODO: I think we can spawn off subtasks for the put ops... maybe
        loop {
            select! {
                _ = self.token.cancelled() => { break; },
                failure = puts_failure_rx.recv() => match failure {
                    Some(res) => self.handle_insertion_failure(res).await,
                    None => break,
                },
                put = puts_rx.recv() => match put {
                    Some(put) => self.handle_put(put).await,
                    None => {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_insertion_failure(&self, fail: InsertionFailure) {
        // Log the errors
        tracing::error!("insertion failure: {fail:?}");
        if fail.id != self.clockument.id() {
            return;
        }
        // Send this so that heads_ready can stop waiting on a thing that'll never happen
        let _ = self.clockument_put_tx.send(ClockumentPutResult::Failure {
            heads: fail.heads,
            error: Arc::new(fail.error),
        });
    }

    // Whenever we see new heads incoming from somewhere, broadcast it to every driver.
    async fn handle_put(&self, put: PutResult) {
        // Notify subscribers if the clockument changed
        if put.doc_ref.id() == self.clockument.id() {
            let _ = self.clockument_put_tx.send(ClockumentPutResult::Success);
        }

        let worker = {
            let workers = self.workers.lock().await;
            let Some(worker) = workers.get(&put.source) else {
                // Worker was removed, therefore we don't bother
                return;
            };
            worker.clone()
        };

        // TODO: do we really always need to clone this into every worker? definitely not.
        // We should instead store the heads of the inserted doc during put (the actual heads that would occur
        // on materialization), pass them through to here, and get ad-hoc if needed.
        // But that causes tricky conditions where a clockument is updated on the persistence again,
        // and the merged result is then bad. So maybe we could only pull the doc through here for clockuments?
        let doc = select! {
            _ = worker.token.cancelled() => { return; }
            res = worker.target.get(put.doc_ref.id()) => res,
        };

        let doc = match doc {
            Ok(d) => d,
            Err(e) => {
                tracing::error!("error getting putted doc: {e}");
                return;
            }
        };

        let token = worker.token.clone();
        let workers = self.workers.lock().await;
        for (id, worker) in &*workers {
            // Don't do extra work
            if *id == put.source {
                continue;
            }
            worker.insert(Insertion {
                doc: doc.clone(),
                id: put.doc_ref.id(),
                // This will be used to cancel a PendingInsertion if the source worker drops.
                // TODO: Investigate issues here -- is there ever a situation where the source worker
                // drops after broadcasting all dependencies, and the pending cancels anyways?
                // That's OK if it's just one worker -- but the issue is that maybe, a source worker A drops
                // and doesn't insert, then a source worker B successfully inserts. But then the source worker C
                // would persist heads successfully... unless the heads didn't actually change on C!
                // But if that's the case, presumably B would already know about the heads from C anyways.
                // So I think we're good? Double check this.
                token: token.clone(),
                original_dependencies: put.dependencies.clone(),
            });
        }
    }
}
