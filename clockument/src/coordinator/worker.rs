use std::{collections::{HashMap, HashSet}, sync::Arc};

use automerge::{Automerge, ChangeHash};
use futures::{Stream, StreamExt};
use indextree::Arena;
use sedimentree_core::id::SedimentreeId;
use tokio::{select, sync::{Mutex, mpsc}, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{coordinator::{Clockument, ClockumentError, Insertion, InsertionFailure, PersistenceId, PutResult}, document_ref::DocumentRef, heads::Heads, persistence::{NegligenceDecision, PersistenceError, PersistenceTarget}};


// TODO: eventually we'll need rebroadcast data probably (like which clockument to broadcast deps of)
pub struct Rebroadcast;

struct PendingInsertion {
    insertion: Insertion,
    dependencies: DependencyTree,
}

#[derive(Clone)]
pub struct PersistenceWorker {
    clockument: Clockument,
    id: PersistenceId,

    pub token: CancellationToken,
    pub target: Arc<dyn PersistenceTarget>,

    /// Channel for insertions this worker needs to process
    insert_tx: mpsc::UnboundedSender<Insertion>,

    /// Channel for when new data is available, i.e. when we put stuff
    put_tx: mpsc::UnboundedSender<PutResult>,
    put_failure_tx: mpsc::UnboundedSender<InsertionFailure>,

    /// Channel for rebroadcast requests
    rebroadcast_tx: mpsc::UnboundedSender<Rebroadcast>,

    pending_insertions: Arc<Mutex<Vec<PendingInsertion>>>,
}

#[derive(Clone)]
pub struct DependencyTreeItem {
    // If a dependency tree item is poisoned, it means we no longer wait for the dependency or any of its children.
    poisoned: bool,
    resolved: bool,
    document_ref: DocumentRef,
} 

pub type DependencyTree = Arena<DependencyTreeItem>;

enum DependencyResolution {
    Resolved { dependencies: HashSet<DocumentRef> },
    NotFound,
    Failed,
}

impl PersistenceWorker {
    pub fn new(
        id: PersistenceId,
        clockument: Clockument,
        target: Arc<dyn PersistenceTarget>,
        put_tx: mpsc::UnboundedSender<PutResult>,
        put_failure_tx: mpsc::UnboundedSender<InsertionFailure>,
    ) -> Arc<Self> {
        let (insert_tx, insert_rx) = mpsc::unbounded_channel();
        let (rebroadcast_tx, rebroadcast_rx) = mpsc::unbounded_channel();

        let this = Self {
            clockument,
            id,
            token: CancellationToken::new(),
            target,
            insert_tx,
            put_tx,
            pending_insertions: Default::default(),
            rebroadcast_tx,
            put_failure_tx,
        };

        // subscribe the heads stream now, so rebroadcast always runs after we've already subscribed to this.
        // this ensures if new_heads are missed, they're always rebroadcasted.

        {
            let this = this.clone();
            let tgt = this.target.clone();
            let heads_stream = tgt.new_heads();
            tokio::task::spawn(async move {
                select! {
                    _ = this.token.cancelled() => {}
                    _ = this.notifier_driver(rebroadcast_rx, heads_stream) => {}
                };
            });
        }
        {
            let this = this.clone();
            tokio::task::spawn(async move {
                select! {
                    _ = this.token.cancelled() => {}
                    _ = this.inserter_driver(insert_rx) => {}
                };
            });
        }
        Arc::new(this)
    }

    pub fn insert(&self, data: Insertion) {
        let _ = self.insert_tx.send(data);
    }

    pub fn rebroadcast(&self, data: Rebroadcast) {
        let _ = self.rebroadcast_tx.send(data);
    }

    async fn notify_rebroadcast(&self) -> Result<(), ClockumentError> {
        // Grab our version of the clockument. If it doesn't exist, we can't rebroadcast it to anyone.
        let heads: Heads = match self.target.get(self.clockument.id).await {
            Ok(doc) => doc.get_heads().into(),
            Err(e) => match e {
                // this is ok!
                PersistenceError::NotFound(_) => return Ok(()),
                PersistenceError::Other(error) => return Err(ClockumentError::Persist(error)),
            },
        };

        // Explicitly poison any not-found elements -- if we've already got a clockument, we'd better have the
        // dependencies!
        let tree = self.initialize_dependencies(heads.clone(), None);
        let tree = self.resolve_dependencies(tree, true).await;

        if tree.iter().any(|d| d.get().poisoned) {
            // TODO: We may want to pause here, and figure out a way to receive other info
            // from other targets. They may have the DocumentRef that is unresolved.
            // Currently, the negligence propagates always. If the target DOES have the dependency,
            // it's removed from the negligence array for future propagations, so eventually targets *should*
            // get informed...
            match self.clockument.dependencies.negligence(self.clockument.id) {
                NegligenceDecision::Propagate => {}
                NegligenceDecision::DoNotPropagate => return Ok(()),
            }
        }

        // Notify everyone of the dependencies that we do have
        for node in &tree {
            let data = node.get();
            if !data.resolved {
                continue;
            }
            let _ = self.put_tx.send(PutResult {
                doc_ref: data.document_ref.clone(),
                source: self.id,
                dependencies: Default::default(),
            });
        }

        // Notify everyone of our clockument at the current heads
        let _ = self.put_tx.send(PutResult {
            doc_ref: DocumentRef::new(self.clockument.id, heads),
            source: self.id,
            dependencies: tree,
        });
        Ok(())
    }

    /// Driver for notifying the parent about puts
    async fn notifier_driver(
        &self,
        mut rebroadcast_rx: mpsc::UnboundedReceiver<Rebroadcast>,
        mut heads_stream: std::pin::Pin<Box<dyn Stream<Item = DocumentRef> + Send>>,
    ) {
        loop {
            select! {
                res = heads_stream.next() => match res {
                    Some(heads) => self.notify_put(&heads).await,
                    None => break,
                },
                res = rebroadcast_rx.recv() => {
                    let _ = match res {
                        Some(_) => (),
                        None => break,
                    };

                    match self.notify_rebroadcast().await {
                        Ok(()) => {},
                        Err(e) => tracing::error!("Error requesting rebroadcast: {e:?}"),
                    }
                }
            }
        }
    }

    async fn notify_put(&self, doc_ref: &DocumentRef) {
        let dependencies = if self.clockument.id == doc_ref.id() {
            let tree = self.initialize_dependencies(doc_ref.heads().clone(), None);

            // Explicitly poison any not-found elements -- if we've put a clockument, we'd better have the
            // dependencies!
            let tree = self.resolve_dependencies(tree, true).await;

            if tree.iter().any(|d| d.get().poisoned) {
                match self.clockument.dependencies.negligence(self.clockument.id) {
                    NegligenceDecision::Propagate => {}
                    NegligenceDecision::DoNotPropagate => return,
                }
            }
            tree
        } else {
            DependencyTree::new()
        };

        let _ = self.put_tx.send(PutResult {
            doc_ref: doc_ref.clone(),
            source: self.id,
            dependencies,
        });

        // Since we persisted a potential dependency, check to see if it resolves anything else.
        self.try_resolve_pending(doc_ref.id()).await;
    }

    /// Returns an unresolved [DependencyTree] with a single dependency, the root Clockument.
    fn initialize_dependencies(
        &self,
        heads: Heads,
        known_root: Option<&mut Automerge>,
    ) -> DependencyTree {
        let mut tree = DependencyTree::new();
        let root = tree.new_node(DependencyTreeItem {
            poisoned: false,
            resolved: known_root.is_some(),
            document_ref: DocumentRef::new(self.clockument.id, heads.clone()),
        });
        if let Some(known_root) = known_root {
            let deps = self.clockument.get_dependencies(known_root, &heads);
            for dep in deps {
                root.append_value(
                    DependencyTreeItem {
                        poisoned: false,
                        resolved: false,
                        document_ref: dep,
                    },
                    &mut tree,
                );
            }
        }
        tree
    }

    // TODO: Currently, every check of this checks the entire fringe. Add a scope_to_id parameter
    // that allows us to ONLY check the incoming document (and whatever children it might produce when resolved).
    /// Resolve the [DependencyTree], inserting new dependencies as they're discovered.
    /// The root item is expected to be a dependency for the clockument itself.
    /// If should_poison is true, marks any not-found dependencies (and their children) as "poisoned".
    /// "Poisoned" just means that the dependency wasn't resolved at the target it's coming from -- meaning the target was negligent.
    /// While this won't prevent future lookups, it ensures that chlid dependencies remain fallible, if their parents are fallible.
    /// In fact, the ability to poison a tree is the entire reason we're using a tree structure at all!
    async fn resolve_dependencies(
        &self,
        mut tree: Arena<DependencyTreeItem>,
        should_poison: bool,
    ) -> Arena<DependencyTreeItem> {
        let mut pending = JoinSet::new();
        // Visited set, so we don't create trees that are diamond-shaped.
        // TODO: This might get a little weird if a deep path resolves quickly, and gives up due to depth, but a shallow
        // path resolves slowly and still needs depth inspection. Some dependencies might be missed in that case.
        // But that never happens with Backstitch, so I'm ignoring it for now...
        // Until I fix that, ship clockument with a constraint: No diamond-shaped graphs.
        let mut visited = HashSet::new();

        let mut frontier: Vec<_> = tree.roots().collect();
        while let Some(item) = frontier.pop() {
            let data = tree.get_data(item).expect("node exists");

            if !visited.insert(data.document_ref.clone()) {
                continue;
            }

            // If the data has already been resolved by a previous invocation, we move immediately onto its children.
            if data.resolved {
                frontier.extend(item.children(&tree));
                continue;
            }

            // Only spawn tasks for unresolved items
            let depth = item.depth(&tree);
            let clockument = self.clockument.clone();
            let target = self.target.clone();
            let doc_ref = data.document_ref.clone();
            pending.spawn(async move {
                (
                    item,
                    Self::resolve_dependency(clockument.clone(), target.clone(), depth, doc_ref)
                        .await,
                )
            });
        }

        while let Some(res) = pending.join_next().await {
            let (item, resolution) = res.expect("dependency resolution task panicked");
            let depth = item.depth(&tree);
            match resolution {
                DependencyResolution::Resolved { dependencies } => {
                    let data = tree.get_data_mut(item).expect("node exists");
                    data.resolved = true;
                    let poisoned = data.poisoned;
                    for dep in dependencies {
                        if !visited.insert(dep.clone()) {
                            continue;
                        }
                        let child = item.append_value(
                            DependencyTreeItem {
                                document_ref: dep.clone(),
                                poisoned,
                                resolved: false,
                            },
                            &mut tree,
                        );
                        let clockument = self.clockument.clone();
                        let target = self.target.clone();
                        pending.spawn(async move {
                            (
                                child,
                                Self::resolve_dependency(clockument, target, depth + 1, dep).await,
                            )
                        });
                    }
                }
                DependencyResolution::NotFound => {
                    let data = tree.get_data_mut(item).expect("node exists");
                    data.poisoned = data.poisoned || should_poison;
                }
                DependencyResolution::Failed => {
                    let data = tree.get_data_mut(item).expect("node exists");
                    data.poisoned = true;
                }
            }
        }
        tree
    }

    async fn resolve_dependency(
        clockument: Clockument,
        target: Arc<dyn PersistenceTarget>,
        depth: usize,
        document_ref: DocumentRef,
    ) -> DependencyResolution {
        // If we're beyond a certain level, replace the expensive get check with a cheap has check.
        if clockument
            .dependency_search_depth
            .is_some_and(|d| d < depth)
        {
            let resolved = target
                .has(&document_ref)
                .await
                .inspect_err(|e| tracing::error!("Unknown error during has: {e}"))
                .unwrap_or(false);
            if resolved {
                return DependencyResolution::Resolved {
                    dependencies: Default::default(),
                };
            } else {
                return DependencyResolution::NotFound;
            }
        }

        // We assume this is cheap on a 404. Then, once we've resolved, we never need to run it again.
        let mut doc = match target.get(document_ref.id()).await {
            Ok(doc) => doc,
            Err(e) => match e {
                PersistenceError::NotFound(_) => {
                    return DependencyResolution::NotFound;
                }
                PersistenceError::Other(error) => {
                    tracing::error!("Unknown error during dependency tree fetch: {error}");
                    // Assume that we'll never get this doc, so give up.
                    return DependencyResolution::Failed;
                }
            },
        };

        // Ensure that the heads exist.
        // TODO: We don't currently check for shared ancestry, because I think it's probably slow...
        // but to be fully correct here we'd ensure user-supplied heads were never conflicting (i.e. one is a ancestor of another.)
        if document_ref
            .heads()
            .iter()
            .any(|h| doc.get_change_meta_by_hash(h).is_none())
        {
            return DependencyResolution::NotFound;
        }

        return DependencyResolution::Resolved {
            dependencies: clockument.get_dependencies(&mut doc, document_ref.heads()),
        };
    }

    fn propagate_poison(
        mut dependencies: DependencyTree,
        source_dependencies: &DependencyTree,
    ) -> DependencyTree {
        // Notes the heads that have been poisoned.
        // Warning: this method doesn't check derivative heads...
        // If a parent head has been poisoned, the child MIGHT be poisoned (unless we've explicitly repaired it)!
        // So, callers just assume that poisoned data has been merged alongside non-poisoned data.
        let mut poisoned: HashMap<SedimentreeId, HashSet<ChangeHash>> = HashMap::new();

        for dep in source_dependencies {
            let data = dep.get();
            if data.poisoned {
                let entry = poisoned
                    .entry(data.document_ref.id())
                    .or_insert(HashSet::new());
                entry.extend(data.document_ref.heads().iter());
            }
        }

        for node in &mut dependencies {
            let data = node.get_mut();
            let Some(poisoned_heads) = poisoned.get(&data.document_ref.id()) else {
                continue;
            };
            // The heads are only poisoned if all the poisoned heads are included.
            data.poisoned = data.poisoned
                || poisoned_heads
                    .iter()
                    .all(|h| data.document_ref.heads().iter().any(|head| head == h));
        }

        // Now that we've seeded the poison from the original set, make sure all children have that poison.
        let mut processing = Vec::new();
        for root in dependencies.roots() {
            processing.push(root);
        }
        while let Some(item) = processing.pop() {
            let parent = item.parent(&dependencies);
            let poisoned = parent
                .map(|node| dependencies.get_data(node).expect("parent exists").poisoned)
                .unwrap_or(false);
            let data = dependencies.get_data_mut(item).expect("id exists");
            data.poisoned = data.poisoned || poisoned;

            for child in item.children(&dependencies) {
                processing.push(child);
            }
        }

        dependencies
    }

    /// Driver for handling new insertions coming in from other sources
    async fn inserter_driver(&self, mut insert_rx: mpsc::UnboundedReceiver<Insertion>) {
        loop {
            let insertion = match insert_rx.recv().await {
                Some(r) => r,
                None => break,
            };
            // we may want to semaphore this?
            let this = self.clone();
            let tok = self.token.clone();
            tokio::task::spawn(async move {
                select! {
                    _ = tok.cancelled() => {}
                    _ = this.try_insert(insertion) => {}
                }
            });
        }
    }

    // TODO: See if we can spawn off a task to do this method -- this could be v slow!
    async fn try_insert(&self, mut insertion: Insertion) {
        if insertion.id == self.clockument.id {
            let tree = self.initialize_dependencies(
                insertion.doc.get_heads().into(),
                Some(&mut insertion.doc),
            );

            // We MUST lock this here. At any point, we might get a new_heads notification that resolves a dependency.
            // As such, we need to lock our pending insertions array at the same time we're checking for the dependency.
            // That way, we never miss a try_resolve_pending.
            let mut pendings = self.pending_insertions.lock().await;

            // Don't poison 404s here -- we're just still waiting on missing deps.
            // This still could get poisoned if our target fails.
            let tree = self.resolve_dependencies(tree, false).await;

            // Propagate the poison from the original insertion
            let tree = Self::propagate_poison(tree, &insertion.original_dependencies);

            if self.try_put_tree(&tree).await {
                self.do_put(insertion).await;
                return;
            }

            // If the dependencies are not resolved, we gotta wait on them first.
            // Push it to our pending array.
            // When others are inserted, we'll re-check and try again.
            pendings.push(PendingInsertion {
                insertion,
                dependencies: tree,
            });
        } else {
            self.do_put(insertion).await;
        }
    }

    async fn try_put_tree(&self, tree: &DependencyTree) -> bool {
        // don't do anything until the whole tree is either poisoned or resolved
        if !tree
            .iter()
            .all(|item| item.get().poisoned || item.get().resolved)
        {
            return false;
        }

        // If we resolved any poisoned deps, announce them to everyone!
        for node in tree {
            let data = node.get();
            if !data.poisoned || !data.resolved {
                continue;
            }
            let _ = self.put_tx.send(PutResult {
                doc_ref: data.document_ref.clone(),
                source: self.id,
                dependencies: Default::default(),
            });
        }
        true
    }

    async fn do_put(&self, insertion: Insertion) {
        let heads = insertion.doc.get_heads();
        match self.target.put(insertion.id, insertion.doc).await {
            Ok(()) => {}
            Err(e) => {
                let _ = self.put_failure_tx.send(InsertionFailure {
                    id: insertion.id,
                    heads: Heads::from(heads),
                    error: e,
                });
            }
        }
    }

    /// Called when we've inserted a new potential dependency and should check
    /// to see if any pending clockument writes can go through.
    async fn try_resolve_pending(&self, _persisted_id: SedimentreeId) {
        // TODO: This is awkward; this could take some time if persist() or retain_pending_deps()
        // takes some time. This mutex locks up the put threading, which sucks.
        let mut pendings = self.pending_insertions.lock().await;

        let mut i = 0;
        while i < pendings.len() {
            let pending = &mut pendings[i];

            // TODO: Scope this by persisted_id
            pending.dependencies = self
                .resolve_dependencies(std::mem::take(&mut pending.dependencies), false)
                .await;

            if self.try_put_tree(&pending.dependencies).await {
                let pending = pendings.remove(i);
                self.do_put(pending.insertion).await;
                continue;
            }

            // If we still have more deps, and it's been canceled, give up actually.
            if pending.insertion.token.is_cancelled() {
                let _ = pendings.remove(i);
                continue;
            }

            i += 1;
        }
    }
}
