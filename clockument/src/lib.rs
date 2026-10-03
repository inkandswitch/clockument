


/// Desired API (pseudocode for now)
///
/// First, we need to implement our persitence targets, with the [PersistenceTarget] struct.
///
/// Implement [PersistenceTarget] for disk, server, peers, etc.
///
/// Then, make a Clockument. A Clockument is defined as an ID and a dependencies function.
///
/// We need a way to get the dependencies from a clockument at a particular heads.
/// get_dependencies must return all current dependencies.
/// If revert-safety is desired, it must return all current and past dependencies.
///
/// ```no_run
///
/// fn get_dependencies(doc: &Automerge, heads: Heads) -> HashSet<DocumentRef> {
///     return doc.deps; // get all dependencies here
/// }
///
/// ```
///
/// To initialize the clockument:
/// ```no_run
/// // root clockument ID
/// let clockument = Clockument::new(clockument_id, get_dependencies);
///
/// let coordinator = ClockumentCoordinator::new(clockument);
/// // Persistences immediately begin syncing with each other as needed
/// let disk_persistence = coordinator.add_persistence(disk: DiskPersistenceTarget)
/// let remote_persistence = coordinator.add_persistence(remote: RemotePersistenceTarget)
/// // If this is a brand new clockument, insert the data:
/// coordinator.insert(my_data: Automerge);
/// // Otherwise, the clockument and dependencies will automatically be loaded from the persistence.
///
/// ```
///
/// When we want to commit:
/// ```no_run
/// // To alter dependencies, first get the ref of the dependency from the coordinator.
/// // The current transaction heads are guaranteed to be valid on disk_persistence (e.g. `tx.get()`).
/// // Historical heads are guaranteed to be valid as well, because we always track all previous dependencies.
/// let dep_1_ref = coordinator.transact(disk_persistence, async move |tx| {
///     doc.get("dep1")
/// });
///
/// // Then we can directly alter it on disk_persistence, getting a new ref.
/// let dep_1_ref = coordinator.transact_at(dep_1_ref, disk_persistence, async move |mut tx| {
///     // do some work, and then commit:
///     let new_dep_head = tx.commit();
///     DocumentRef { dep_1_ref.id, new_dep_head }
/// });
///
/// // Alternatively, we could add a new dependency.
/// let dep_2_doc = Automerge::new("some content");
/// let dep_2_ref = DocumentRef { id: SedimentreeId::generate(), heads: dep_2_doc.get_heads() };
/// // If somehow there's already a document in here, it'll just get merged and we'll end up using our new, unrelated heads.
/// // This effectively overwrites the document at our new clockument heads.
/// coordinator.insert(dep_2_ref.id, dep_2_doc);
///
/// // Or perhaps, we could roll back a dependency (revert).
/// // If get_dependencies provides the historical dependency support, this is always safe for well-behaved peers.
/// let dep_3_ref = clockument.transact(disk_persistence, async move |tx| {
///     doc.get_at("dep3", some_old_heads)
/// });
///
/// // Add both documents as a dependencies to the clockument.
/// // This will automatically begin discovery and propagation of dep3.
/// let new_head = clockument.transact(disk_persistence, async move |mut tx| {
///     tx.insert("dep1", dep_1_ref);
///     tx.insert("dep2", dep_2_ref);
///     tx.insert("dep3", dep_3_ref);
///     tx.commit() // return the new_head
/// });
///
/// // Wait for everything to be fully persisted.
/// // Do this before transacting anymore stuff, because it might commit to the old heads and cause
/// // a merge conflict.
/// clockument.heads_ready(clockument_id, new_head, disk_persistence).await;
/// ```
///
///
///
///
///
///
/// 
/// 
pub mod coordinator;
pub mod persistence;
pub mod heads;
pub mod document_ref;
pub mod dependency;