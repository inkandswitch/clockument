use std::collections::HashSet;

use crate::{document_ref::DocumentRef, persistence::NegligenceDecision};
use async_trait::async_trait;
use automerge::transaction::Transaction;
use sedimentree_core::id::SedimentreeId;
use std::fmt::Debug;

/// Provides utilities to manage dependencies, given a transaction.
#[async_trait]
pub trait DependencyResolver: Send + Sync + Debug {
    /// Get an array of shallowly-nested dependencies from the [Transaction].
    /// Users should implement this based on their own schema, which may vary.
    /// Sub-documents may include more dependencies, so this method must handle arbitrary sizes.
    /// There are two options for a contract: Users can either provide all dependencies at the transaction heads,
    /// or they can provide all dpeendencies at the transaction heads AND all previous heads.
    /// The former upholds the clockument contract for any monotonically-advancing dependencies, but not for reverts.
    /// The latter supports arbitrary reversion of dependencies.
    fn get_dependencies(&self, tx: &Transaction) -> HashSet<DocumentRef>;

    /// The [NegligenceDecision] to enact if a negligent dependency is detected for the clockument.
    fn negligence(&self, clockument_id: SedimentreeId) -> NegligenceDecision;
}
