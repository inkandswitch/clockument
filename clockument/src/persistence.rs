use std::fmt::Debug;

use crate::document_ref::DocumentRef;
use async_trait::async_trait;
use automerge::Automerge;
use futures::stream::BoxStream;
use sedimentree_core::id::SedimentreeId;

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("document {0} not found")]
    NotFound(SedimentreeId),

    #[error("operation failed")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

// Initial assumptions:
//  - Users are using Subduction, Tokio, Automerge
//  - 2PC to a document is not required by the user

/// Persistence targets, like disk, or peers, or a memory store
#[async_trait]
pub trait PersistenceTarget: Send + Sync + Debug {
    /// Persist a document to the source.
    /// Implementers should ensure this is cancellation-safe, and parallelizable.
    /// If there is an existing doc at the source, it should be merged into the doc.
    /// Null-op persistence should do nothing (i.e. if the destination document is identical to
    /// or a superset of the source document).
    /// Additionally, any actual persistent writes should appear atomic (such as to-disk).
    async fn put(&self, id: SedimentreeId, doc: Automerge) -> Result<(), PersistenceError>;

    /// Check to see if the document is durably available at the provided ref (i.e. can be acquired by
    /// [PersistenceTarget::has])
    async fn has(&self, doc_ref: &DocumentRef) -> Result<bool, PersistenceError>;

    /// Materialize a persisted document.
    async fn get(&self, id: SedimentreeId) -> Result<Automerge, PersistenceError>;

    /// A stream returning new heads when they are persisted to the source.
    /// This MUST be called by the implementation of [PersistenceTarget::put], when it puts new heads.
    fn new_heads(&self) -> BoxStream<'static, DocumentRef>;
}

/// Describes how a negligent [PersistenceTarget] should be handled.
/// A negligent [PersistenceTarget] is a target that has persisted some clockument with heads `A`,
/// but one or more of `dependencies(A)` are not persisted.
/// Negligence will never occur under normal conditions, but disk errors, permission changes, user foolishness,
/// or similar may cause negligence.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NegligenceDecision {
    /// The target should propagate the document, despite the negligent behavior.
    /// This will likely cause an explosion of negligence across every persistence target.
    /// The dependency will be cached as negligent, and other targets will remove the dependency
    /// when checking if it is safe to propagate
    Propagate,
    /// The target should halt propagation of the clockument. This means that the negligence
    /// will never propagate to other targets, but it also means that we can never receive changes from
    /// the negligent target for as long as it is negligent.
    DoNotPropagate,
}
