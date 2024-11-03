//! Table definitions and accessors for the redb database.
use std::collections::BTreeSet;

use iroh_base::hash::{Hash, HashAndFormat};
use iroh_blobs::Tag;
use redb::{ReadableTable, TableDefinition, TableError};

use super::{EntryState, PathOptions};

pub(super) const BLOBS_TABLE: TableDefinition<Hash, EntryState> = TableDefinition::new("blobs-0");

pub(super) const TAGS_TABLE: TableDefinition<Tag, HashAndFormat> = TableDefinition::new("tags-0");

/// A trait similar to [`redb::ReadableTable`] but for all tables that make up
/// the blob store. This can be used in places where either a readonly or
/// mutable table is needed.
pub(super) trait ReadableTables {
    fn blobs(&self) -> &impl ReadableTable<Hash, EntryState>;
    fn tags(&self) -> &impl ReadableTable<Tag, HashAndFormat>;
}

/// A struct similar to [`redb::Table`] but for all tables that make up the
/// blob store.
pub(super) struct Tables<'a> {
    pub blobs: redb::Table<'a, Hash, EntryState>,
    pub tags: redb::Table<'a, Tag, HashAndFormat>,
}

impl<'txn> Tables<'txn> {
    pub fn new(
        tx: &'txn redb::WriteTransaction,
    ) -> std::result::Result<Self, TableError> {
        Ok(Self {
            blobs: tx.open_table(BLOBS_TABLE)?,
            tags: tx.open_table(TAGS_TABLE)?,
        })
    }
}

impl ReadableTables for Tables<'_> {
    fn blobs(&self) -> &impl ReadableTable<Hash, EntryState> {
        &self.blobs
    }
    fn tags(&self) -> &impl ReadableTable<Tag, HashAndFormat> {
        &self.tags
    }
}

/// A struct similar to [`redb::ReadOnlyTable`] but for all tables that make up
/// the blob store.
pub(super) struct ReadOnlyTables {
    pub blobs: redb::ReadOnlyTable<Hash, EntryState>,
    pub tags: redb::ReadOnlyTable<Tag, HashAndFormat>,
}

impl<'txn> ReadOnlyTables {
    pub fn new(tx: &'txn redb::ReadTransaction) -> std::result::Result<Self, TableError> {
        Ok(Self {
            blobs: tx.open_table(BLOBS_TABLE)?,
            tags: tx.open_table(TAGS_TABLE)?,
        })
    }
}

impl ReadableTables for ReadOnlyTables {
    fn blobs(&self) -> &impl ReadableTable<Hash, EntryState> {
        &self.blobs
    }
    fn tags(&self) -> &impl ReadableTable<Tag, HashAndFormat> {
        &self.tags
    }
}
