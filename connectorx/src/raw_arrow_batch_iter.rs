//! Raw Arrow batch iterator for streaming results from raw/stored procedure queries.
//!
//! This module provides iterators for streaming Arrow RecordBatches from raw query results,
//! such as Oracle PL/SQL blocks or MSSQL stored procedures that return result sets via
//! implicit cursors.
//!
//! Unlike the standard `ArrowBatchIter` which uses ConnectorX's parallel query execution,
//! these iterators work with single-threaded raw query execution for cases where
//! parallelism isn't applicable (e.g. stored procedures).

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

#[cfg(feature = "src_oracle")]
use crate::destinations::arrowstream::ArrowDestinationError;
#[cfg(feature = "src_oracle")]
use crate::sources::oracle::OracleSourceError;

// ============================================================================
// Error Types
// ============================================================================

/// Error type for raw Arrow streaming operations
#[derive(Debug)]
pub enum RawArrowError {
    /// Error from Oracle source
    #[cfg(feature = "src_oracle")]
    Oracle(OracleSourceError),
    /// Error from Arrow operations
    Arrow(ArrowError),
    /// Error from Arrow destination
    #[cfg(feature = "src_oracle")]
    ArrowDestination(ArrowDestinationError),
    /// Error from ConnectorX
    ConnectorX(crate::errors::ConnectorXError),
    /// Generic error
    Other(String),
}

impl std::fmt::Display for RawArrowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "src_oracle")]
            RawArrowError::Oracle(e) => write!(f, "Oracle error: {}", e),
            RawArrowError::Arrow(e) => write!(f, "Arrow error: {}", e),
            #[cfg(feature = "src_oracle")]
            RawArrowError::ArrowDestination(e) => write!(f, "Arrow destination error: {}", e),
            RawArrowError::ConnectorX(e) => write!(f, "ConnectorX error: {}", e),
            RawArrowError::Other(e) => write!(f, "Error: {}", e),
        }
    }
}

impl std::error::Error for RawArrowError {}

#[cfg(feature = "src_oracle")]
impl From<OracleSourceError> for RawArrowError {
    fn from(e: OracleSourceError) -> Self {
        RawArrowError::Oracle(e)
    }
}

impl From<ArrowError> for RawArrowError {
    fn from(e: ArrowError) -> Self {
        RawArrowError::Arrow(e)
    }
}

#[cfg(feature = "src_oracle")]
impl From<ArrowDestinationError> for RawArrowError {
    fn from(e: ArrowDestinationError) -> Self {
        RawArrowError::ArrowDestination(e)
    }
}

impl From<crate::errors::ConnectorXError> for RawArrowError {
    fn from(e: crate::errors::ConnectorXError) -> Self {
        RawArrowError::ConnectorX(e)
    }
}

impl From<String> for RawArrowError {
    fn from(e: String) -> Self {
        RawArrowError::Other(e)
    }
}

// ============================================================================
// Oracle Support
// ============================================================================

#[cfg(feature = "src_oracle")]
mod oracle {
    use super::*;
    use crate::data_order::DataOrder;
    use crate::destinations::arrowstream::{ArrowDestination, ArrowTypeSystem};
    use crate::destinations::{Consume, Destination, DestinationPartition};
    use crate::sources::oracle::{OracleRawSourceParser, OracleSource, OracleTypeSystem};
    use crate::sources::{PartitionParser, Produce, RawSource};
    use crate::transports::OracleArrowStreamTransport;
    use crate::typesystem::Transport;
    use arrow::datatypes::Schema;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::sync::Arc;

    /// Default batch size for Arrow record batches
    const DEFAULT_BATCH_SIZE: usize = 1024;

    // ------------------------------------------------------------------------
    // Type Dispatch
    // ------------------------------------------------------------------------

    /// Dispatch a single value from the Oracle parser to the Arrow writer.
    /// This bridges the parser's `Produce` trait with the writer's `Consume` trait.
    fn dispatch_oracle_value<'a, W>(
        parser: &mut OracleRawSourceParser,
        writer: &mut W,
        oracle_type: &OracleTypeSystem,
    ) -> Result<(), RawArrowError>
    where
        W: Consume<i64, Error = ArrowDestinationError>
            + Consume<Option<i64>, Error = ArrowDestinationError>
            + Consume<f64, Error = ArrowDestinationError>
            + Consume<Option<f64>, Error = ArrowDestinationError>
            + Consume<String, Error = ArrowDestinationError>
            + Consume<Option<String>, Error = ArrowDestinationError>
            + Consume<Vec<u8>, Error = ArrowDestinationError>
            + Consume<Option<Vec<u8>>, Error = ArrowDestinationError>
            + Consume<NaiveDateTime, Error = ArrowDestinationError>
            + Consume<Option<NaiveDateTime>, Error = ArrowDestinationError>
            + Consume<DateTime<Utc>, Error = ArrowDestinationError>
            + Consume<Option<DateTime<Utc>>, Error = ArrowDestinationError>,
    {
        match oracle_type {
            // Integer types
            OracleTypeSystem::NumInt(true) => {
                let v: Option<i64> = parser.produce()?;
                writer.consume(v)?;
            }
            OracleTypeSystem::NumInt(false) => {
                let v: i64 = parser.produce()?;
                writer.consume(v)?;
            }

            // Float types
            OracleTypeSystem::NumFloat(true)
            | OracleTypeSystem::Float(true)
            | OracleTypeSystem::BinaryFloat(true)
            | OracleTypeSystem::BinaryDouble(true) => {
                let v: Option<f64> = parser.produce()?;
                writer.consume(v)?;
            }
            OracleTypeSystem::NumFloat(false)
            | OracleTypeSystem::Float(false)
            | OracleTypeSystem::BinaryFloat(false)
            | OracleTypeSystem::BinaryDouble(false) => {
                let v: f64 = parser.produce()?;
                writer.consume(v)?;
            }

            // String types
            OracleTypeSystem::VarChar(true)
            | OracleTypeSystem::Char(true)
            | OracleTypeSystem::NVarChar(true)
            | OracleTypeSystem::NChar(true)
            | OracleTypeSystem::Clob(true) => {
                let v: Option<String> = parser.produce()?;
                writer.consume(v)?;
            }
            OracleTypeSystem::VarChar(false)
            | OracleTypeSystem::Char(false)
            | OracleTypeSystem::NVarChar(false)
            | OracleTypeSystem::NChar(false)
            | OracleTypeSystem::Clob(false) => {
                let v: String = parser.produce()?;
                writer.consume(v)?;
            }

            // Binary types
            OracleTypeSystem::Blob(true) => {
                let v: Option<Vec<u8>> = parser.produce()?;
                writer.consume(v)?;
            }
            OracleTypeSystem::Blob(false) => {
                let v: Vec<u8> = parser.produce()?;
                writer.consume(v)?;
            }

            // Date/Timestamp types (without timezone)
            OracleTypeSystem::Date(true)
            | OracleTypeSystem::Timestamp(true)
            | OracleTypeSystem::TimestampNano(true) => {
                let v: Option<NaiveDateTime> = parser.produce()?;
                writer.consume(v)?;
            }
            OracleTypeSystem::Date(false)
            | OracleTypeSystem::Timestamp(false)
            | OracleTypeSystem::TimestampNano(false) => {
                let v: NaiveDateTime = parser.produce()?;
                writer.consume(v)?;
            }

            // Timestamp with timezone types
            OracleTypeSystem::TimestampTz(true) | OracleTypeSystem::TimestampTzNano(true) => {
                let v: Option<DateTime<Utc>> = parser.produce()?;
                writer.consume(v)?;
            }
            OracleTypeSystem::TimestampTz(false) | OracleTypeSystem::TimestampTzNano(false) => {
                let v: DateTime<Utc> = parser.produce()?;
                writer.consume(v)?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Oracle Raw Record Batch Iterator
    // ------------------------------------------------------------------------

    /// Iterator over Oracle raw query results (PL/SQL blocks, stored procedures) as Arrow RecordBatches.
    ///
    /// This iterator executes a raw query (such as a PL/SQL block) and yields RecordBatches.
    /// It's designed for queries that return result sets via implicit cursors
    /// (e.g., `dbms_sql.return_result` in Oracle).
    ///
    /// This implementation uses ConnectorX's `ArrowDestination` infrastructure for type-safe
    /// Arrow array building, ensuring consistent type conversions with the main ConnectorX pipeline.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use connectorx::prelude::*;
    ///
    /// let source = OracleSource::new("oracle://user:pass@host:1521/db", 1)?;
    /// let query = r#"
    ///     declare
    ///       cursor1 SYS_REFCURSOR;
    ///     begin
    ///       cursor1 := get_all_users();
    ///       dbms_sql.return_result(cursor1);
    ///     end;
    /// "#;
    ///
    /// let mut iter = OracleRawRecordBatchIterator::new(source, query)?;
    /// iter.prepare()?;
    ///
    /// for batch_result in iter {
    ///     let batch = batch_result?;
    ///     println!("Got {} rows", batch.num_rows());
    /// }
    /// ```
    pub struct OracleRawRecordBatchIterator {
        parser: OracleRawSourceParser,
        destination: ArrowDestination,
        writer: Option<<ArrowDestination as Destination>::Partition<'static>>,
        oracle_types: Vec<OracleTypeSystem>,
        arrow_schema: Arc<Schema>,
        is_finished: bool,
        pending_rows: usize,
    }

    impl OracleRawRecordBatchIterator {
        /// Create a new iterator for an Oracle raw query.
        ///
        /// # Arguments
        /// * `source` - An OracleSource with a connection pool
        /// * `query` - The raw query (e.g., PL/SQL block) to execute
        pub fn new(
            source: OracleSource,
            query: &str,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            Self::new_with_batch_size(source, query, DEFAULT_BATCH_SIZE)
        }

        /// Create a new iterator with a custom batch size.
        ///
        /// # Arguments
        /// * `source` - An OracleSource with a connection pool
        /// * `query` - The raw query (e.g., PL/SQL block) to execute
        /// * `batch_size` - Number of rows per RecordBatch
        pub fn new_with_batch_size(
            mut source: OracleSource,
            query: &str,
            batch_size: usize,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            // Execute raw query and get parser with schema
            let (parser, names, oracle_types) = source.execute_raw_query(query)?;

            // Convert Oracle types to Arrow types using the transport's mapping
            let arrow_types: Vec<ArrowTypeSystem> = oracle_types
                .iter()
                .map(|&t| {
                    <OracleArrowStreamTransport as Transport>::convert_typesystem(t)
                })
                .collect::<Result<Vec<_>, crate::errors::ConnectorXError>>()?;

            // Create ArrowDestination with the schema
            let mut destination = ArrowDestination::new_with_batch_size(batch_size);
            destination.allocate(0, &names, &arrow_types, DataOrder::RowMajor)?;

            // Get the Arrow schema before taking ownership of writer
            let arrow_schema = destination.arrow_schema();

            // Create partition writer
            let mut writers = destination.partition(1)?;
            let writer = writers.pop().unwrap();

            // We need to use unsafe to extend the lifetime of the writer
            // This is safe because we ensure the destination outlives the writer
            // by keeping both in the same struct
            let writer: <ArrowDestination as Destination>::Partition<'static> =
                unsafe { std::mem::transmute(writer) };

            Ok(Self {
                parser,
                destination,
                writer: Some(writer),
                oracle_types,
                arrow_schema,
                is_finished: false,
                pending_rows: 0,
            })
        }

        /// Get the Arrow schema for this result set.
        pub fn schema(&self) -> Arc<Schema> {
            self.arrow_schema.clone()
        }

        /// Prepare the iterator by fetching the first batch of rows.
        ///
        /// Call this before iterating to ensure data is ready.
        pub fn prepare(&mut self) -> Result<(), RawArrowError> {
            if self.pending_rows == 0 && !self.is_finished {
                let (n, finished) = self.parser.fetch_next()?;
                self.pending_rows = n;
                self.is_finished = finished && n == 0;
            }
            Ok(())
        }

        /// Process pending rows through the writer.
        fn process_rows(&mut self, nrows: usize) -> Result<(), RawArrowError> {
            let writer = self.writer.as_mut().ok_or_else(|| {
                RawArrowError::Other("Writer already finalized".to_string())
            })?;

            for _ in 0..nrows {
                for oracle_type in &self.oracle_types {
                    dispatch_oracle_value(&mut self.parser, writer, oracle_type)?;
                }
            }
            Ok(())
        }
    }

    impl Iterator for OracleRawRecordBatchIterator {
        type Item = Result<RecordBatch, RawArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            // First, check if there are any batches already available
            if let Some(batch) = self.destination.try_record_batch() {
                return Some(Ok(batch));
            }

            // If we have pending rows, process them
            if self.pending_rows > 0 {
                let nrows = self.pending_rows;
                self.pending_rows = 0;

                if let Err(e) = self.process_rows(nrows) {
                    return Some(Err(e));
                }

                // Check if processing produced any batches
                if let Some(batch) = self.destination.try_record_batch() {
                    return Some(Ok(batch));
                }
            }

            // Check if we're finished
            if self.is_finished {
                // Finalize the writer to flush any remaining data
                if let Some(mut writer) = self.writer.take() {
                    if let Err(e) = writer.finalize() {
                        return Some(Err(RawArrowError::ArrowDestination(e)));
                    }
                }
                // Get any remaining batches
                return self.destination.try_record_batch().map(Ok);
            }

            // Fetch more rows
            match self.parser.fetch_next() {
                Ok((n, finished)) => {
                    self.is_finished = finished;

                    if n == 0 {
                        // No more rows, finalize
                        if let Some(mut writer) = self.writer.take() {
                            if let Err(e) = writer.finalize() {
                                return Some(Err(RawArrowError::ArrowDestination(e)));
                            }
                        }
                        return self.destination.try_record_batch().map(Ok);
                    }

                    // Process the fetched rows
                    if let Err(e) = self.process_rows(n) {
                        return Some(Err(e));
                    }

                    // Return any batch that was produced
                    if let Some(batch) = self.destination.try_record_batch() {
                        Some(Ok(batch))
                    } else if finished {
                        // If finished but no batch yet, finalize to flush remaining
                        if let Some(mut writer) = self.writer.take() {
                            if let Err(e) = writer.finalize() {
                                return Some(Err(RawArrowError::ArrowDestination(e)));
                            }
                        }
                        self.destination.try_record_batch().map(Ok)
                    } else {
                        // Continue fetching - call next recursively
                        self.next()
                    }
                }
                Err(e) => {
                    self.is_finished = true;
                    Some(Err(RawArrowError::Oracle(e)))
                }
            }
        }
    }
}

// Re-export Oracle types at module level
#[cfg(feature = "src_oracle")]
pub use oracle::OracleRawRecordBatchIterator;

// ============================================================================
// Future: MSSQL Support
// ============================================================================
// When implementing MSSQL raw support, add a similar `mssql` submodule here with:
// - MsSQLRawRecordBatchIterator
// - Similar dispatch function for MSSQL types
