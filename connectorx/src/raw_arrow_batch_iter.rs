//! Raw Arrow batch iterator for streaming results from raw/stored procedure queries.
//!
//! This module provides iterators for streaming Arrow RecordBatches from raw query results,
//! such as Oracle PL/SQL blocks or MSSQL stored procedures that return result sets via
//! implicit cursors.
//!
//! Unlike the standard `ArrowBatchIter` which uses ConnectorX's parallel query execution,
//! these iterators work with single-threaded raw query execution for cases where
//! parallelism isn't applicable (e.g. stored procedures).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date64Builder, Float64Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime, Utc};

#[cfg(feature = "src_oracle")]
use crate::sources::oracle::{OracleRawSourceParser, OracleSourceError, OracleTypeSystem};
#[cfg(feature = "src_oracle")]
use crate::sources::{PartitionParser, Produce, RawSource};

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
    /// Generic error
    Other(String),
}

impl std::fmt::Display for RawArrowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "src_oracle")]
            RawArrowError::Oracle(e) => write!(f, "Oracle error: {}", e),
            RawArrowError::Arrow(e) => write!(f, "Arrow error: {}", e),
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
    use crate::sources::oracle::OracleSource;

    /// Build Arrow schema from Oracle column names and types
    pub fn build_arrow_schema(names: &[String], types: &[OracleTypeSystem]) -> Arc<Schema> {
        let fields: Vec<Field> = names
            .iter()
            .zip(types.iter())
            .map(|(name, oracle_type)| {
                let (data_type, nullable) = oracle_type_to_arrow(oracle_type);
                Field::new(name, data_type, nullable)
            })
            .collect();

        Arc::new(Schema::new(fields))
    }

    /// Convert Oracle type system to Arrow data type
    fn oracle_type_to_arrow(oracle_type: &OracleTypeSystem) -> (DataType, bool) {
        match oracle_type {
            OracleTypeSystem::NumInt(nullable) => (DataType::Int64, *nullable),
            OracleTypeSystem::NumFloat(nullable)
            | OracleTypeSystem::Float(nullable)
            | OracleTypeSystem::BinaryFloat(nullable)
            | OracleTypeSystem::BinaryDouble(nullable) => (DataType::Float64, *nullable),
            OracleTypeSystem::VarChar(nullable)
            | OracleTypeSystem::Char(nullable)
            | OracleTypeSystem::NVarChar(nullable)
            | OracleTypeSystem::NChar(nullable)
            | OracleTypeSystem::Clob(nullable) => (DataType::LargeUtf8, *nullable),
            OracleTypeSystem::Blob(nullable) => (DataType::LargeBinary, *nullable),
            OracleTypeSystem::Date(nullable) | OracleTypeSystem::Timestamp(nullable) => {
                (DataType::Date64, *nullable)
            }
            OracleTypeSystem::TimestampNano(nullable) => (DataType::Date64, *nullable),
            OracleTypeSystem::TimestampTz(nullable)
            | OracleTypeSystem::TimestampTzNano(nullable) => (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                *nullable,
            ),
        }
    }

    // ------------------------------------------------------------------------
    // Array Builders
    // ------------------------------------------------------------------------

    /// Trait for building Arrow arrays from Oracle parser
    trait OracleArrayBuilder: Send {
        fn append_value(&mut self, parser: &mut OracleRawSourceParser) -> Result<(), RawArrowError>;
        fn finish(&mut self) -> ArrayRef;
    }

    struct Int64ArrayBuilder {
        builder: Int64Builder,
        nullable: bool,
    }

    impl Int64ArrayBuilder {
        fn new(capacity: usize, nullable: bool) -> Self {
            Self {
                builder: Int64Builder::with_capacity(capacity),
                nullable,
            }
        }
    }

    impl OracleArrayBuilder for Int64ArrayBuilder {
        fn append_value(
            &mut self,
            parser: &mut OracleRawSourceParser,
        ) -> Result<(), RawArrowError> {
            if self.nullable {
                let val: Option<i64> = parser.produce()?;
                self.builder.append_option(val);
            } else {
                let val: i64 = parser.produce()?;
                self.builder.append_value(val);
            }
            Ok(())
        }

        fn finish(&mut self) -> ArrayRef {
            Arc::new(self.builder.finish())
        }
    }

    struct Float64ArrayBuilder {
        builder: Float64Builder,
        nullable: bool,
    }

    impl Float64ArrayBuilder {
        fn new(capacity: usize, nullable: bool) -> Self {
            Self {
                builder: Float64Builder::with_capacity(capacity),
                nullable,
            }
        }
    }

    impl OracleArrayBuilder for Float64ArrayBuilder {
        fn append_value(
            &mut self,
            parser: &mut OracleRawSourceParser,
        ) -> Result<(), RawArrowError> {
            if self.nullable {
                let val: Option<f64> = parser.produce()?;
                self.builder.append_option(val);
            } else {
                let val: f64 = parser.produce()?;
                self.builder.append_value(val);
            }
            Ok(())
        }

        fn finish(&mut self) -> ArrayRef {
            Arc::new(self.builder.finish())
        }
    }

    struct StringArrayBuilder {
        builder: LargeStringBuilder,
        nullable: bool,
    }

    impl StringArrayBuilder {
        fn new(capacity: usize, nullable: bool) -> Self {
            Self {
                builder: LargeStringBuilder::with_capacity(capacity, capacity * 32),
                nullable,
            }
        }
    }

    impl OracleArrayBuilder for StringArrayBuilder {
        fn append_value(
            &mut self,
            parser: &mut OracleRawSourceParser,
        ) -> Result<(), RawArrowError> {
            if self.nullable {
                let val: Option<String> = parser.produce()?;
                self.builder.append_option(val);
            } else {
                let val: String = parser.produce()?;
                self.builder.append_value(&val);
            }
            Ok(())
        }

        fn finish(&mut self) -> ArrayRef {
            Arc::new(self.builder.finish())
        }
    }

    struct BinaryArrayBuilder {
        builder: LargeBinaryBuilder,
        nullable: bool,
    }

    impl BinaryArrayBuilder {
        fn new(capacity: usize, nullable: bool) -> Self {
            Self {
                builder: LargeBinaryBuilder::with_capacity(capacity, capacity * 64),
                nullable,
            }
        }
    }

    impl OracleArrayBuilder for BinaryArrayBuilder {
        fn append_value(
            &mut self,
            parser: &mut OracleRawSourceParser,
        ) -> Result<(), RawArrowError> {
            if self.nullable {
                let val: Option<Vec<u8>> = parser.produce()?;
                self.builder.append_option(val);
            } else {
                let val: Vec<u8> = parser.produce()?;
                self.builder.append_value(&val);
            }
            Ok(())
        }

        fn finish(&mut self) -> ArrayRef {
            Arc::new(self.builder.finish())
        }
    }

    struct Date64ArrayBuilder {
        builder: Date64Builder,
        nullable: bool,
    }

    impl Date64ArrayBuilder {
        fn new(capacity: usize, nullable: bool) -> Self {
            Self {
                builder: Date64Builder::with_capacity(capacity),
                nullable,
            }
        }
    }

    impl OracleArrayBuilder for Date64ArrayBuilder {
        fn append_value(
            &mut self,
            parser: &mut OracleRawSourceParser,
        ) -> Result<(), RawArrowError> {
            if self.nullable {
                let val: Option<NaiveDateTime> = parser.produce()?;
                self.builder
                    .append_option(val.map(|dt| dt.and_utc().timestamp_millis()));
            } else {
                let val: NaiveDateTime = parser.produce()?;
                self.builder.append_value(val.and_utc().timestamp_millis());
            }
            Ok(())
        }

        fn finish(&mut self) -> ArrayRef {
            Arc::new(self.builder.finish())
        }
    }

    struct TimestampTzArrayBuilder {
        builder: TimestampMicrosecondBuilder,
        nullable: bool,
    }

    impl TimestampTzArrayBuilder {
        fn new(capacity: usize, nullable: bool) -> Self {
            let builder: TimestampMicrosecondBuilder =
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC");
            Self { builder, nullable }
        }
    }

    impl OracleArrayBuilder for TimestampTzArrayBuilder {
        fn append_value(
            &mut self,
            parser: &mut OracleRawSourceParser,
        ) -> Result<(), RawArrowError> {
            if self.nullable {
                let val: Option<DateTime<Utc>> = parser.produce()?;
                self.builder
                    .append_option(val.map(|dt| dt.timestamp_micros()));
            } else {
                let val: DateTime<Utc> = parser.produce()?;
                self.builder.append_value(val.timestamp_micros());
            }
            Ok(())
        }

        fn finish(&mut self) -> ArrayRef {
            Arc::new(self.builder.finish())
        }
    }

    /// Create array builders for each column based on Oracle types
    fn create_builders(
        types: &[OracleTypeSystem],
        capacity: usize,
    ) -> Vec<Box<dyn OracleArrayBuilder>> {
        types
            .iter()
            .map(|t| -> Box<dyn OracleArrayBuilder> {
                match t {
                    OracleTypeSystem::NumInt(nullable) => {
                        Box::new(Int64ArrayBuilder::new(capacity, *nullable))
                    }
                    OracleTypeSystem::NumFloat(nullable)
                    | OracleTypeSystem::Float(nullable)
                    | OracleTypeSystem::BinaryFloat(nullable)
                    | OracleTypeSystem::BinaryDouble(nullable) => {
                        Box::new(Float64ArrayBuilder::new(capacity, *nullable))
                    }
                    OracleTypeSystem::VarChar(nullable)
                    | OracleTypeSystem::Char(nullable)
                    | OracleTypeSystem::NVarChar(nullable)
                    | OracleTypeSystem::NChar(nullable)
                    | OracleTypeSystem::Clob(nullable) => {
                        Box::new(StringArrayBuilder::new(capacity, *nullable))
                    }
                    OracleTypeSystem::Blob(nullable) => {
                        Box::new(BinaryArrayBuilder::new(capacity, *nullable))
                    }
                    OracleTypeSystem::Date(nullable)
                    | OracleTypeSystem::Timestamp(nullable)
                    | OracleTypeSystem::TimestampNano(nullable) => {
                        Box::new(Date64ArrayBuilder::new(capacity, *nullable))
                    }
                    OracleTypeSystem::TimestampTz(nullable)
                    | OracleTypeSystem::TimestampTzNano(nullable) => {
                        Box::new(TimestampTzArrayBuilder::new(capacity, *nullable))
                    }
                }
            })
            .collect()
    }

    /// Build a RecordBatch from parser data
    fn build_record_batch(
        parser: &mut OracleRawSourceParser,
        schema: &Arc<Schema>,
        types: &[OracleTypeSystem],
        nrows: usize,
    ) -> Result<RecordBatch, RawArrowError> {
        let mut builders = create_builders(types, nrows);

        // Process each row
        for _ in 0..nrows {
            for builder in builders.iter_mut() {
                builder.append_value(parser)?;
            }
        }

        // Finish all builders
        let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

        Ok(RecordBatch::try_new(schema.clone(), columns)?)
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
        schema: Arc<Schema>,
        types: Vec<OracleTypeSystem>,
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
            mut source: OracleSource,
            query: &str,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            // Execute raw query and get parser with schema
            let (parser, names, types) = source.execute_raw_query(query)?;

            // Build Arrow schema from Oracle types
            let schema = build_arrow_schema(&names, &types);

            Ok(Self {
                parser,
                schema,
                types,
                is_finished: false,
                pending_rows: 0,
            })
        }

        /// Get the Arrow schema for this result set.
        pub fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
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
    }

    impl Iterator for OracleRawRecordBatchIterator {
        type Item = Result<RecordBatch, RawArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            // If we have pending rows, build a batch from them
            if self.pending_rows > 0 {
                let nrows = self.pending_rows;
                self.pending_rows = 0;

                match build_record_batch(&mut self.parser, &self.schema, &self.types, nrows) {
                    Ok(batch) => return Some(Ok(batch)),
                    Err(e) => return Some(Err(e)),
                }
            }

            // Check if we're finished
            if self.is_finished {
                return None;
            }

            // Fetch more rows
            match self.parser.fetch_next() {
                Ok((n, finished)) => {
                    self.is_finished = finished;

                    if n == 0 {
                        return None;
                    }

                    match build_record_batch(&mut self.parser, &self.schema, &self.types, n) {
                        Ok(batch) => Some(Ok(batch)),
                        Err(e) => Some(Err(e)),
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
pub use oracle::{build_arrow_schema as build_oracle_arrow_schema, OracleRawRecordBatchIterator};

// ============================================================================
// Future: MSSQL Support
// ============================================================================
// When implementing MSSQL raw support, add a similar `mssql` submodule here with:
// - MsSQLRawRecordBatchIterator
// - MSSQL array builders
// - build_mssql_arrow_schema function

