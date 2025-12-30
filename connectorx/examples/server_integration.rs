//! Reference implementation for integrating raw Arrow streaming with your server.
//!
//! Copy the `stream_raw_query` method below into your ConnectionSource implementation.
//!
//! # Required imports in your server code:
//!
//! ```rust
//! use connectorx::prelude::{
//!     OracleRawRecordBatchIterator, OracleSource, RawArrowError,
//!     // ... your existing imports
//! };
//! ```

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

// These would come from arrow_flight in your server
// use arrow_flight::error::FlightError;
// use futures::Stream;

/// Placeholder for FlightError - use arrow_flight::error::FlightError in your code
#[derive(Debug)]
pub struct FlightError(Box<dyn std::error::Error + Send + Sync>);

impl FlightError {
    pub fn external_error(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        FlightError(Box::new(e))
    }
}

impl std::fmt::Display for FlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Flight error: {}", self.0)
    }
}

impl std::error::Error for FlightError {}

// ============================================================================
// Add this method to your ConnectionSource implementation
// ============================================================================

/*
impl ConnectionSource {
    /// Stream results from a raw query (PL/SQL block, stored procedure).
    ///
    /// This method bypasses ConnectorX's parallel query execution and directly
    /// executes raw queries that return implicit result sets.
    ///
    /// # Supported databases:
    /// - Oracle: PL/SQL blocks using `dbms_sql.return_result`
    /// - MSSQL: (future) Stored procedures
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = r#"
    ///     declare
    ///       cursor1 SYS_REFCURSOR;
    ///     begin
    ///       cursor1 := get_all_users();
    ///       dbms_sql.return_result(cursor1);
    ///     end;
    /// "#;
    ///
    /// let (schema, stream) = connection.stream_raw_query(query.to_string())?;
    /// ```
    pub fn stream_raw_query(
        self,
        query: String,
    ) -> Result<
        (
            Arc<Schema>,
            Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>>,
        ),
        Box<dyn std::error::Error>,
    > {
        match self {
            ConnectionSource::Oracle(source) => {
                // Use the new OracleRawRecordBatchIterator from connectorx
                let mut iter = OracleRawRecordBatchIterator::new(source, &query)?;
                iter.prepare()?;
                let schema = iter.schema();

                let stream = async_stream::stream! {
                    for batch_result in iter {
                        match batch_result {
                            Ok(batch) => {
                                // Apply interchange conversion if needed for arrow version compatibility
                                // let mut v = Interchange::from_arrow_54(vec![batch])?.to_arrow_56()?;
                                // yield Ok(v.remove(0));
                                yield Ok(batch);
                            }
                            Err(e) => {
                                yield Err(FlightError::ExternalError(Box::new(e)));
                            }
                        }
                    }
                };

                Ok((schema, Box::pin(stream)))
            }
            ConnectionSource::MsSQL(_source) => {
                // Future: Implement MsSQLRawRecordBatchIterator
                Err("Raw queries for MSSQL not yet implemented".into())
            }
            _ => Err("Raw queries not supported for this database type".into()),
        }
    }
}
*/

// ============================================================================
// Complete implementation example (compilable reference)
// ============================================================================

use connectorx::prelude::{OracleRawRecordBatchIterator, OracleSource, RawArrowError};

/// Example function showing how to use OracleRawRecordBatchIterator
pub fn execute_oracle_raw_query(
    source: OracleSource,
    query: &str,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), Box<dyn std::error::Error + Send + Sync>> {
    let mut iter = OracleRawRecordBatchIterator::new(source, query)?;
    iter.prepare()?;

    let schema = iter.schema();
    let mut batches = Vec::new();

    for batch_result in iter {
        batches.push(batch_result?);
    }

    Ok((schema, batches))
}

fn main() {
    println!("This is a reference implementation file.");
    println!("Copy the stream_raw_query method into your ConnectionSource.");
}

