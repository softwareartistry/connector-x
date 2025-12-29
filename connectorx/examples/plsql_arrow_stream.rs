//! Example demonstrating how to stream PL/SQL query results as Arrow RecordBatches.
//!
//! This example shows how to use the `OracleRawRecordBatchIterator` to execute
//! PL/SQL blocks that return result sets via implicit cursors (`dbms_sql.return_result`).
//!
//! # Usage
//!
//! ```bash
//! cargo run --example plsql_arrow_stream --features "src_oracle,dst_arrow"
//! ```

use connectorx::prelude::{OracleRawRecordBatchIterator, OracleSource, RawArrowError};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Connection string - adjust as needed
    let conn_str = "oracle://user:password@host:1521/service";

    // Create Oracle source with 1 connection (no parallelism needed for raw queries)
    let source = OracleSource::new(conn_str, 1)?;

    // Example PL/SQL query that returns a cursor via dbms_sql.return_result
    let query = r#"
        declare
          cursor1 SYS_REFCURSOR;
        begin
          cursor1 := get_all_users();
          dbms_sql.return_result(cursor1);
        end;
    "#;

    // Create the iterator
    let mut iter = OracleRawRecordBatchIterator::new(source, query)?;

    // Prepare (fetches first batch of rows)
    iter.prepare()?;

    // Get the Arrow schema
    let schema = iter.schema();
    println!("Schema: {:?}", schema);
    println!();

    // Iterate over batches
    let mut total_rows = 0;
    for (batch_num, batch_result) in iter.enumerate() {
        match batch_result {
            Ok(batch) => {
                println!("Batch {}: {} rows", batch_num + 1, batch.num_rows());
                total_rows += batch.num_rows();
                println!("Batch Result \n {:?}", batch);
            }
            Err(e) => {
                eprintln!("Error reading batch: {}", e);
                return Err(Box::new(e));
            }
        }
    }

    println!();
    println!("Total rows: {}", total_rows);
    println!("PL/SQL streaming complete!");

    Ok(())
}

/// Example of converting the iterator to a Stream for Arrow Flight.
///
/// This is what you'd use in server code:
///
/// ```ignore
/// use futures::Stream;
/// use std::pin::Pin;
/// use arrow_flight::error::FlightError;
///
/// fn to_flight_stream(
///     iter: OracleRawRecordBatchIterator,
/// ) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>> {
///     let stream = async_stream::stream! {
///         for batch_result in iter {
///             match batch_result {
///                 Ok(batch) => yield Ok(batch),
///                 Err(e) => yield Err(FlightError::ExternalError(Box::new(e))),
///             }
///         }
///     };
///     Box::pin(stream)
/// }
/// ```
#[allow(dead_code)]
fn _flight_stream_example() {}
