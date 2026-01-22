mod errors;
mod typesystem;

use std::collections::HashMap;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

pub use self::errors::OracleSourceError;
pub use self::typesystem::OracleTypeSystem;
use crate::constants::{DB_BUFFER_SIZE, ORACLE_ARRAY_SIZE};
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, RawSource, Source, SourcePartition},
    sql::{count_query, limit1_query_oracle, CXQuery},
    utils::DummyBox,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use fehler::{throw, throws};
use log::debug;
use owning_ref::OwningHandle;
use r2d2::{Pool, PooledConnection};
use r2d2_oracle::oracle::ResultSet;
use r2d2_oracle::{
    oracle::{Connector, Row, Statement},
    OracleConnectionManager,
};
use rust_decimal::Decimal;
use sqlparser::dialect::Dialect;
use url::Url;
use urlencoding::decode;

type OracleManager = OracleConnectionManager;
type OracleConn = PooledConnection<OracleManager>;

#[derive(Debug)]
pub struct OracleDialect {}

// implementation copy from AnsiDialect
impl Dialect for OracleDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }
}

#[derive(Clone)]
pub struct OracleSource {
    pool: Pool<OracleManager>,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<OracleTypeSystem>,
    array_size: Option<u32>,
    current_schema: Option<String>,
}

#[throws(OracleSourceError)]
pub fn connect_oracle(conn: &Url) -> Connector {
    let user = decode(conn.username())?.into_owned();
    let password = decode(conn.password().unwrap_or(""))?.into_owned();
    let host = decode(conn.host_str().unwrap_or("localhost"))?.into_owned();

    let params: HashMap<String, String> = conn.query_pairs().into_owned().collect();

    let conn_str = if params.get("alias").map_or(false, |v| v == "true") {
        host.clone()
    } else {
        let port = conn.port().unwrap_or(1521);
        let path = decode(conn.path())?.into_owned();
        format!("//{}:{}{}", host, port, path)
    };

    let mut connector = oracle::Connector::new(user.as_str(), password.as_str(), conn_str.as_str());
    if user.is_empty() && password.is_empty() {
        debug!("No username or password provided, assuming system auth.");
        connector.external_auth(true);
    }
    connector
}

impl OracleSource {
    #[throws(OracleSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        let conn = Url::parse(conn)?;
        let connector = connect_oracle(&conn)?;
        let manager = OracleConnectionManager::from_connector(connector);
        let pool = r2d2::Pool::builder()
            .max_size(nconn as u32)
            .build(manager)?;

        let params: HashMap<String, String> = conn.query_pairs().into_owned().collect();
        let current_schema = params.get("schema").cloned();

        Self {
            pool,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            array_size: None,
            current_schema,
        }
    }

    /// Set the Oracle array size for prefetch_rows and fetch_array_size.
    /// If not set, defaults to ORACLE_ARRAY_SIZE constant (1024).
    pub fn set_array_size(&mut self, size: u32) {
        self.array_size = Some(size);
    }

    /// Get the configured array size, or None if using default.
    pub fn array_size(&self) -> Option<u32> {
        self.array_size
    }
    pub fn get_conn(&self) -> Result<OracleConn, OracleSourceError> {
        let conn = self.pool.get()?;
        if let Some(schema) = &self.current_schema {
            conn.set_current_schema(schema)?;
        }
        Ok(conn)
    }
}

impl Source for OracleSource
where
    OracleSourcePartition:
        SourcePartition<TypeSystem = OracleTypeSystem, Error = OracleSourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = OracleSourcePartition;
    type TypeSystem = OracleTypeSystem;
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }
    }

    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    #[throws(OracleSourceError)]
    fn fetch_metadata(&mut self) {
        assert!(!self.queries.is_empty());

        let conn = self.get_conn()?;
        for (i, query) in self.queries.iter().enumerate() {
            // assuming all the partition queries yield same schema
            // without rownum = 1, derived type might be wrong
            // example: select avg(test_int), test_char from test_table group by test_char
            // -> (NumInt, Char) instead of (NumtFloat, Char)
            match conn.query(limit1_query_oracle(query)?.as_str(), &[]) {
                Ok(rows) => {
                    let (names, types) = rows
                        .column_info()
                        .iter()
                        .map(|col| {
                            (
                                col.name().to_string(),
                                OracleTypeSystem::from(col.oracle_type()),
                            )
                        })
                        .unzip();
                    self.names = names;
                    self.schema = types;
                    return;
                }
                Err(e) if i == self.queries.len() - 1 => {
                    // tried the last query but still get an error
                    debug!("cannot get metadata for '{}': {}", query, e);
                    throw!(e);
                }
                Err(_) => {}
            }
        }
        // tried all queries but all get empty result set
        let iter = conn.query(self.queries[0].as_str(), &[])?;
        let (names, types) = iter
            .column_info()
            .iter()
            .map(|col| (col.name().to_string(), OracleTypeSystem::VarChar(false)))
            .unzip();
        self.names = names;
        self.schema = types;
    }

    #[throws(OracleSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => {
                let cxq = CXQuery::Naked(q.clone());
                let conn = self.get_conn()?;

                let nrows = conn
                    .query_row_as::<usize>(count_query(&cxq, &OracleDialect {})?.as_str(), &[])?;
                Some(nrows)
            }
            None => None,
        }
    }

    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.schema.clone()
    }

    #[throws(OracleSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        let mut ret = vec![];
        for query in &self.queries {
            let conn = self.get_conn()?;
            ret.push(OracleSourcePartition::new(
                conn,
                &query,
                &self.schema,
                self.array_size,
            ));
        }
        ret
    }
}

/// Schema information sent from the streaming thread
struct StreamingSchema {
    names: Vec<String>,
    types: Vec<OracleTypeSystem>,
}

impl RawSource for OracleSource {
    type Parser = OracleRawSourceParser;

    fn execute_raw_query(
        &mut self,
        query: &str,
    ) -> Result<(Self::Parser, Vec<String>, Vec<OracleTypeSystem>), OracleSourceError> {
        // Create bounded channel for row streaming (provides backpressure)
        let (row_sender, row_receiver): (
            SyncSender<oracle::Result<Row>>,
            Receiver<oracle::Result<Row>>,
        ) = sync_channel(RAW_STREAMING_BUFFER_SIZE);

        // Create channel for schema (one-shot style)
        let (schema_sender, schema_receiver) =
            sync_channel::<Result<StreamingSchema, OracleSourceError>>(1);

        // Get connection from pool for the background thread
        let conn = self.pool.get()?;
        let query_owned = query.to_string();

        // Spawn background thread that owns Oracle resources and streams rows
        let thread_handle = thread::spawn(move || {
            // Execute query and stream results
            let result = (|| -> Result<(), OracleSourceError> {
                let stmt = conn.statement(&query_owned).build()?;
                let mut boxed_stmt = Box::new(stmt);
                boxed_stmt.execute(&[])?;

                if let Some(mut cursor) = boxed_stmt.implicit_result()? {
                    let result_set = cursor.query()?;

                    // Extract schema from result set
                    let col_info: Vec<_> = result_set
                        .column_info()
                        .iter()
                        .map(|c| {
                            (
                                c.name().to_string(),
                                OracleTypeSystem::from(c.oracle_type()),
                            )
                        })
                        .collect();
                    let (names, types): (Vec<_>, Vec<_>) = col_info.into_iter().unzip();

                    // Send schema back to main thread
                    let _ = schema_sender.send(Ok(StreamingSchema { names, types }));

                    // Stream rows through bounded channel
                    // The bounded channel provides backpressure - if consumer is slow,
                    // this will block until there's room in the buffer
                    for row_result in result_set {
                        // If receiver is dropped (consumer stopped), stop streaming
                        if row_sender.send(row_result).is_err() {
                            break;
                        }
                    }
                } else {
                    let _ = schema_sender.send(Err(OracleSourceError::ConnectorXError(
                        ConnectorXError::Other(anyhow::anyhow!("No implicit result")),
                    )));
                }
                Ok(())
            })();

            // If there was an error before we could send schema, send the error
            if let Err(e) = result {
                let _ = schema_sender.send(Err(e));
            }
            // row_sender is dropped here, which signals end of stream to receiver
        });

        // Wait for schema from background thread
        let schema = schema_receiver.recv().map_err(|_| {
            OracleSourceError::ConnectorXError(ConnectorXError::Other(anyhow::anyhow!(
                "Failed to receive schema from streaming thread"
            )))
        })??;

        let ncols = schema.names.len();
        let parser = OracleRawSourceParser::new_streaming(row_receiver, ncols, thread_handle);

        Ok((parser, schema.names, schema.types))
    }
}

/// Default buffer size for streaming channel (number of rows)
const RAW_STREAMING_BUFFER_SIZE: usize = 1024;

pub struct OracleRawSourceParser {
    receiver: Receiver<oracle::Result<Row>>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
    /// Thread handle for the background streaming thread.
    /// Stored to ensure proper cleanup on drop.
    _thread_handle: Option<JoinHandle<()>>,
}

unsafe impl Send for OracleRawSourceParser {}
unsafe impl Sync for OracleRawSourceParser {}

impl OracleRawSourceParser {
    /// Create a new streaming parser from a channel receiver.
    /// The background thread sends rows through the channel.
    pub fn new_streaming(
        receiver: Receiver<oracle::Result<Row>>,
        ncols: usize,
        thread_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            receiver,
            rowbuf: Vec::with_capacity(RAW_STREAMING_BUFFER_SIZE),
            ncols,
            current_col: 0,
            current_row: 0,
            is_finished: false,
            _thread_handle: Some(thread_handle),
        }
    }

    #[throws(OracleSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for OracleRawSourceParser {
    type TypeSystem = OracleTypeSystem;
    type Error = OracleSourceError;

    fn fetch_next(&mut self) -> Result<(usize, bool), Self::Error> {
        if self.is_finished && self.rowbuf.is_empty() {
            // Query is finished - shrink buffer to release memory
            if self.rowbuf.capacity() > 0 {
                self.rowbuf = Vec::new();
            }
            return Ok((0, true));
        }

        // Clear buffer
        self.rowbuf.clear();
        self.current_row = 0;
        self.current_col = 0;

        let batch_size = RAW_STREAMING_BUFFER_SIZE;

        // Fetch batch from channel receiver
        for _ in 0..batch_size {
            match self.receiver.recv() {
                Ok(Ok(row)) => self.rowbuf.push(row),
                Ok(Err(e)) => return Err(OracleSourceError::from(e)),
                Err(_) => {
                    // Channel closed - no more data
                    self.is_finished = true;
                    // Shrink buffer capacity when query completes to free memory
                    self.rowbuf.shrink_to_fit();
                    break;
                }
            }
        }
        Ok((self.rowbuf.len(), self.is_finished))
    }
}

macro_rules! impl_produce_raw {
    ($($t: ty,)+) => {
        $(
            impl<'r> Produce<'r, $t> for OracleRawSourceParser {
                type Error = OracleSourceError;

                #[throws(OracleSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx)?;
                    res
                }
            }

            impl<'r> Produce<'r, Option<$t>> for OracleRawSourceParser {
                type Error = OracleSourceError;

                #[throws(OracleSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx)?;
                    res
                }
            }
        )+
    };
}

impl_produce_raw!(
    i64,
    f64,
    String,
    NaiveDate,
    NaiveDateTime,
    DateTime<Utc>,
    Vec<u8>,
);

impl<'r> Produce<'r, Decimal> for OracleRawSourceParser {
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn produce(&'r mut self) -> Decimal {
        let (ridx, cidx) = self.next_loc()?;
        let s: String = self.rowbuf[ridx].get(cidx)?;
        let res = s.parse::<Decimal>()?;
        res
    }
}

impl<'r> Produce<'r, Option<Decimal>> for OracleRawSourceParser {
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn produce(&'r mut self) -> Option<Decimal> {
        let (ridx, cidx) = self.next_loc()?;
        let s: Option<String> = self.rowbuf[ridx].get(cidx)?;
        match s {
            Some(val) => Some(val.parse::<Decimal>()?),
            None => None,
        }
    }
}

pub struct OracleSourcePartition {
    conn: OracleConn,
    query: CXQuery<String>,
    schema: Vec<OracleTypeSystem>,
    nrows: usize,
    ncols: usize,
    array_size: Option<u32>,
}

impl OracleSourcePartition {
    pub fn new(
        conn: OracleConn,
        query: &CXQuery<String>,
        schema: &[OracleTypeSystem],
        array_size: Option<u32>,
    ) -> Self {
        Self {
            conn,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
            array_size,
        }
    }
}

impl SourcePartition for OracleSourcePartition {
    type TypeSystem = OracleTypeSystem;
    type Parser<'a> = OracleTextSourceParser<'a>;
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn result_rows(&mut self) {
        self.nrows = self
            .conn
            .query_row_as::<usize>(count_query(&self.query, &OracleDialect {})?.as_str(), &[])?;
    }

    #[throws(OracleSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let query = self.query.clone();

        // let iter = self.conn.query(query.as_str(), &[])?;
        OracleTextSourceParser::new(&self.conn, query.as_str(), &self.schema, self.array_size)?
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

unsafe impl<'a> Send for OracleTextSourceParser<'a> {}

pub struct OracleTextSourceParser<'a> {
    rows: OwningHandle<Box<Statement>, DummyBox<ResultSet<'a, Row>>>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> OracleTextSourceParser<'a> {
    #[throws(OracleSourceError)]
    pub fn new(
        conn: &'a OracleConn,
        query: &str,
        schema: &[OracleTypeSystem],
        array_size: Option<u32>,
    ) -> Self {
        let size = array_size.unwrap_or(ORACLE_ARRAY_SIZE);
        let stmt = conn
            .statement(query)
            .prefetch_rows(size)
            .fetch_array_size(size)
            .build()?;
        let rows: OwningHandle<Box<Statement>, DummyBox<ResultSet<'a, Row>>> =
            OwningHandle::new_with_fn(Box::new(stmt), |stmt: *const Statement| unsafe {
                DummyBox((*(stmt as *mut Statement)).query(&[]).unwrap())
            });

        Self {
            rows,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(OracleSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for OracleTextSourceParser<'a> {
    type TypeSystem = OracleTypeSystem;
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            // Query is finished - shrink buffer to release memory
            if self.rowbuf.capacity() > 0 {
                self.rowbuf = Vec::new();
            }
            return (0, self.is_finished);
        }

        if !self.rowbuf.is_empty() {
            self.rowbuf.drain(..);
        }
        for _ in 0..DB_BUFFER_SIZE {
            if let Some(item) = (*self.rows).next() {
                self.rowbuf.push(item?);
            } else {
                self.is_finished = true;
                // Shrink buffer capacity when query completes to free memory
                self.rowbuf.shrink_to_fit();
                break;
            }
        }
        self.current_row = 0;
        self.current_col = 0;
        (self.rowbuf.len(), self.is_finished)
    }
}

macro_rules! impl_produce_text {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for OracleTextSourceParser<'a> {
                type Error = OracleSourceError;

                #[throws(OracleSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx)?;
                    res
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for OracleTextSourceParser<'a> {
                type Error = OracleSourceError;

                #[throws(OracleSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx)?;
                    res
                }
            }
        )+
    };
}

impl_produce_text!(
    i64,
    f64,
    String,
    NaiveDate,
    NaiveDateTime,
    DateTime<Utc>,
    Vec<u8>,
);

// Manual implementation for Decimal since Oracle doesn't support it directly via FromSql
impl<'r, 'a> Produce<'r, Decimal> for OracleTextSourceParser<'a> {
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn produce(&'r mut self) -> Decimal {
        let (ridx, cidx) = self.next_loc()?;
        let s: String = self.rowbuf[ridx].get(cidx)?;
        let res = s.parse::<Decimal>()?;
        res
    }
}

impl<'r, 'a> Produce<'r, Option<Decimal>> for OracleTextSourceParser<'a> {
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn produce(&'r mut self) -> Option<Decimal> {
        let (ridx, cidx) = self.next_loc()?;
        let s: Option<String> = self.rowbuf[ridx].get(cidx)?;
        match s {
            Some(val) => Some(val.parse::<Decimal>()?),
            None => None,
        }
    }
}
