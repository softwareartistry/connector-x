mod errors;
mod typesystem;

use std::collections::HashMap;

pub use self::errors::OracleSourceError;
pub use self::typesystem::OracleTypeSystem;
use crate::constants::{DB_BUFFER_SIZE, ORACLE_ARRAY_SIZE};
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourcePartition, RawSource},
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

        Self {
            pool,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
        }
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

        let conn = self.pool.get()?;
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
                let conn = self.pool.get()?;

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
        for query in self.queries {
            let conn = self.pool.get()?;
            ret.push(OracleSourcePartition::new(conn, &query, &self.schema));
        }
        ret
    }
}

impl RawSource for OracleSource {
    type Parser = OracleRawSourceParser;

    fn execute_raw_query(&mut self, query: &str) 
        -> Result<(Self::Parser, Vec<String>, Vec<OracleTypeSystem>), OracleSourceError> 
    {
        let conn = self.pool.get()?;
        let stmt = conn.statement(query).build()?;
        let mut boxed_stmt = Box::new(stmt);
        boxed_stmt.execute(&[])?;
    
        let (rows, names, types) = if let Some(mut cursor) = boxed_stmt.implicit_result()? {
            let result_set = cursor.query()?;
            
            // Schema
            let col_info: Vec<_> = result_set.column_info()
                    .iter()
                    .map(|c| (c.name().to_string(), OracleTypeSystem::from(c.oracle_type())))
                    .collect();
            let (n, t): (Vec<_>, Vec<_>) = col_info.into_iter().unzip();
            
            // Data (rows, names, types)
            let r = result_set.collect::<Result<Vec<_>, _>>()?;
            (r, n, t)
        } else {
            return Err(OracleSourceError::ConnectorXError(
                ConnectorXError::Other(anyhow::anyhow!("No implicit result"))
            ));
        };

        // Create Parser
        let ncols = names.len();
        let iter = rows.into_iter().map(Ok);
        let parser = OracleRawSourceParser::new(Box::new(iter), ncols);

        Ok((parser, names, types))
    }
}

pub struct OracleRawSourceParser {
    iter: Box<dyn Iterator<Item = oracle::Result<Row>> + Send>, 
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

unsafe impl Send for OracleRawSourceParser {}
unsafe impl Sync for OracleRawSourceParser {}

impl OracleRawSourceParser {
    // Accept any iterator
    pub fn new(iter: impl Iterator<Item = oracle::Result<Row>> + Send + 'static, ncols: usize) -> Self {
        Self {
            iter: Box::new(iter),
            rowbuf: Vec::with_capacity(1024),
            ncols,
            current_col: 0,
            current_row: 0,
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

impl<'a> PartitionParser<'a> for OracleRawSourceParser {
    type TypeSystem = OracleTypeSystem;
    type Error = OracleSourceError;

    fn fetch_next(&mut self) -> Result<(usize, bool), Self::Error> {
        if self.is_finished && self.rowbuf.is_empty() {
            return Ok((0, true));
        }
        
        // Clear buffer
        self.rowbuf.clear();
        self.current_row = 0;
        self.current_col = 0;

        let batch_size = 1024;

        // Fetch batch
        for _ in 0..batch_size {
            match self.iter.next() {
                Some(Ok(row)) => self.rowbuf.push(row),
                Some(Err(e)) => return Err(OracleSourceError::from(e)),
                None => {
                    self.is_finished = true;
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

pub struct OracleSourcePartition {
    conn: OracleConn,
    query: CXQuery<String>,
    schema: Vec<OracleTypeSystem>,
    nrows: usize,
    ncols: usize,
}

impl OracleSourcePartition {
    pub fn new(conn: OracleConn, query: &CXQuery<String>, schema: &[OracleTypeSystem]) -> Self {
        Self {
            conn,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
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
        OracleTextSourceParser::new(&self.conn, query.as_str(), &self.schema)?
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
    pub fn new(conn: &'a OracleConn, query: &str, schema: &[OracleTypeSystem]) -> Self {
        let stmt = conn
            .statement(query)
            .prefetch_rows(ORACLE_ARRAY_SIZE)
            .fetch_array_size(ORACLE_ARRAY_SIZE)
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
