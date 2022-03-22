//! ChiselStore server module.

use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use derivative::Derivative;
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use slog::{debug, info, trace, warn, Logger};
// CHANGE:
use tokio::sync::mpsc;

use omnipaxos_core::ballot_leader_election::{BLEConfig, BallotLeaderElection, Ballot};
use omnipaxos_core::ballot_leader_election::messages::BLEMessage;
use omnipaxos_core::{
    messages::Message,
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{Storage, Snapshot, StopSignEntry}
};

// Used for handling async queries
// #[derive(Clone, Debug)]
#[derive(Debug)]
pub struct QueryResultsHolder {
    query_completion_notifiers: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl QueryResultsHolder {
    pub fn insert_notifier(&mut self, id: u64, notifier: Arc<Notify>) {
        self.query_completion_notifiers.insert(id, notifier);
    }

    pub fn push_result(&mut self, id: u64, result: Result<QueryResults, StoreError>) {
        if let Some(completion) = self.query_completion_notifiers.remove(&(id as u64)) {
            self.results.insert(id as u64, result);
            completion.notify();
        }
    }

    pub fn remove_result(&mut self, id: &u64) -> Option<Result<QueryResults, StoreError>> {
        self.results.remove(id)
    }
    
    fn default() -> Self {
        Self {
            query_completion_notifiers: HashMap::new(),
            results: HashMap::new(),
        }
    }
}


/// ChiselStore transport layer.
///
/// Your application should implement this trait to provide network access
/// to the ChiselStore server.
#[async_trait]
pub trait StoreTransport {
    /// Send a store command message `msg` to `to_id` node.
    fn send(&self, to_id: u64, msg: Message<StoreCommand, KVSnapshot>);

    fn send_ble(&self, to_id: u64, msg: BLEMessage);
}

/// Consistency mode.
#[derive(Debug)]
pub enum Consistency {
    /// Strong consistency. Both reads and writes go through the Raft leader,
    /// which makes them linearizable.
    Strong,
    /// Relaxed reads. Reads are performed on the local node, which relaxes
    /// read consistency and allows stale reads.
    RelaxedReads,
}

/// Store command.
///
/// A store command is a SQL statement that is replicated in the Raft cluster.
#[derive(Clone, Debug)]
pub struct StoreCommand {
    /// Unique ID of this command.
    pub id: u64,
    /// The SQL statement of this command.
    pub sql: String,
}

/// KVSnapshot
#[derive(Clone, Debug)]
pub struct KVSnapshot {
    // the snapshotted data
    pub snapshotted: HashMap<u64, String>,
}

impl Snapshot<StoreCommand> for KVSnapshot {
    fn create(entries: &[StoreCommand]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            let StoreCommand { id, sql } = e;
            snapshotted.insert(id.clone(), sql.clone());
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}

/// Store configuration.
#[derive(Debug)]
struct StoreConfig {
    /// Connection pool size.
    conn_pool_size: usize,
    query_results_holder: Arc<Mutex<QueryResultsHolder>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Store {
    /// Vector which contains all the replicated entries in-memory.
    log: Vec<StoreCommand>,
    /// Last promised round.
    n_prom: Ballot,
    /// Last accepted round.
    acc_round: Ballot,
    /// Length of the decided log.
    ld: u64,

    // TMP snapshots impl

    /// Garbage collected index.
    trimmed_idx: u64,
    /// Stored snapshot
    snapshot: Option<KVSnapshot>,
    /// Stored StopSign
    stopsign: Option<omnipaxos_core::storage::StopSignEntry>,


    /// ID of the node this Cluster objecti s on.
    this_id: u64,
    /// Is this node the leader?
    leader: Option<u64>,
    leader_exists: AtomicBool,

    
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,

    pending_transitions: Vec<StoreCommand>,
    query_results_holder: Arc<Mutex<QueryResultsHolder>>,
}


impl Store {
    pub fn new(this_id: u64, config: StoreConfig) -> Self {
        let mut conn_pool = vec![];
        let conn_pool_size = config.conn_pool_size;
        for _ in 0..conn_pool_size {
            // FIXME: Let's use the 'memdb' VFS of SQLite, which allows concurrent threads
            // accessing the same in-memory database.
            let flags = OpenFlags::new()
                .set_read_write()
                .set_create()
                .set_no_mutex();
            let mut conn =
                Connection::open_with_flags(format!("node{}.db", this_id), flags).unwrap();
            conn.set_busy_timeout(5000).unwrap();
            conn_pool.push(Arc::new(Mutex::new(conn)));
        }
        let conn_idx = 0;
        Store {
            this_id,
            leader: None,
            leader_exists: AtomicBool::new(false),

            log: Vec::new(),
            n_prom: Ballot::default(),
            acc_round: Ballot::default(),
            ld: 0,

            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,

            conn_pool,
            conn_idx,
            pending_transitions: Vec::new(),
            query_results_holder: config.query_results_holder,
        }
    }
    

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }
}

fn query(conn: Arc<Mutex<Connection>>, sql: String) -> Result<QueryResults, StoreError> {
    let conn = conn.lock().unwrap();
    let mut rows = vec![];
    log::info!("INSIDE QUERY 1");
    conn.iterate(sql, |pairs| {
        let mut row = QueryRow::new();
        for &(_, value) in pairs.iter() {
            row.values.push(value.unwrap().to_string());
        }
        rows.push(row);
        true
    })?;
    log::info!("INSIDE QUERY 2");
    Ok(QueryResults { rows })
}


/// ChiselStore server.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: StoreTransport + Send + Sync> {
    next_cmd_id: AtomicU64,
    #[derivative(Debug = "ignore")]
    replica: Arc<Mutex<SequencePaxos<StoreCommand, KVSnapshot, Store>>>,
    #[derivative(Debug = "ignore")]
    ble: Arc<Mutex<BallotLeaderElection>>,
    /// Transport layer.
    transport: Arc<T>,
    query_results_holder: Arc<Mutex<QueryResultsHolder>>
}

impl Storage<StoreCommand, KVSnapshot> for Store
{
    fn append_entry(&mut self, entry: StoreCommand) -> u64 {
        self.log.push(entry);
        self.get_log_len()
    }

    fn append_entries(&mut self, entries: Vec<StoreCommand>) -> u64 {
        let mut e = entries;
        self.log.append(&mut e);
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<StoreCommand>) -> u64 {
        self.log.truncate(from_idx as usize);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        self.n_prom = n_prom;
    }

    fn set_decided_idx(&mut self, ld: u64) {
        // run queries
        let queries_to_run = self.log[(self.ld as usize)..(ld as usize)].to_vec();
        //let conn = self.get_connection();
        for q in queries_to_run.iter() {
            let conn = self.get_connection();
            let results = query(conn, q.sql.clone());

            let mut query_results_holder = self.query_results_holder.lock().unwrap();
            query_results_holder.push_result(q.id, results);
        }

        self.ld = ld;
    }

    fn get_decided_idx(&self) -> u64 {
        self.ld
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        self.acc_round = na;
    }

    fn get_accepted_round(&self) -> Ballot {
        self.acc_round
    }

    fn get_entries(&self, from: u64, to: u64) -> &[StoreCommand] {
        self.log.get(from as usize..to as usize).unwrap_or(&[])
    }

    fn get_log_len(&self) -> u64 {
        self.log.len() as u64
    }

    fn get_suffix(&self, from: u64) -> &[StoreCommand] {
        match self.log.get(from as usize..) {
            Some(s) => s,
            None => &[],
        }
    }

    fn get_promise(&self) -> Ballot {
        self.n_prom
    }

    // TEMP Snapshots impl
    fn set_stopsign(&mut self, s: StopSignEntry) {
        self.stopsign = Some(s);
    }

    fn get_stopsign(&self) -> Option<StopSignEntry> {
        self.stopsign.clone()
    }

    fn trim(&mut self, trimmed_idx: u64) {
        self.log.drain(0..trimmed_idx as usize);
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) {
        self.trimmed_idx = trimmed_idx;
    }

    fn get_compacted_idx(&self) -> u64 {
        self.trimmed_idx
    }

    fn set_snapshot(&mut self, snapshot: KVSnapshot) {
        self.snapshot = Some(snapshot);
    }

    fn get_snapshot(&self) -> Option<KVSnapshot> {
        self.snapshot.clone()
    }
}

/// Query row.
#[derive(Debug)]
pub struct QueryRow {
    /// Column values of the row.
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

/// Query results.
#[derive(Debug)]
pub struct QueryResults {
    /// Query result rows.
    pub rows: Vec<QueryRow>,
}

const NOP_TRANSITION_ID: u64 = 0;
const HEARTBEAT_TIMEOUT: u64 =  100;
impl<T: StoreTransport + Send + Sync> StoreServer<T> {
    /// Start a new server as part of a ChiselStore cluster.
    pub fn start(this_id: u64, peers: Vec<u64>, transport: T) -> Result<Self, StoreError> {
        log::info!("Start {} !", this_id);
        // create a store
        let query_results_holder = Arc::new(Mutex::new(QueryResultsHolder::default()));
        let config = StoreConfig { conn_pool_size: 20, query_results_holder: query_results_holder.clone() };
        let store = Store::new(this_id, config);
        //let store = Arc::new(Mutex::new(store1));
        let noop = StoreCommand {
            id: NOP_TRANSITION_ID,
            sql: "".to_string(),
        };

        let mut sp_config = SequencePaxosConfig::default();
        // TODO: What is a congiguration id?
        sp_config.set_configuration_id(1);
        sp_config.set_pid(this_id);
        sp_config.set_peers(peers.clone());

        let replica = SequencePaxos::with(sp_config, store);
        let replica = Arc::new(Mutex::new(replica));
        
        let mut ble_config = BLEConfig::default();
        ble_config.set_pid(this_id);
        ble_config.set_peers(peers);
        ble_config.set_hb_delay(HEARTBEAT_TIMEOUT);     // a leader timeout of 20 ticks

        let ble = BallotLeaderElection::with(ble_config);
        let ble =  Arc::new(Mutex::new(ble));
        
        Ok(StoreServer {
            next_cmd_id: AtomicU64::new(1), // zero is reserved for no-op.
            replica,
            ble,
            transport: Arc::new(transport),
            query_results_holder
        })
    }
    
    pub fn run(&self) {
        loop {
                let mut ble = self.ble.lock().unwrap();
                let mut replica = self.replica.lock().unwrap();
                //let mut store = self.store.lock().unwrap();
                if let Some(leader) = ble.tick() {
                    // a new leader is elected, pass it to SequencePaxos.
                    replica.handle_leader(leader);
                    //store.leader = Some(leader.pid);
                }
                let transport = self.transport.clone();

                for out_msg in ble.get_outgoing_msgs() {
                    let receiver = out_msg.to;
                    // send out_msg to receiver on network layer
                    transport.send_ble(receiver, out_msg);      
                }

                for out_msg in replica.get_outgoing_msgs() {
                    let receiver = out_msg.to;
                    // send out_msg to receiver on network layer
                    transport.send(receiver, out_msg);
                }
            
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    

    /// Execute a SQL statement on the ChiselStore cluster.
    pub async fn query<S: AsRef<str>>(
        &self,
        stmt: S,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError> {
        
        let results = {
                log::info!("before notify");
                let (notify, id) = {
                    let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                    let cmd = StoreCommand {
                        id: id,
                        sql: stmt.as_ref().to_string(),
                    };
                    
                    let notify = Arc::new(Notify::new());

                    let mut query_results_holder = self.query_results_holder.lock().unwrap();
                    query_results_holder.insert_notifier(id, notify.clone());
                    
                    let mut replica = self.replica.lock().unwrap();
                    replica.append(cmd).expect("Failed to append");
                    log::info!("end of notify");
                    (notify, id)
                };
                log::info!("wait for notify");
                // wait for append (and decide) to finish in background
                notify.notified().await;
                log::info!("notify came");
                let results = self.query_results_holder.lock().unwrap().remove_result(&id).unwrap();
                // TODO: RETURN AN ERROR IF THIS IS AN
                log::info!("results notify");
                results?
        };  
        log::info!("END notify");
        Ok(results)
    }

    pub fn handle(&self, msg: Message<StoreCommand, KVSnapshot>) {
        self.replica.lock().unwrap().handle(msg);
    }
    pub fn handle_ble(&self, msg: BLEMessage) {
        self.ble.lock().unwrap().handle(msg);
    }
}

fn is_read_statement(stmt: &str) -> bool {
    stmt.to_lowercase().starts_with("select")
}