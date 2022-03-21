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

use omnipaxos_core::ballot_leader_election::{BLEConfig, BallotLeaderElection};
use omnipaxos_core::ballot_leader_election::messages::BLEMessage;
// use std::sync::mpsc::Sender;
use omnipaxos_core::{
    messages::Message,
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage, Snapshot},
};

/// ChiselStore transport layer.
///
/// Your application should implement this trait to provide network access
/// to the ChiselStore server.
#[async_trait]
pub trait StoreTransport {
    /// Send a store command message `msg` to `to_id` node.
    fn send(&self, to_id: u64, msg: Message<StoreCommand, KVSnapshot>);

    fn send_ble(&self, to_id: u64, msg: BLEMessage);

    /// Delegate command to another node.
    async fn delegate(
        &self,
        to_id: u64,
        sql: String,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError>;
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
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Store<T: StoreTransport + Send + Sync> {
    /// ID of the node this Cluster objecti s on.
    this_id: u64,
    /// Is this node the leader?
    leader: Option<u64>,
    leader_exists: AtomicBool,
    waiters: Vec<Arc<Notify>>,
    /// Transport layer.
    transport: Arc<T>,
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
    pending_transitions: Vec<StoreCommand>,
    command_completions: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
    last_executed_cmd: u64
}

impl<T: StoreTransport + Send + Sync> Store<T> {
    pub fn new(this_id: u64, transport: T, config: StoreConfig) -> Self {
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
            waiters: Vec::new(),
            transport: Arc::new(transport),
            conn_pool,
            conn_idx,
            pending_transitions: Vec::new(),
            command_completions: HashMap::new(),
            results: HashMap::new(),
            last_executed_cmd : 0
        }
    }

    pub fn is_leader(&self) -> bool {
        match self.leader {
            Some(id) => id == self.this_id,
            _ => false,
        }
    }
    
    pub fn insert(&mut self, id: u64, results: Result<QueryResults, StoreError>) {
        self.results.insert(id, results);
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
    store: Arc<Mutex<Store<T>>>,
    #[derivative(Debug = "ignore")]
    replica: Arc<Mutex<SequencePaxos<StoreCommand, KVSnapshot, MemoryStorage<StoreCommand, KVSnapshot>>>>,
    #[derivative(Debug = "ignore")]
    ble: Arc<Mutex<BallotLeaderElection>>,
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
        let config = StoreConfig { conn_pool_size: 20 };
        let store = Arc::new(Mutex::new(Store::new(this_id, transport, config)));
        let noop = StoreCommand {
            id: NOP_TRANSITION_ID,
            sql: "".to_string(),
        };

        let mut sp_config = SequencePaxosConfig::default();
        // TODO: What is a congiguration id?
        sp_config.set_configuration_id(1);
        sp_config.set_pid(this_id);
        sp_config.set_peers(peers.clone());


        let storage = MemoryStorage::<StoreCommand, KVSnapshot>::default();
        let replica = SequencePaxos::with(sp_config, storage);

        let replica = Arc::new(Mutex::new(replica));

        let mut ble_config = BLEConfig::default();
        ble_config.set_pid(this_id);
        ble_config.set_peers(peers);
        ble_config.set_hb_delay(HEARTBEAT_TIMEOUT);     // a leader timeout of 20 ticks

        let ble = BallotLeaderElection::with(ble_config);
        let ble =  Arc::new(Mutex::new(ble));
        
        Ok(StoreServer {
            next_cmd_id: AtomicU64::new(1), // zero is reserved for no-op.
            store,
            replica,
            ble
        })
    }

    pub fn run_commands(&self) {
        // thread to get the decided sequence and run the commands 
        loop {
            {
                let replica = self.replica.lock().unwrap();
                let mut store = self.store.lock().unwrap();
                // let command = replica.read(1);
                // log::info!("READ COMMAND");
            //     match command {
            //         Some(cmd) => {
            //             log::info!("GET DECIDED COMMANDS: SOME SOME SOME");
            //         }
            //         None => {}
            //     }
            // }
                let commands = replica.read_decided_suffix(store.last_executed_cmd);
               // log::info!("GET DECIDED COMMANDS");
                match commands {
                    Some(cmds) => {
                        log::info!("GET DECIDED COMMANDS: SOME");
                        for cmd in cmds {
                            log::info!("DECIDED CMD");
                            store.last_executed_cmd += 1;
                            // get the decided log entry
                            if let omnipaxos_core::util::LogEntry::Decided(c) = cmd {
                                log::info!("DECIDED COMMAND: {}", c.id);
                                // execute it
                                let conn = store.get_connection();
                                let results = query(conn, c.sql.clone());
                                log::info!("RESULTS COMMAND: {}", c.id);
                                // if the server is the leader
                                if store.is_leader() {
                                    log::info!("I AM THE LEADER");
                                    // save the result
                                    store.insert(c.id , results);
                                    // the command is completed so remove its notifier
                                    match store.command_completions.remove(&(c.id)) {
                                        Some(completion) => {
                                            log::info!("NOTIFY THAT THE RESULT IS DONE");
                                            completion.notify();
                                        }
                                        None => {
                                            log::info!("NOTIFYYYYYY nOT");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None => { // any command have not been decided 
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(2));
        } 
    }

    pub fn run_leader_election(&self) {
        // loop {
        //     {
        //         let mut ble = self.ble.lock().unwrap();
        //         if let Some(leader) = ble.tick() {
        //             // a new leader is elected, pass it to SequencePaxos.
        //             log::info!("TICK {}", leader.pid);
        //             let mut replica = self.replica.lock().unwrap();
        //             replica.handle_leader(leader);
        //         } else {

        //         }
        //     }
        //     std::thread::sleep(Duration::from_millis(1));
        // }
    }
    pub fn get_messages(&self) {
        loop {
                let mut ble = self.ble.lock().unwrap();
                let mut replica = self.replica.lock().unwrap();
                let mut store = self.store.lock().unwrap();
                if let Some(leader) = ble.tick() {
                    // a new leader is elected, pass it to SequencePaxos.
                    log::info!("TICK {}", leader.pid);
                    replica.handle_leader(leader);
                    store.leader = Some(leader.pid);
                }
                let transport = store.transport.clone();

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
        // If the statement is a read statement, let's use whatever
        // consistency the user provided; otherwise fall back to strong
        // consistency.
        let consistency = if is_read_statement(stmt.as_ref()) {
            consistency
        } else {
            Consistency::Strong
        };
        log::info!("START QUERY 2");
        let results = match consistency {
            Consistency::Strong => {
                log::info!("STRONG 1");
                // wait for the leader
                let leader_id = self.wait_for_leader().await;
                
                log::info!("STRONG 2  {}", leader_id);
                
                // when we have the leader
                // we check if we are not the leader or not 
                let (delegate, transport) = {
                    let store = self.store.lock().unwrap();
                    let delegate = !(store.this_id == leader_id);
                    (delegate, store.transport.clone())
                };

                log::info!("STRONG 3");
                // if we are not the leader we will delegate it to the leader
                if delegate {
                    log::info!("STRONG 33   ");
                    // when delegate wait for the result
                    // the leader will send us the result
                    return transport
                        .delegate(leader_id, stmt.as_ref().to_string(), consistency)
                        .await;
                }

                
                log::info!("STRONG 5");
                let (notify, id) = {
                     // create a unique id for the command
                    let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                    // create the command
                    let cmd = StoreCommand {id: id as u64, sql: stmt.as_ref().to_string()};
                    // write command
                    self.replica.lock().unwrap().append(cmd).expect("Failed to append");

                    // wait for a notification that the command has been decided
                    let mut store = self.store.lock().unwrap();
                    // create a notify in order to be notified that the command has been executed
                    let notify = Arc::new(Notify::new());
                    store.command_completions.insert(id, notify.clone());
                    (notify, id)
                };
                log::info!("STRONG 6-0.5: WAIT FOR NOTIFICATION");
                // wait to be notified that the result is ready
                
                notify.notified().await;
                
                log::info!("STRONG 6: NOTIFICATION JUST CAME");
                // if  the command has been the decided the results are written
                let results = self.store.lock().unwrap().results.remove(&id).unwrap();
                log::info!("STRONG 7");
                log::info!("STRONG 8");
                results?
            }
            Consistency::RelaxedReads => {
                log::info!("RELAXED READS");
                let conn = {
                    let mut store = self.store.lock().unwrap();
                    store.get_connection()
                };
                log::info!("RELAXED READS QUERY"); 
                query(conn, stmt.as_ref().to_string())?
            }
        };
        Ok(results)
    }

    /// Wait for a leader to be elected.
    pub async fn wait_for_leader(&self) -> u64 {
        loop {
            let leader_id = self.replica.lock().unwrap().get_current_leader();
            if leader_id != 0 {
                log::info!("GOT CURRENT LEADER!!");
                return leader_id;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // pub async fn run_when_decided(&self, stm: String) -> Result<QueryResults, StoreError> {
    //     let _entries = self.replica.lock().unwrap().read(10);
    //     let conn = {
    //         let mut store = self.store.lock().unwrap();
    //         store.get_connection()
    //     };
    //     return query(conn,stm);
    // }

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
