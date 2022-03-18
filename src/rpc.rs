//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{Consistency, StoreCommand, KVSnapshot, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;

use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use omnipaxos_core::messages::{Message, PaxosMsg, AcceptDecide, AcceptStopSign, AcceptSync, Accepted, AcceptedStopSign, Compaction, Decide, DecideStopSign, FirstAccept, Prepare, Promise};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use omnipaxos_core::ballot_leader_election::messages::{BLEMessage, HeartbeatMsg, HeartbeatRequest, HeartbeatReply};
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::util::SyncItem;
use omnipaxos_core::storage::SnapshotType;
use slog::{debug, info, trace, warn, Logger};


// use slog::{o, Drain, Logger};

//se std::option::Option;
#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
// define all types of messages that Raft replicas pass between each other
use proto::{
    Query, QueryResults, QueryRow, Void, RpcMessage, B, RpcPromise, RpcAcceptedStopSign, RpcAcceptDecide, RpcAcceptSync, Command, StopSign, RpcDecide, RpcAccepted, Commands, RpcPrepare, RpcSyncItem, RpcSnapshotType, RpcSyncItemType, RpcFirstAccept, RpcAcceptStopSign, RpcDecideStopSign,
    RpcHeartbeatReply, RpcHeartbeatRequest, RpcBleMessage, RpcCompaction, RpcProposalForward, RpcPrepareReq, RpcSnapshotTypeEnum
};

use proto::rpc_message::Message::{ Rpcproposalforward, Rpcpreparereq, Rpccompaction, Rpcacceptedstopsign, Rpcpromise, Rpcacceptdecide, Rpcacceptsync, Rpcdecide, Rpcaccepted, Rpcprepare, Rpcfirstaccept, Rpcacceptstopsign, Rpcdecidestopsign};
use proto::rpc_sync_item::V:: {Storecommands, Snapshottype, Syncitemtype};
use proto::rpc_ble_message::Message::{Heartbeatreply, Heartbeatrequest};

type NodeAddrFn = dyn Fn(u64) -> String + Send + Sync;

// this `struct` is printable with `fmt::Debug`.
// a ConnectionPool has a queue connections which are actually RpcClients
#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

// Connection
// A Connection has a conn: an RpcClient
// and a pool: ConnectionPool
struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

// Connections
// Arc and Mutext: for thread safety and shared data protextion respectively
// HashMap<String, Arc<ConnectionPool>>:
// so given a string we can find a connectionPool
// what is this string?
#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

/// RPC transport.
/// an RPC transport has a node_addr and connections
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
    
}

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send(&self, to_id: u64, msg: Message<StoreCommand, KVSnapshot>) {
        // based on the type of message we want to send we implement the send function differently
        let from = msg.from;
        let to = msg.to;
        log::info!("[SEND] FROM {} TO {}, BLE REQUEST: ",from, to);
        match msg.msg {
            PaxosMsg::AcceptDecide(x) => {
                log::info!("[SEND] ACCEPT DECIDE");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let entries = x.entries.iter().map(|entry| {Command { id: entry.id, sql: entry.sql.clone()}}).collect();
                let themessage = Rpcacceptdecide(RpcAcceptDecide{n: ballot, ld: x.ld,  entries: entries});
                let request = RpcMessage {to, from, message: Some(themessage)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::Accepted(x) => {
                log::info!("[SEND] ACCEPTED");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let themessage = Rpcaccepted(RpcAccepted{n:ballot, la: x.la});
                let request = RpcMessage {to, from, message: Some(themessage)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::AcceptedStopSign(x) => {
                log::info!("[SEND] ACCEPTED STOP SIGN");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let themessage = Rpcacceptedstopsign(RpcAcceptedStopSign{n: ballot});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::AcceptStopSign(x) => {
                log::info!("[SEND] ACCEPT STOP SIGN");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});

                let stopsign = Some(StopSign {config_id: x.ss.config_id, nodes: x.ss.nodes, metadata: Some(x.ss.metadata.unwrap())});
                let themessage = Rpcacceptstopsign(RpcAcceptStopSign{n:ballot, ss:stopsign});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });

            }
            PaxosMsg::AcceptSync(x) => {
                log::info!("[SEND] ACCEPT SYNC");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                
                // create stop sign
                let mut stopsign = None;
                match x.stopsign{
                    Some(ss) => {
                        stopsign = Some(StopSign {config_id: ss.config_id, nodes: ss.nodes, metadata: ss.metadata});
                    }
                    None => {
                    }
                }
                
                // create the SyncItem
                let mut sync_item = None;
                match x.sync_item {
                    SyncItem::Entries(y) => {
                        let commands: Vec<Command> = y
                                    .iter()
                                    .map(|entry| {
                                        Command {id: entry.id, sql: entry.sql.clone()}
                                    }).collect();
                        sync_item = Some(RpcSyncItem{v: Some(Storecommands(Commands {entries: commands}))});
                    }
                    SyncItem::Snapshot(y) => {
                         match y {
                            SnapshotType::_Phantom(_) => {
                                sync_item = Some(RpcSyncItem{v: Some(Syncitemtype(RpcSyncItemType::Phantom as i32))});
                            } 
                            SnapshotType::Complete(s) => {
                                sync_item = Some(RpcSyncItem{v: Some(Snapshottype(RpcSnapshotType {t: RpcSnapshotTypeEnum::Complete as i32 , s: s.snapshotted}))});
                            }
                            SnapshotType::Delta(s) => { 
                                sync_item =Some(RpcSyncItem{v: Some(Snapshottype(RpcSnapshotType {t: RpcSnapshotTypeEnum::Delta as i32 , s: s.snapshotted}))});  
                            }
                        }
                    }
                    SyncItem::None => {
                        sync_item = Some(RpcSyncItem{v: Some(Syncitemtype(RpcSyncItemType::None as i32))});
                    }
                }

                let themessage = Rpcacceptsync(RpcAcceptSync{n: ballot, sync_item, sync_idx: x.sync_idx, decide_idx: x.decide_idx, stopsign});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::Decide(x) => {
                log::info!("[SEND] ACCEPT DECIDE");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let themessage = Rpcdecide(RpcDecide{n:ballot, ld: x.ld});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::DecideStopSign(x) => {
                log::info!("[SEND] DECIDE STOP SIGN");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let themessage = Rpcdecidestopsign(RpcDecideStopSign{n:ballot});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::FirstAccept(x) => {
                log::info!("[SEND] FIRST ACCEPT");
                let ballot = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let entries = x.entries.iter().map(|entry| {Command { id: entry.id, sql: entry.sql.clone()}}).collect();

                let themessage = Rpcfirstaccept(RpcFirstAccept{n: ballot, entries: entries});
                let request = RpcMessage {to, from, message: Some(themessage)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::Compaction(x)  => {
                log::info!("[SEND] COMPACTION");
                let mut themessage = None;
                match x {
                    Compaction::Trim(y) => {
                        themessage = Some(Rpccompaction( RpcCompaction {s: "trim".to_string(), v: y, itisforward: false}));
                    }
                    Compaction::Snapshot(y) => {
                        themessage = Some(Rpccompaction( RpcCompaction {s: "snapshot".to_string(), v: Some(y), itisforward: false}));
                    }
                }
                let request = RpcMessage {to, from, message: themessage};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::ForwardCompaction(x) => {
                log::info!("[SEND] FORWARD COMPACTION");
                let mut themessage = None;
                match x {
                    Compaction::Trim(y) => {
                        themessage = Some(Rpccompaction( RpcCompaction {s: "trim".to_string(), v: y, itisforward: true}));
                    }
                    Compaction::Snapshot(y) => {
                        //String s = "snapshot";
                        themessage = Some(Rpccompaction( RpcCompaction {s: "snapshot".to_string(), v: Some(y), itisforward: true}));
                    }
                }
                let request = RpcMessage {to, from, message: themessage};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::Prepare(x) => {
                log::info!("[SEND] PREPARE");
                let ballot_n = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let ballot_n_accepted = Some(B{n: x.n_accepted.n as u32, priority: x.n_accepted.priority, pid: x.n_accepted.pid});
                let themessage = Rpcprepare(RpcPrepare{n:ballot_n, ld: x.ld, n_accepted:ballot_n_accepted, la: x.la});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::Promise(x) => {
                log::info!("[SEND] PROMISE");
                let ballot_n = Some(B{n: x.n.n, priority: x.n.priority, pid: x.n.pid});
                let ballot_n_accepted = Some(B{n: x.n_accepted.n, priority: x.n_accepted.priority, pid: x.n_accepted.pid});
                // create the SyncItem
                let mut sync_item = None;
                match  x.sync_item {
                    Some(SyncItem::Entries(y)) => {
                        let commands: Vec<Command> = y
                                    .iter()
                                    .map(|entry| {
                                        let id = entry.id;
                                        let sql = entry.sql.clone();
                                        Command {id, sql}
                                    }).collect();
                        sync_item = Some(RpcSyncItem{v: Some(Storecommands(Commands {entries: commands}))});
                    }
                    Some(SyncItem::Snapshot(y)) => {
                         match y {
                            SnapshotType::_Phantom(_) => {
                                sync_item = Some(RpcSyncItem{v: Some(Syncitemtype(RpcSyncItemType::Phantom  as i32))});
                            } 
                            SnapshotType::Complete(s) => {
                                sync_item = Some(RpcSyncItem{v: Some(Snapshottype(RpcSnapshotType {t: RpcSnapshotTypeEnum::Complete as i32 , s: s.snapshotted}))});
                            }
                            SnapshotType::Delta(s) => { 
                                sync_item =Some(RpcSyncItem{v: Some(Snapshottype(RpcSnapshotType {t: RpcSnapshotTypeEnum::Delta as i32 , s: s.snapshotted}))});  
                            }
                        }
                    }
                    Some(SyncItem::None) => {
                        sync_item = Some(RpcSyncItem{v: Some(Syncitemtype(RpcSyncItemType::None  as i32))});
                    }
                    None => { 
                        sync_item = Some(RpcSyncItem{v: Some(Syncitemtype(RpcSyncItemType::Empty  as i32))});
                    }
                }
                // create stop sign
                let mut stopsign = None;
                match x.stopsign {
                    Some(ss) => {
                        stopsign = Some(StopSign {config_id: ss.config_id, nodes: ss.nodes, metadata: ss.metadata});
                    }
                    None => {
                    }
                }
                let themessage = Rpcpromise(RpcPromise{n:ballot_n, n_accepted: ballot_n_accepted, syncitem: sync_item, ld: x.ld, la: x.la, stopsign: stopsign});
                let request = RpcMessage {to, from, message: Some(themessage)};
                
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    // here we try to send promise to the leader
                    client.conn.message(request).await.unwrap();
                    
                });
            }
            PaxosMsg::ProposalForward(x) => {
                let entries = x.iter().map(|entry| { Command {id: entry.id, sql: entry.sql.clone()}}).collect();
                let themessage = Rpcproposalforward (RpcProposalForward {entries});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
            PaxosMsg::PrepareReq => {
                log::info!("[SEND] PREPARE REQ");
                let themessage = Rpcpreparereq (RpcPrepareReq {});
                let request = RpcMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.message(request).await.unwrap();
                });
            }
        }
    }


    fn send_ble(&self, to_id: u64, msg: BLEMessage) {
        // based on the type of message we want to send we implement the send function differently
        let from = msg.from;
        let to = msg.to;
        match msg.msg {
            HeartbeatMsg::Request(r) => {
                log::info!("[BLE_SEND] FROM {} TO {}, BLE REQUEST: {}",from, to, r.round);
                let themessage =  Heartbeatrequest(RpcHeartbeatRequest{round: r.round});
                let request = RpcBleMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.ble_message(request).await.unwrap();
                });

            }
            HeartbeatMsg::Reply(r) => {
                log::info!("[BLE_SEND] FROM {} TO {}, BLE REPLY: Ballot: n: {} priority: {} pid: {}",from, to,  r.ballot.n, r.ballot.priority, r.ballot.pid);
                let ballot = Some(B{n: r.ballot.n, priority: r.ballot.priority, pid: r.ballot.pid});
                let themessage = Heartbeatreply(RpcHeartbeatReply{round: r.round, ballot, majority_connected: r.majority_connected});
                let request = RpcBleMessage {to, from, message: Some(themessage)};

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.ble_message(request).await.unwrap();
                });
            }
        }
    }

    async fn delegate(
        &self,
        to_id: u64,
        sql: String,
        consistency: Consistency,
    ) -> Result<crate::server::QueryResults, crate::StoreError> {
        let addr = (self.node_addr)(to_id);
        let mut client = self.connections.connection(addr.clone()).await;
        let query = tonic::Request::new(Query {
            sql,
            consistency: consistency as i32,
        });
        let response = client.conn.execute(query).await.unwrap();
        let response = response.into_inner();
        let mut rows = vec![];
        for row in response.rows {
            rows.push(crate::server::QueryRow { values: row.values });
        }
        Ok(crate::server::QueryResults { rows })
    }
}

/// RPC service.
/// an RpcService has a <StoreServer<RpcTransport>
/// StoreServer: important at the server.RpcService
/// RpcTransport: here
#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server}

    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let query = request.into_inner();
        // choose consistency
        let consistency =
            proto::Consistency::from_i32(query.consistency).unwrap_or(proto::Consistency::Strong);
        let consistency = match consistency {
            proto::Consistency::Strong => Consistency::Strong,
            proto::Consistency::RelaxedReads => Consistency::RelaxedReads,
        };
        println!("CONSISTENCY! ");
        // pass the query to the server
        let server = self.server.clone();
        println!("before query! ");
        let results = match server.query(query.sql, consistency).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        println!("after query! ");
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

    async fn message(&self, request: Request<RpcMessage>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let to = msg.to;
        let from = msg.from;

        match msg.message {
            None => {//TODO print error
            }
            Some(Rpcprepare(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                let mut ballot_n_accepted = None;
                match x.n_accepted {
                    Some(b) => {
                        ballot_n_accepted = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }

                if is_ok {
                    let themessage = PaxosMsg::Prepare(Prepare{n: ballot_n.unwrap(), ld: x.ld, n_accepted: ballot_n_accepted.unwrap(), la: x.la});
                    let msg = Message{to, from, msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {
                    // print error
                }
            }
            Some(Rpcfirstaccept(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                
                let entries = x.entries.iter().map(|entry| {StoreCommand { id: entry.id, sql: entry.sql.clone()}}).collect();

                if is_ok {
                    let themessage = PaxosMsg::FirstAccept(FirstAccept{n: ballot_n.unwrap(), entries});
                    let msg = Message{to, from,msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {

                }
            }
            Some(Rpcdecidestopsign(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                if is_ok {
                    let themessage = PaxosMsg::DecideStopSign(DecideStopSign{n: ballot_n.unwrap()});
                    let msg = Message{to, from,msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {

                }
            }
            Some(Rpcdecide(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }

                
               if is_ok {
                let themessage = PaxosMsg::Decide(Decide{n: ballot_n.unwrap(), ld: x.ld});
                    let msg = Message{to, from,msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {

                }
            }
            Some(Rpcacceptedstopsign(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                
                if is_ok {
                    let themessage = PaxosMsg::AcceptedStopSign(AcceptedStopSign{n: ballot_n.unwrap()});
                    let msg = Message{to, from,msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {

                }
            
            }
            Some(Rpcaccepted(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                
                if is_ok {
                    let themessage = PaxosMsg::Accepted(Accepted{n: ballot_n.unwrap(), la: x.la});
                    let msg = Message{to, from,msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {

                }
            }
            Some(Rpcacceptstopsign(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }

                // create stopsign
                let mut ss = None;
                match x.ss {
                    Some(s) => {
                        ss = Some(omnipaxos_core::storage::StopSign{config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                
                if is_ok {
                    let themessage = PaxosMsg::AcceptStopSign(AcceptStopSign{n: ballot_n.unwrap(), ss: ss.unwrap()});
                    let msg = Message{to, from, msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {

                }
            }
            Some(Rpcacceptdecide(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }

                let entries = x.entries.iter().map(|entry| {StoreCommand { id: entry.id , sql: entry.sql.clone()}}).collect();

                if is_ok {
                    let themessage = PaxosMsg::AcceptDecide(AcceptDecide{n: ballot_n.unwrap(), ld: x.ld, entries});
                    let msg = Message{to, from, msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                } else {
                    
                }
            }
            Some(Rpcpromise(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                let mut ballot_n_accepted = None;
                match x.n_accepted {
                    Some(b) => {
                        ballot_n_accepted = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
                // create sync item
                let mut sync_item = None;
                match x.syncitem {
                    Some(item) => {
                        match item.v {
                            None => {
                                is_ok = false;
                            }
                            // Entries case
                            Some(Storecommands (Commands {entries})) => {
                                let entries = entries.iter().map(|entry| {StoreCommand { id: entry.id, sql: entry.sql.clone()}}).collect();
                                sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Entries(entries));
                            }
                            // None or Phantom or Empty case
                            Some(Syncitemtype(t)) => {
                                match RpcSyncItemType::from_i32(t) {
                                    Some(RpcSyncItemType::Phantom) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Snapshot(SnapshotType::<StoreCommand, KVSnapshot>::_Phantom(core::marker::PhantomData::<StoreCommand>)));
                                    } ,
                                    Some(RpcSyncItemType::None) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::None);
                                    }
                                    Some(RpcSyncItemType::Empty) =>{
                                    }
                                    None => {
                                        is_ok = false;
                                    }
                                }
                            }
                            // Complete or Delta case
                            Some(Snapshottype(RpcSnapshotType{t, s})) => {
                                match RpcSnapshotTypeEnum::from_i32(t) {
                                    Some(RpcSnapshotTypeEnum::Complete) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Snapshot(SnapshotType::Delta(KVSnapshot{snapshotted: s})));
                                    } ,
                                    Some(RpcSnapshotTypeEnum::Delta) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Snapshot(SnapshotType::Complete(KVSnapshot{snapshotted: s})));
                                    }
                                    None => {
                                        is_ok = false;
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        is_ok = false;
                    }
                }
             
                // create stopsign
                let mut ss = None;
                match x.stopsign {
                    Some(s) => {
                        ss = Some(omnipaxos_core::storage::StopSign{config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                        is_ok = false;
                    }
                }
 
                 if is_ok {
                     let themessage = PaxosMsg::Promise(Promise{n: ballot_n.unwrap(), n_accepted: ballot_n_accepted.unwrap(), sync_item: sync_item, ld: x.ld, la: x.la, stopsign: ss});
                     let msg = Message{
                         to,
                         from,
                         msg: themessage,
                     };
                     let server = self.server.clone();
                     server.handle(msg);
                 } else {
 
                 }
            }
            Some(Rpcacceptsync(x)) => {
                let mut is_ok: bool = true;
                let mut ballot_n = None;
                match x.n {
                    Some(b) => {
                        ballot_n = Some(Ballot{n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        is_ok = false;
                    }
                }
               
                // create sync item
                let mut sync_item = None;
                match x.sync_item {
                    Some(item) => {
                        match item.v {
                            None => {
                                is_ok = false;
                            }
                            // Entries case
                            Some(Storecommands (Commands {entries})) => {
                                let entries = entries.iter().map(|entry| {StoreCommand { id: entry.id, sql: entry.sql.clone()}}).collect();
                                sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Entries(entries));
                            }
                            // None or Phantom or Empty case
                            Some(Syncitemtype(t)) => {
                                match RpcSyncItemType::from_i32(t) {
                                    Some(RpcSyncItemType::Phantom) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Snapshot(SnapshotType::<StoreCommand, KVSnapshot>::_Phantom(core::marker::PhantomData::<StoreCommand>)));
                                    } ,
                                    Some(RpcSyncItemType::None) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::None);
                                    }
                                    Some(RpcSyncItemType::Empty) =>{
                                    }
                                    None => {
                                        is_ok = false;
                                    }
                                }
                            }
                            // Complete or Delta case
                            Some(Snapshottype(RpcSnapshotType{t, s})) => {
                                match RpcSnapshotTypeEnum::from_i32(t) {
                                    Some(RpcSnapshotTypeEnum::Complete) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Snapshot(SnapshotType::Delta(KVSnapshot{snapshotted: s})));
                                    } ,
                                    Some(RpcSnapshotTypeEnum::Delta) =>{
                                        sync_item = Some(SyncItem::<StoreCommand, KVSnapshot>::Snapshot(SnapshotType::Complete(KVSnapshot{snapshotted: s})));
                                    }
                                    None => {
                                        is_ok = false;
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        is_ok = false;
                    }
                }
                 
                // create stopsign
                let mut ss = None;
                match x.stopsign {
                    Some(s) => {
                        ss = Some(omnipaxos_core::storage::StopSign{config_id: s.config_id, nodes: s.nodes, metadata: s.metadata});
                    }
                    None => {
                        is_ok = false;
                    }
                }

                 if is_ok {
                    let themessage = PaxosMsg::AcceptSync(AcceptSync{n: ballot_n.unwrap(), sync_item: sync_item.unwrap(), sync_idx: x.sync_idx, decide_idx: x.decide_idx, stopsign: ss});
                    let msg = Message{to, from, msg: themessage};
                    let server = self.server.clone();
                    server.handle(msg);
                }
            }
            Some(Rpcproposalforward(x)) => {
                let entries = x.entries.iter().map(|entry| {StoreCommand { id: entry.id, sql: entry.sql.clone()}}).collect();
                let themessage = PaxosMsg::ProposalForward(entries);
                let msg = Message{ to, from, msg: themessage,
                };
                let server = self.server.clone();
                server.handle(msg);
            }
            Some(Rpcpreparereq(_)) => {
                let themessage = PaxosMsg::PrepareReq;
                let msg = Message{to, from, msg: themessage,
                };
                let server = self.server.clone();
                server.handle(msg);
            }
            Some(Rpccompaction(x)) => {
                let mut themessage = None;
                match x.itisforward {
                    true => {
                        if x.s == "trim" {
                             themessage = Some(PaxosMsg::ForwardCompaction(Compaction::Trim(x.v))); 
                        }
                        else if x.s == "snapshot" {
                             themessage = Some(PaxosMsg::ForwardCompaction(Compaction::Snapshot(x.v.unwrap())));
                        }
                    }
                    false => {
                        if x.s == "trim" {
                            themessage = Some(PaxosMsg::Compaction(Compaction::Trim(x.v))); 
                       }
                       else if x.s == "snapshot" {
                            themessage = Some(PaxosMsg::Compaction(Compaction::Snapshot(x.v.unwrap())));
                       }
                    }
                }
                let msg = Message{
                    to,
                    from,
                    msg: themessage.unwrap(),
                };
                let server = self.server.clone();
                server.handle(msg);
            }
        }
        Ok(Response::new(Void {}))
    }

    async fn ble_message(&self, request: Request<RpcBleMessage>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let to = msg.to;
        let from = msg.from;

        match msg.message {
            Some(Heartbeatreply(x)) => {  
                log::info!("[BLE_MESSAGE] Reply");
                let mut isOk: bool =  true;
                let mut ballot = None;
                match x.ballot {
                    Some(b) => {
                        ballot = Some(Ballot {n: b.n, priority: b.priority, pid: b.pid});
                    }
                    None => {
                        isOk = false;
                    }
                }
                if isOk {
                    let theblemessage = HeartbeatMsg::Reply(HeartbeatReply{round: x.round, ballot: ballot.unwrap(), majority_connected: x.majority_connected});
                    let msg = omnipaxos_core::ballot_leader_election::messages::BLEMessage{to,from, msg:theblemessage};
                    let server = self.server.clone();
                    server.handle_ble(msg);
                } else {
                    // TODO: print error
                }
            }
            Some(Heartbeatrequest(x)) => { 
                log::info!("[BLE_MESSAGE] Request: from: {} to: {} round:{}",from, to, x.round);
                let theblemessage = HeartbeatMsg::Request(HeartbeatRequest{round: x.round});
                let msg = omnipaxos_core::ballot_leader_election::messages::BLEMessage{to, from, msg: theblemessage};
                let server = self.server.clone();
                server.handle_ble(msg);
            }
            None => {
                // print error
            }
        }
        Ok(Response::new(Void {}))
    }
}
