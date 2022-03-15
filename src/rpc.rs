//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{Consistency, StoreCommand, KVSnapshot, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use omnipaxos_core::messages::{Message, PaxosMsg};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use omnipaxos_core::ballot_leader_election::messages::BLEMessage;

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
// define all types of messages that Raft replicas pass between each other
use proto::{
    Query, QueryResults, QueryRow, Void, TheMessage, B
};

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
        match msg.msg {
            PaxosMsg::AcceptDecide(_) => {
                
            }
            PaxosMsg::Accepted(x) => {
                // get the fields
                let n = x.n;
                let la = x.la;
            }
            PaxosMsg::AcceptedStopSign(_) => {}
            PaxosMsg::AcceptStopSign(_) => {}
            PaxosMsg::AcceptSync(_) => {}
            PaxosMsg::Compaction(_) => {}
            PaxosMsg::Decide(_) => {}
            PaxosMsg::DecideStopSign(_) => {}
            PaxosMsg::FirstAccept(_) => {}
            PaxosMsg::ForwardCompaction(_) => {}
            PaxosMsg::Prepare(_) => {}
            PaxosMsg::PrepareReq => {}
            PaxosMsg::Promise(_) => {}
            PaxosMsg::ProposalForward(_) => {}
        }
    }

    fn send_ble(&self, to_id: u64, msg: BLEMessage) {
        // based on the type of message we want to send we implement the send function differently
        // match msg {
           
        // }
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
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        println!("EXECUTE! ");
        let query = request.into_inner();
        let consistency =
            proto::Consistency::from_i32(query.consistency).unwrap_or(proto::Consistency::Strong);
        let consistency = match consistency {
            proto::Consistency::Strong => Consistency::Strong,
            proto::Consistency::RelaxedReads => Consistency::RelaxedReads,
        };
        // pass the query to the server
        let server = self.server.clone();
        let results = match server.query(query.sql, consistency).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

    async fn message(&self, request: Request<TheMessage>) -> Result<Response<Void>, tonic::Status> {

        Ok(Response::new(Void {}))
    }

    // async fn respond_to_vote(
    //     &self,
    //     request: Request<VoteResponse>,
    // ) -> Result<Response<Void>, tonic::Status> {
    //     let msg = request.into_inner();
    //     let from_id = msg.from_id as usize;
    //     let term = msg.term as usize;
    //     let vote_granted = msg.vote_granted;
    //     // let msg = little_raft::message::Message::VoteResponse {
    //     //     from_id,
    //     //     term,
    //     //     vote_granted,
    //     // };
    //     let server = self.server.clone();
    //     //server.recv_msg(msg);
    //     Ok(Response::new(Void {}))
    // }

    // async fn append_entries(
    //     &self,
    //     request: Request<AppendEntriesRequest>,
    // ) -> Result<Response<Void>, tonic::Status> {
    //     let msg = request.into_inner();
    //     let from_id = msg.from_id as usize;
    //     let term = msg.term as usize;
    //     let prev_log_index = msg.prev_log_index as usize;
    //     let prev_log_term = msg.prev_log_term as usize;
    //     // let entries: Vec<little_raft::message::LogEntry<StoreCommand>> = msg
    //     //     .entries
    //     //     .iter()
    //     //     .map(|entry| {
    //     //         let id = entry.id as usize;
    //     //         let sql = entry.sql.to_string();
    //     //         let transition = StoreCommand { id, sql };
    //     //         let index = entry.index as usize;
    //     //         let term = entry.term as usize;
    //     //         little_raft::message::LogEntry {
    //     //             transition,
    //     //             index,
    //     //             term,
    //     //         }
    //     //     })
    //     //     .collect();
    //     let commit_index = msg.commit_index as usize;
    //     // let msg = little_raft::message::Message::AppendEntryRequest {
    //     //     from_id,
    //     //     term,
    //     //     prev_log_index,
    //     //     prev_log_term,
    //     //     entries,
    //     //     commit_index,
    //     // };
    //     let server = self.server.clone();
    //    // server.recv_msg(msg);
    //     Ok(Response::new(Void {}))
    // }

    // async fn respond_to_append_entries(
    //     &self,
    //     request: tonic::Request<AppendEntriesResponse>,
    // ) -> Result<tonic::Response<Void>, tonic::Status> {
    //     let msg = request.into_inner();
    //     let from_id = msg.from_id as usize;
    //     let term = msg.term as usize;
    //     let success = msg.success;
    //     let last_index = msg.last_index as usize;
    //     let mismatch_index = msg.mismatch_index.map(|idx| idx as usize);
    //     // let msg = little_raft::message::Message::AppendEntryResponse {
    //     //     from_id,
    //     //     term,
    //     //     success,
    //     //     last_index,
    //     //     mismatch_index,
    //     // };
    //     let server = self.server.clone();
    //     //server.recv_msg(msg);
    //     Ok(Response::new(Void {}))
    // }
}
