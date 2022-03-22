use anyhow::Result;
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};

use futures_util::future::FutureExt;

use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Server;
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{Consistency, Query};

use std::error::Error;
use tokio::sync::oneshot;

/// Node authority (host and port) in the cluster.
fn node_authority(id: u64) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: u64) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

pub struct Replica {
    store_server: std::sync::Arc<StoreServer<RpcTransport>>,
    server_kill_sender: tokio::sync::oneshot::Sender<()>,
    rpc_kill_sender: tokio::sync::oneshot::Sender<()>,
    message_handle: tokio::task::JoinHandle<()>,
    leader_handle: tokio::task::JoinHandle<()>,
    rpc_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}


impl Replica {
    pub async fn shutdown(self) {
        self.rpc_kill_sender.send(());
        self.server_kill_sender.send(());
        self.rpc_handle.await.unwrap();

        self.message_handle.await.unwrap();
        self.leader_handle.await.unwrap();
    }

    pub fn is_leader(&self) -> bool {
        //self.store_server.is_leader()
        false
    }

    pub fn get_id(&self) -> u64 {
        self.store_server.get_id()
    }
}

pub async fn setup(number_of_replicas: u64) -> Vec<Replica> {
    // some setup code, like creating required files/directories, starting
    // servers, etc.
    let mut replicas: Vec<Replica> = Vec::new();
    for id in 1..(number_of_replicas+1) {
        println!("IN SET UP {}", id);
        let mut peers: Vec<u64> = (1..number_of_replicas+1).collect();
        peers.remove((id - 1) as usize);
        replicas.push(start_server(id, peers).await);
    }
    println!("END SET UP");
    return replicas
}

use slog::info;
use sloggers::Build;
use sloggers::terminal::{TerminalLoggerBuilder, Destination};
use sloggers::types::Severity;
fn log(s: String) {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    info!(logger, "{}", s);
}

async fn start_server(id: u64, peers: Vec<u64>) ->Replica {
    let (host, port) = node_authority(id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    let server = StoreServer::start(id, peers, transport).unwrap();
    let server = Arc::new(server);

    let (server_kill_sender, server_kill_receiver) = oneshot::channel::<()>();
    let (message_handle, leader_handle) = {
        let server_receiver = server.clone();
        let server_run  = server.clone();
        let server_leader  = server.clone();

        tokio::task::spawn(async move {
            match server_kill_receiver.await {
                Ok(_) => { server_receiver.kill(true);
                    log(format!("killlllllll").to_string());},
                Err(_) => println!("Received error in halt_receiver"),
            };
        });

        let x = tokio::task::spawn(async move {
            server_run.run().await;
        });

        //run_leader
        let y = tokio::task::spawn(async move {
            server_leader.run_leader().await;
        });

        (x, y)
    };

    let (rpc_kill_sender, rpc_kill_receiver) = oneshot::channel::<()>();
    let rpc_handle = {
        let server = server.clone();
        let rpc = RpcService::new(server);
        tokio::task::spawn(async move {
            let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve_with_shutdown(rpc_listen_addr, rpc_kill_receiver.map(drop))
            .await;  
            ret
        })
    };
    // todo: check the results that there is not error
    return Replica {
        store_server: server.clone(),
        server_kill_sender,
        rpc_kill_sender,
        rpc_handle,
        message_handle,
        leader_handle
    }
}

pub async fn run_query(id: u64, line: String) -> Result<String, Box<dyn Error>>{
    // create address for the replica
    let addr = node_rpc_addr(id);
    // connect
    let mut client = RpcClient::connect(addr).await.unwrap();
    // query creation
    let query = tonic::Request::new(Query {
        sql: line.to_string(),
        consistency: Consistency::RelaxedReads as i32,
    });
    // execute the query
    let response = client.execute(query).await.unwrap();
    // get the response
    let response = response.into_inner();
    if response.rows.len() == 0 || response.rows[0].values.len() == 0 {
        return Ok(String::from(""));
    }
    let res = response.rows[0].values[0].clone();
    Ok(res)
}

pub async fn shutdown_replicas(mut replicas: Vec<Replica>) {
    while let Some(r) = replicas.pop() {
        r.shutdown().await;
        log(format!("SHUTDOWN").to_string());
    }
}