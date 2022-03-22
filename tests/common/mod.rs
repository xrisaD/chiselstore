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
    // stop_sender: tokio::sync::oneshot::Sender<()>,
    // shutdown_sender: tokio::sync::oneshot::Sender<()>,
}


impl Replica {
    // pub async fn shutdown(self) {
    //     self.shutdown_sender.send(());
    //     self.rpc_handle.await.unwrap();

    //     self.halt_sender.send(());
    //     self.store_message_handle.await.unwrap();
    //     self.store_ble_handle.await.unwrap();
    // }

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

async fn start_server(id: u64, peers: Vec<u64>) ->Replica {
    let (host, port) = node_authority(id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    let server = StoreServer::start(id, peers, transport).unwrap();
    let server = Arc::new(server);

    let (kill_sender, kill_receiver) = oneshot::channel::<()>();
    let f = {
        
        let server = server.clone();
        tokio::task::spawn_blocking(move || {
            server.run();
        })
    };

    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let g = {
        let server = server.clone();
        let rpc = RpcService::new(server);
        tokio::task::spawn(async move {
            let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve_with_shutdown(rpc_listen_addr, shutdown_receiver.map(drop))
            .await;
            
            ret
        })
    };
    // todo: check the results that there is not error
    return Replica {
        store_server: server.clone(),
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

async fn shutdown_replicas(mut replicas: Vec<Replica>) {
    // while let Some(r) = replicas.pop() {
    //     r.shutdown().await;
    // }
}