use anyhow::Result;
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use std::sync::{Arc, Mutex};
use structopt::StructOpt;
use tonic::transport::Server;
use std::time::Duration;

#[derive(StructOpt, Debug)]
#[structopt(name = "gouged")]
struct Opt {
    /// The ID of this server.
    #[structopt(short, long)]
    id: usize,
    /// The IDs of peers.
    #[structopt(short, long, required = false)]
    peers: Vec<usize>,
}

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

use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Config, Root};


#[tokio::main]
async fn main() -> Result<()> {

    // get arguments
    let opt = Opt::from_args();

    // convert to u64
    let id = opt.id as u64;
    let peers: Vec<u64> = opt.peers.into_iter().map(|x| x as u64).collect();
    
    // log messages
    let logfile = FileAppender::builder()
    .encoder(Box::new(PatternEncoder::new("{l} - {m}\n")))
    .build(format!("log/output{}.log",id))?;

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder()
                .appender("logfile")
                .build(LevelFilter::Info))?;

    log4rs::init_config(config)?;


    // get host and port
    let (host, port) = node_authority(id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();

    // create an RPC transport
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    // start server
    let server = StoreServer::start(id, peers, transport)?;
    
    let server = Arc::new(server);

    let f = {
        let server = server.clone();
        tokio::task::spawn( async move {
            server.run();
        })
    };


    // create RPC Service
    let rpc = RpcService::new(server);
    let g = tokio::task::spawn(async move {
        println!("RPC listening to {} ...", rpc_listen_addr);
        let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve(rpc_listen_addr)
            .await;
        ret
    });
    let results = tokio::try_join!(f, g)?;

    results.1?;
    Ok(())
}

