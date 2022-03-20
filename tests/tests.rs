mod common;

use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{Consistency, Query};
use std::sync::{Arc, Mutex};


#[tokio::test]
async fn test_add() {
    // using common code
    let setup =  tokio::task::spawn(async {common::setup().await});
    // wait for set up to finish
    let result = tokio::join!(setup);
    // todo: check that the result is not an error

    // start tests
    let result2 = run_query(1, "http://127.0.0.1:50001".to_string(), "CREATE TABLE IDS (ID INTEGER);".to_string()).await;

}

async fn run_query(num: u64,addr: String, line: String) -> Result<proto::QueryResults, Box<dyn std::error::Error>>{
    let mut client = RpcClient::connect(addr).await?;
    let query = tonic::Request::new(Query {
        sql: line.to_string(),
        consistency: Consistency::RelaxedReads as i32,
    });
    let response = client.execute(query).await?;
    let response = response.into_inner();
    Ok(response)
}