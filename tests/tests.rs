mod common;

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
 use futures;

 use std::time::Duration;
#[tokio::test(flavor = "multi_thread")]
async fn test1_simple_query() {

  // set up the servers  
  let mut replicas = common::setup(2).await;
  tokio::time::sleep(Duration::from_millis(20)).await;
  // run test
  tokio::task::spawn(async {
      let res = common::run_query(1, String::from("SELECT 1+1;")).await.unwrap();
      assert!(res == "2");
  }).await.unwrap();
  tokio::time::sleep(Duration::from_millis(20)).await;
  common::shutdown_replicas(replicas).await;
}



#[tokio::test(flavor = "multi_thread")]
async fn test2_create_insert_select() {
    // set up the servers
    let mut replicas = common::setup(2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    // run test
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T1 (I INTEGER);")).await.unwrap();

        common::run_query(1, String::from("INSERT INTO T1 (I) VALUES (1);")).await.unwrap();

        let res = common::run_query(1, String::from("SELECT * FROM T1;")).await.unwrap();
        assert!(res == "1");
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    common::shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test3_create_insert_select() {
    // set up the servers
    let mut replicas = common::setup(2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    // run test
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T1 (I INTEGER);")).await.unwrap();

        common::run_query(1, String::from("INSERT INTO T1 (I) VALUES (1);")).await.unwrap();

        let res = common::run_query(2, String::from("SELECT * FROM T1;")).await.unwrap();
        assert!(res == "1");
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    common::shutdown_replicas(replicas).await;
}