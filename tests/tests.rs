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

#[tokio::test]
async fn simpler() {
    assert!(1 == 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_query() {
    // set up the servers
    log(format!("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIi").to_string());
    
    let mut replicas = common::setup(2).await;
    log(format!("OOOOOOOOOOOOOOOOOOOOOOOOOOoOOOOoOO").to_string());
    // run test
    let res = tokio::task::spawn(async {
        log(format!("beforeeeeeee query").to_string());
        let res = common::run_query(1, String::from("SELECT 1+1;")).await.unwrap();
        // log(format!("after query").to_string());
        // assert!(res == "3");
        res
    }).await.unwrap();
    // log(format!("beforeeeeeee query").to_string());
    // let res = common::run_query(1, String::from("CREATE TABLE X (X INTEGER);")).await.unwrap();
    // assert!(res == "3");
    // log(format!("after query").to_string());
    let res = common::run_query(1, String::from("CREATE TABLE X (X INTEGER);")).await;
    tokio::join!(res);
    let res = res.unwrap();
    assert!(res == "3");
    log(format!("enddddddddddddddddddddddd").to_string());
}
