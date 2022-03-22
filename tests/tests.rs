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

#[tokio::test(flavor = "multi_thread")]
async fn simple_query() {
    // set up the servers
    log(format!("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIi").to_string());
    
    let mut replicas = common::setup(2).await;
    log(format!("OOOOOOOOOOOOOOOOOOOOOOOOOOoOOOOoOO").to_string());
    // run test
    tokio::task::spawn(async {
        log(format!("beforeeeeeee query").to_string());
        let res = common::run_query(1, String::from("SELECT 1+1;")).await.unwrap();
        log(format!("after query").to_string());
        assert!(res == "2");
    }).await.unwrap();

    log(format!("enddddddddddddddddddddddd").to_string());
    common::shutdown_replicas(replicas).await;
}
