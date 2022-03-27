mod common;

use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn test1_simple_query() {
  let replicas = common::setup(2).await;
  tokio::task::spawn(async {
      let res = common::run_query(1, String::from("SELECT 1+1;")).await.unwrap();
      assert!(res == "2");
  }).await.unwrap();
  tokio::time::sleep(Duration::from_millis(20)).await;
  common::shutdown_replicas(replicas).await;
}



#[tokio::test(flavor = "multi_thread")]
async fn test2_create_insert_select() {
    let replicas = common::setup(2).await;
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T1 (I INTEGER);")).await.unwrap();
        common::run_query(1, String::from("INSERT INTO T1 (I) VALUES (1);")).await.unwrap();
        let res = common::run_query(1, String::from("SELECT * FROM T1;")).await.unwrap();
        assert!(res == "1");
    }).await.unwrap();
    common::run_query(1, String::from("DROP TABLE T1;")).await.unwrap();
    common::shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test3_create_insert_select() {
    let replicas = common::setup(3).await;
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T2 (I INTEGER);")).await.unwrap();
        common::run_query(1, String::from("INSERT INTO T2 (I) VALUES (1);")).await.unwrap();
        let res = common::run_query(2, String::from("SELECT * FROM T2;")).await.unwrap();
        assert!(res == "1");
    }).await.unwrap();
    common::run_query(1, String::from("DROP TABLE T2;")).await.unwrap();
    common::shutdown_replicas(replicas).await;
}

// #[tokio::test(flavor = "multi_thread")]
// async fn test4_reconfiguration() {
//     // set up 3 servers
//     let replicas = common::setup(3).await;
//     tokio::time::sleep(Duration::from_millis(20)).await;
//     // run test
//     // reconfigure
//     // 3 will not be at the new configuration
//     replicas.get(1).unwrap().reconfigure(vec![1, 2], None);
    
//     // wait some arbitrary time
//     // hope it will be decided
//     tokio::time::sleep(Duration::from_millis(40)).await;

//     let peers_after = replicas.get(1).unwrap().get_peers();

//     assert!(peers_after == vec![1, 2]);
//     common::shutdown_replicas(replicas).await;
// }

#[tokio::test(flavor = "multi_thread")]
async fn test5_decide_same_sequence() {
    let replicas = common::setup(2).await;
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T5 (ID INTEGER PRIMARY KEY, V INTEGER);")).await.unwrap();
    }).await.unwrap();
    let write_a = tokio::task::spawn(async {
        common::run_query(1, String::from("INSERT OR REPLACE INTO T5 VALUES(1,1);")).await.unwrap();
    });
    let write_b = tokio::task::spawn(async {
        common::run_query(2, String::from("INSERT OR REPLACE INTO T5 VALUES(1,2);")).await.unwrap();
    });  
    write_a.await.unwrap();
    write_b.await.unwrap();
    let first = common::run_query(1, String::from("SELECT V FROM T5 WHERE ID = 1;")).await.unwrap();
    let second = common::run_query(2, String::from("SELECT V FROM T5 WHERE ID = 1;")).await.unwrap();  
    assert!(first == second);
    common::run_query(1, String::from("DROP TABLE T5;")).await.unwrap();
    common::shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test6_kill_a_node() {
    let mut replicas = common::setup(3).await;
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T6 (ID INTEGER);")).await.unwrap();
    }).await.unwrap();

    let kill_replica = replicas.remove(0);
    kill_replica.shutdown().await;
    let living_replica_id = replicas[0].get_id();
    tokio::task::spawn(async move {
        common::run_query(living_replica_id, String::from("INSERT INTO T6 VALUES(1);")).await.unwrap();
    }).await.unwrap();
    let living_replica_id = replicas[0].get_id();
    tokio::task::spawn(async move {
        common::run_query(living_replica_id, String::from("DROP TABLE T6;")).await.unwrap();
    }).await.unwrap();
    common::shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test7_snapshotting() {
    let replicas = common::setup(2).await;
    tokio::task::spawn(async {
        common::run_query(1, String::from("CREATE TABLE IF NOT EXISTS T7 (ID INTEGER);")).await.unwrap();
        common::run_query(1, String::from("INSERT INTO T7 (ID) VALUES (1);")).await.unwrap();
        common::run_query(1, String::from("INSERT INTO T7 (ID) VALUES (2);")).await.unwrap();
        common::run_query(1, String::from("INSERT INTO T7 (ID) VALUES (3);")).await.unwrap();
    }).await.unwrap();

    // we should be able to snapshot these values
    let is_snapshotted = replicas.get(0).unwrap().snapshot(Some(2));
    // let compacted_idx = replicas.get(0).unwrap().get_compacted_idx();

    assert!(is_snapshotted == true);
    common::shutdown_replicas(replicas).await;
}