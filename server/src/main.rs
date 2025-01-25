use common::KeyRange;
use consensus::ConsensusLayer;
use db::DBLayer;
use env_logger;
use log::info;
use network::NetworkLayer;
use once_cell::sync::Lazy;
use scheduler::SchedulerLayer;
use serde::Deserialize;
use std::{env, fs};
use tokio::{sync::mpsc, time};

mod common;
mod consensus;
mod db;
mod network;
mod scheduler;

#[derive(Debug, Deserialize)]
struct Config {
    server_ids: Vec<u64>,
    client_ids: Vec<u64>,
    port: u16,
    partitions: Vec<[char; 2]>,
}

pub static PID: Lazy<u64> = Lazy::new(|| {
    let pid = env::var("PID").expect("A PID must be specified");
    let pid = pid.parse().expect("PIDs must be u64");
    match pid {
        0 => panic!("PIDs cannot be 0"),
        pid => pid,
    }
});

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();

    // Read the TOML file
    let config_data = fs::read_to_string("config.toml").expect("Unable to read file");

    // Parse the TOML data
    let config: Config = toml::de::from_str(&config_data).expect("Failed to parse TOML");

    let partition_ranges: Vec<KeyRange> = config
        .partitions
        .iter()
        .map(|key_pair| (key_pair[0], key_pair[1]))
        .collect();

    let (tx_network_scheduler, rx_network_scheduler) = mpsc::channel(1000);
    let (tx_scheduler_network, rx_scheduler_network) = mpsc::channel(1000);

    let mut network = NetworkLayer::new(
        config.port,
        config.server_ids.clone(),
        tx_network_scheduler,
        rx_scheduler_network,
    );

    let mut zipped_channels_scheduler = vec![];

    let (tx_consensus_db, rx_consensus_db) = mpsc::channel(1000);

    let mut consensus_instances = vec![];
    for partition_range in partition_ranges {
        let (tx_scheduler_consensus, rx_scheduler_consensus) = mpsc::channel(1000);
        let (tx_consensus_scheduler, rx_consensus_scheduler) = mpsc::channel(1000);

        let zip_scheduler = (
            partition_range,
            tx_scheduler_consensus,
            rx_consensus_scheduler,
        );

        zipped_channels_scheduler.push(zip_scheduler);

        let tx_consensus_db = tx_consensus_db.clone();
        let consensus = ConsensusLayer::new(
            *PID,
            config.server_ids.clone(),
            partition_range,
            tx_consensus_scheduler,
            rx_scheduler_consensus,
            tx_consensus_db,
        );
        consensus_instances.push(consensus);
    }

    let mut scheduler = SchedulerLayer::new(
        tx_scheduler_network,
        rx_network_scheduler,
        zipped_channels_scheduler,
    );

    let mut db = DBLayer::new(rx_consensus_db);
    info!("Initialisation complete");

    info!("Starting Network");
    let network_handle = tokio::spawn(async move { network.run().await });

    info!("Starting Scheduler");
    let scheduler_handle =
        tokio::spawn(async move { scheduler.run(config.client_ids.clone()).await });

    info!("Starting DB");
    let db_handle = tokio::spawn(async move { db.run().await });

    // TODO: specify init properly instead of just sleeping
    time::sleep(time::Duration::from_secs(3)).await;

    info!("Starting Consensus");
    let mut consensus_handles = vec![];
    for mut consensus_instance in consensus_instances {
        let consensus_handle = tokio::spawn(async move { consensus_instance.run().await });
        consensus_handles.push(consensus_handle);
    }

    for consensus_handle in consensus_handles {
        consensus_handle.await.unwrap();
    }

    network_handle.await.unwrap();
    scheduler_handle.await.unwrap();
    db_handle.await.unwrap();

    Ok(())
}
