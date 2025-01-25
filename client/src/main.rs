use crate::stat::Stats;
use env_logger;
use log::{error, info};
use once_cell::sync::Lazy;
use serde::Deserialize;
use simple_db::DistributionType;
use std::{
    sync::Arc,
    {env, fs},
};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};

mod listener;
mod request;
mod stat;

#[derive(Debug, Deserialize)]
struct ExperimentConfig {
    duration: u64,
    requests_per_second: f64,
    distribution_type: DistributionType,
}

#[derive(Debug, Deserialize)]
struct Config {
    server_ids: Vec<u64>,
    port: u16,
    experiment: ExperimentConfig,
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

    // Let the network and consensus boot up
    sleep(Duration::from_secs(6)).await;

    // NOTE: We need to access the stats across multiple threads
    let stats = Arc::new(Mutex::new(Stats::new()));

    // NOTE: Listen as long as the main thread is alive
    let cloned_stats = Arc::clone(&stats);
    let request_listener_handle = tokio::spawn(listener::listen(
        config.port,
        config.server_ids.clone(),
        cloned_stats,
    ));

    let cloned_stats = Arc::clone(&stats);
    info!("Start sending requests");
    let request_generator_handle = tokio::spawn(request::generate_random_requests(
        *PID,
        config.server_ids.clone(),
        config.experiment.requests_per_second,
        config.experiment.duration,
        config.experiment.distribution_type,
        cloned_stats,
    ));

    request_generator_handle.await.expect("Exits gracefully");

    request::send_end_signal(*PID, config.server_ids.clone()).await;

    request_listener_handle.await.expect("Exits gracefully");

    let stats = stats.lock().await;

    match stats.get_average_latency_as_micros() {
        Ok(avg) => info!("Average latency in micros: {}", avg),
        Err(err) => error!("{}", err),
    };

    stats.write_to_file(*PID).unwrap();

    Ok(())
}
