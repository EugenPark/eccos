use crate::stat::Stats;
use log::{info, trace};
use network::{send, Address, Endpoint, Payload};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use simple_db::{generate_random_kv, CmdType, Command, DistributionType};
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};
use uuid::Uuid;

pub async fn generate_random_requests(
    pid: u64,
    nodes: Vec<u64>,
    rate_per_seconds: f64,
    duration_as_secs: u64,
    distribution: DistributionType,
    stats: Arc<Mutex<Stats>>,
) {
    // INFO: Send requests for the duration of the experiment
    let mut rng = SmallRng::seed_from_u64(pid);

    let duration = std::time::Duration::from_secs(duration_as_secs);
    let start = std::time::Instant::now();

    while start.elapsed() < duration {
        let inter_arrival_time = compute_inter_arrival_time(rate_per_seconds, &mut rng);
        sleep(Duration::from_secs_f64(inter_arrival_time)).await;

        let id = Uuid::new_v4();
        let kv = generate_random_kv(&distribution);
        let cmd_type = CmdType::Put(kv);
        let index = rng.gen_range(0..=nodes.len() - 1);
        let coordinator_id: u64 = index.try_into().expect("Should be able to convert");

        let command = Command::new(id, cmd_type, pid, coordinator_id);
        let payload = Payload::ClientRequest(command);

        let server_domain = format!("cc_server{}", nodes[index].to_string());
        let endpoint = Endpoint::new(Address::Domain(server_domain), 8080);

        // INFO: Save the request for the statistics later
        stats.lock().await.send_request(id);

        trace!("Sending Payload {:?} to {}", payload, endpoint.to_string());
        send(&payload, endpoint)
            .await
            .expect("Sends payload successfully over the network");
    }
}

pub async fn send_end_signal(pid: u64, server_ids: Vec<u64>) {
    info!("Experiment ended - Stop sending request");
    let payload = Payload::EndExperiment(pid);
    for server_id in server_ids {
        let server_domain = format!("cc_server{}", server_id.to_string());
        let endpoint = Endpoint::new(Address::Domain(server_domain), 8080);
        send(&payload, endpoint)
            .await
            .expect("Should send successfully");
    }
}

// INFO: taken from https://stackoverflow.com/questions/5148635/how-to-simulate-poisson-arrival
fn compute_inter_arrival_time(rate: f64, rng: &mut SmallRng) -> f64 {
    let uniform_random: f64 = rng.gen(); // Random number between 0 and 1

    // Exponential distribution formula: -ln(1 - U) / λ
    -((1.0 - uniform_random).ln()) / rate
}
