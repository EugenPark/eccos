use crate::stat::Stats;
use log::{debug, info, trace};
use network::{Address, Endpoint, Payload};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

enum Status {
    RUNNING,
    STOPPED,
}

struct ServerStatus {
    status_map: HashMap<u64, Status>,
}

impl ServerStatus {
    fn new(nodes: Vec<u64>) -> Self {
        let mut status_map = HashMap::new();
        for node in nodes {
            status_map.insert(node, Status::RUNNING);
        }

        ServerStatus { status_map }
    }

    fn server_stopped(&mut self, id: u64) {
        self.status_map.insert(id, Status::STOPPED);
    }

    fn are_all_server_stopped(&self) -> bool {
        self.status_map.iter().all(|(_id, status)| match status {
            Status::RUNNING => false,
            Status::STOPPED => true,
        })
    }
}

// INFO: Listen here to responses from the server
pub async fn listen(port: u16, nodes: Vec<u64>, stats: Arc<Mutex<Stats>>) {
    let mut server_status = ServerStatus::new(nodes);

    let endpoint = Endpoint::new(Address::IP(Ipv4Addr::new(0, 0, 0, 0)), port);
    let listener = TcpListener::bind(&endpoint.to_string()).await.unwrap();

    info!("Listening on {}", &endpoint);
    loop {
        let (socket, connecting_addr) = listener.accept().await.unwrap();
        trace!("Connection received from {}", connecting_addr.ip());
        match read_payload(socket).await.unwrap() {
            Payload::ClientResponse(response) => stats.lock().await.receive_request(&response.id),
            Payload::AckEndExperiment(node_id) => {
                server_status.server_stopped(node_id);
                // NOTE: Once all servers stop running we can stop listening
                if server_status.are_all_server_stopped() {
                    trace!("All servers shut down experiment is concluded");
                    break;
                }
            }
            _ => unreachable!(),
        }
    }
}

// INFO: Deserialize the payload that was sent over the network
async fn read_payload(mut socket: TcpStream) -> Result<Payload, String> {
    let mut buf = Vec::new();
    match socket.read_to_end(&mut buf).await {
        Ok(_) => {
            let payload: Payload = bincode::deserialize(&buf[..]).unwrap();
            debug!("Received Response: {:?}", payload);
            Ok(payload)
        }
        Err(e) => Err(format!("Failed to read from Socket: {}", e)),
    }
}
