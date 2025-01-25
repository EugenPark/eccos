pub type KeyRange = (char, char);
pub fn is_in_key_range(key: &char, key_range: &KeyRange) -> bool {
    key_range.0 <= *key && *key <= key_range.1
}

pub mod message {
    use network::Payload;
    use simple_db::Command;

    pub type SchedulerToNetwork = Payload;
    pub type NetworkToScheduler = Payload;

    pub type SchedulerToConsensus = Payload;
    pub type ConsensusToScheduler = Payload;

    pub type ConsensusToDB = Command;
}

pub mod experiment {
    use std::collections::HashMap;
    pub struct ClientStatus {
        clients: HashMap<u64, bool>,
    }

    impl ClientStatus {
        pub fn new(client_ids: Vec<u64>) -> ClientStatus {
            let mut clients = HashMap::new();
            for client_id in client_ids {
                clients.insert(client_id, true);
            }
            ClientStatus { clients }
        }

        pub fn are_all_clients_stopped(&self) -> bool {
            self.clients.iter().all(|(_client_id, running)| !running)
        }

        pub fn client_shutdown(&mut self, client_id: u64) {
            self.clients.insert(client_id, false);
        }
    }
}
