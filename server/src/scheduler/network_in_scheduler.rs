use crate::common::{
    experiment::ClientStatus,
    is_in_key_range,
    message::{NetworkToScheduler, SchedulerToConsensus},
    KeyRange,
};
use crate::scheduler::SchedulingStrategy;
use network::Payload;
use simple_db::CmdType;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{
    mpsc::{error::TryRecvError, Receiver, Sender},
    Mutex,
};

pub struct NetworkInScheduler {
    msg_queue: std::sync::Arc<Mutex<VecDeque<(char, Payload)>>>,
    strategy: SchedulingStrategy,
}

impl NetworkInScheduler {
    pub fn new(strategy: SchedulingStrategy) -> Self {
        NetworkInScheduler {
            msg_queue: Arc::new(Mutex::new(VecDeque::new())),
            strategy,
        }
    }

    fn find_channel(
        key: &char,
        txs: &Vec<(KeyRange, Sender<SchedulerToConsensus>)>,
    ) -> Sender<Payload> {
        txs.iter()
            .find(|&(key_range, _)| is_in_key_range(key, key_range))
            .expect("There should be a matching key range")
            .1
            .clone()
    }

    pub fn start(
        &self,
        mut rx: Receiver<NetworkToScheduler>,
        txs: Vec<(KeyRange, Sender<SchedulerToConsensus>)>,
        clients: Vec<u64>,
    ) {
        let msg_queue = Arc::clone(&self.msg_queue);
        let key_ranges: Vec<(char, char)> = txs
            .iter()
            .map(|(key_range, _tx)| key_range.clone())
            .collect();

        let network_incoming = match self.strategy {
            SchedulingStrategy::FCFS => NetworkInScheduler::fcfs,
            _ => unimplemented!(),
        };

        tokio::spawn(async move {
            let mut client_status = ClientStatus::new(clients);
            loop {
                let cloned_msg_queue = Arc::clone(&msg_queue);
                network_incoming(&mut rx, cloned_msg_queue, &key_ranges, &mut client_status).await;
            }
        });

        let msg_queue = Arc::clone(&self.msg_queue);
        tokio::spawn(async move {
            NetworkInScheduler::consensus_outgoing(msg_queue, &txs).await;
        });
    }

    async fn consensus_outgoing(
        msg_queue: Arc<Mutex<VecDeque<(char, Payload)>>>,
        txs: &Vec<(KeyRange, Sender<SchedulerToConsensus>)>,
    ) {
        loop {
            let mut msg_queue = msg_queue.lock().await;
            while let Some((key, payload)) = msg_queue.pop_front() {
                let tx = NetworkInScheduler::find_channel(&key, &txs);
                tx.send(payload).await.expect("Sends successfully");
            }
        }
    }

    async fn fcfs(
        rx: &mut Receiver<NetworkToScheduler>,
        msg_queue: Arc<Mutex<VecDeque<(char, Payload)>>>,
        key_ranges: &Vec<KeyRange>,
        client_status: &mut ClientStatus,
    ) {
        let mut msg_queue = msg_queue.lock().await;
        match rx.try_recv() {
            Ok(payload) => match payload {
                Payload::ServerMessage((key, _)) => msg_queue.push_back((key, payload)),
                Payload::ClientRequest(ref command) => {
                    let key = match &command.cmd_type {
                        CmdType::Put(kv) => kv.key,
                        CmdType::Write => panic!("Client should not initiate a db write"),
                    };
                    msg_queue.push_back((key, payload));
                }
                Payload::EndExperiment(client_id) => {
                    client_status.client_shutdown(client_id);
                    if client_status.are_all_clients_stopped() {
                        // NOTE: Send end experiment to all consensus instances
                        for key_range in key_ranges {
                            msg_queue.push_front((key_range.0, Payload::EndExperiment(client_id)));
                        }
                    }
                }
                Payload::ClientResponse(_) => {
                    panic!("Server should not receive client response!")
                }
                Payload::AckEndExperiment(_) => unreachable!(),
            },
            Err(TryRecvError::Empty) => (),
            Err(TryRecvError::Disconnected) => {
                panic!("Channel closed unexpectedly!")
            }
        }
    }
}
