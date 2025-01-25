use crate::common::message::{ConsensusToScheduler, SchedulerToNetwork};
use crate::scheduler::SchedulingStrategy;
use log::trace;
use network::Payload;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{
    mpsc::{error::TryRecvError, Receiver, Sender},
    Mutex,
};

pub struct ConsensusOutScheduler {
    msg_queue: std::sync::Arc<Mutex<VecDeque<Payload>>>,
    strategy: SchedulingStrategy,
}

impl ConsensusOutScheduler {
    pub fn new(strategy: SchedulingStrategy) -> Self {
        ConsensusOutScheduler {
            msg_queue: Arc::new(Mutex::new(VecDeque::new())),
            strategy,
        }
    }

    async fn round_robin(
        mut rxs: Vec<Option<Receiver<ConsensusToScheduler>>>,
        msg_queue: Arc<Mutex<VecDeque<Payload>>>,
    ) {
        let mut acked_consensus = false;
        loop {
            let mut msg_queue = msg_queue.lock().await;
            for rx in rxs.iter_mut() {
                match rx.as_mut().expect("Receiver should not be none").try_recv() {
                    Ok(Payload::AckEndExperiment(pid)) if !acked_consensus => {
                        acked_consensus = true;
                        msg_queue.push_front(Payload::AckEndExperiment(pid));
                    }
                    Ok(Payload::AckEndExperiment(_pid)) => (), // NOTE: Drop the message - only ack once
                    Ok(payload) => msg_queue.push_back(payload),
                    Err(TryRecvError::Empty) => {
                        trace!("Channel is currently empty checking next channel")
                    }
                    Err(TryRecvError::Disconnected) => {
                        panic!("Channel closed unexpectedly!")
                    }
                }
            }
        }
    }

    async fn network_outgoing(
        tx: &Sender<SchedulerToNetwork>,
        msg_queue: Arc<Mutex<VecDeque<Payload>>>,
    ) {
        loop {
            let mut msg_queue = msg_queue.lock().await;

            while let Some(payload) = msg_queue.pop_front() {
                tx.send(payload).await.expect("Sends successfully");
            }
        }
    }

    pub fn start(
        &self,
        rxs: Vec<Option<Receiver<ConsensusToScheduler>>>,
        tx: Sender<SchedulerToNetwork>,
    ) {
        let msg_queue = Arc::clone(&self.msg_queue);
        let consensus_incoming = match self.strategy {
            SchedulingStrategy::RoundRobin => ConsensusOutScheduler::round_robin,
            _ => unimplemented!(),
        };
        tokio::spawn(async move {
            consensus_incoming(rxs, msg_queue).await;
        });

        let msg_queue = Arc::clone(&self.msg_queue);
        tokio::spawn(async move {
            ConsensusOutScheduler::network_outgoing(&tx, msg_queue).await;
        });
    }
}
