use crate::common::message::{
    ConsensusToScheduler, NetworkToScheduler, SchedulerToConsensus, SchedulerToNetwork,
};
use crate::scheduler::{
    consensus_out_scheduler::ConsensusOutScheduler, network_in_scheduler::NetworkInScheduler,
};
use crate::KeyRange;
use tokio::sync::mpsc::{Receiver, Sender};

pub mod consensus_out_scheduler;
pub mod network_in_scheduler;

pub struct SchedulerLayer {
    incoming_scheduler: Option<NetworkInScheduler>,
    outgoing_scheduler: Option<ConsensusOutScheduler>,
    network_interface: NetworkInterface,
    consensus_interfaces: Vec<ConsensusInterface>,
}

pub enum SchedulingStrategy {
    FCFS,
    RoundRobin,
}

impl SchedulerLayer {
    pub fn new(
        tx: Sender<SchedulerToNetwork>,
        rx: Receiver<NetworkToScheduler>,
        zipped_channels: Vec<(
            KeyRange,
            Sender<SchedulerToConsensus>,
            Receiver<ConsensusToScheduler>,
        )>,
    ) -> Self {
        let network_interface = NetworkInterface::new(tx, rx);
        let mut consensus_interfaces = vec![];
        for zipped_channel in zipped_channels.into_iter() {
            let (key_range, tx, rx) = zipped_channel;
            let consensus_interface = ConsensusInterface::new(key_range, tx, rx);
            consensus_interfaces.push(consensus_interface);
        }
        let incoming_scheduler = Some(NetworkInScheduler::new(SchedulingStrategy::FCFS));
        let outgoing_scheduler = Some(ConsensusOutScheduler::new(SchedulingStrategy::RoundRobin));
        SchedulerLayer {
            incoming_scheduler,
            outgoing_scheduler,
            network_interface,
            consensus_interfaces,
        }
    }

    pub async fn run(&mut self, clients: Vec<u64>) {
        let rx = self.network_interface.rx.take();
        let txs: Vec<(KeyRange, Sender<SchedulerToConsensus>)> = self
            .consensus_interfaces
            .iter()
            .map(|consensus_interface| {
                (
                    consensus_interface.key_range,
                    consensus_interface.tx.clone(),
                )
            })
            .collect();
        let incoming_scheduler = self.incoming_scheduler.take();

        let incoming_handle = tokio::spawn(async move {
            let incoming_scheduler = incoming_scheduler.expect("Should not be None");
            let rx = rx.expect("Not None");
            incoming_scheduler.start(rx, txs, clients);
        });

        let rxs: Vec<Option<Receiver<ConsensusToScheduler>>> = self
            .consensus_interfaces
            .iter_mut()
            .map(|consensus_interface| consensus_interface.rx.take())
            .collect();
        let tx = self.network_interface.tx.clone();
        let outgoing_scheduler = self.outgoing_scheduler.take();

        let outgoing_handle = tokio::spawn(async move {
            let outgoing_scheduler = outgoing_scheduler.expect("Should not be None");
            outgoing_scheduler.start(rxs, tx);
        });

        incoming_handle.await.unwrap();
        outgoing_handle.await.unwrap();
    }
}

struct NetworkInterface {
    rx: Option<Receiver<NetworkToScheduler>>,
    tx: Sender<SchedulerToNetwork>,
}

impl NetworkInterface {
    fn new(tx: Sender<SchedulerToNetwork>, rx: Receiver<NetworkToScheduler>) -> Self {
        NetworkInterface { rx: Some(rx), tx }
    }
}

struct ConsensusInterface {
    key_range: KeyRange,
    tx: Sender<SchedulerToConsensus>,
    rx: Option<Receiver<ConsensusToScheduler>>, // NOTE: This option is required in order to move the rx to the thread
}

impl ConsensusInterface {
    fn new(
        key_range: KeyRange,
        tx: Sender<SchedulerToConsensus>,
        rx: Receiver<ConsensusToScheduler>,
    ) -> Self {
        ConsensusInterface {
            key_range,
            tx,
            rx: Some(rx),
        }
    }
}
