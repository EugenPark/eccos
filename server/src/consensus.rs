use crate::common::{
    message::{ConsensusToDB, ConsensusToScheduler, SchedulerToConsensus},
    KeyRange,
};
use log::trace;
use network::Payload;
use omnipaxos::{util::LogEntry::Decided, ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use simple_db::{CmdType, Command};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time,
};

type OmniPaxosKV = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct ConsensusLayer {
    consensus: Consensus,
    scheduler_interface: SchedulerInterface,
    db_interface: DBInterface,
}

impl ConsensusLayer {
    pub fn new(
        pid: u64,
        nodes: Vec<u64>,
        key_range: KeyRange,
        tx_consensus_scheduler: Sender<ConsensusToScheduler>,
        rx_scheduler_consensus: Receiver<SchedulerToConsensus>,
        tx_consensus_db: Sender<ConsensusToDB>,
    ) -> Self {
        let consensus = Consensus::new(pid, nodes, key_range);
        ConsensusLayer {
            consensus,
            scheduler_interface: SchedulerInterface::new(
                tx_consensus_scheduler,
                rx_scheduler_consensus,
            ),
            db_interface: DBInterface::new(tx_consensus_db),
        }
    }

    pub async fn run(&mut self) {
        let mut rx = self
            .scheduler_interface
            .rx
            .take()
            .expect("Channels should be initialised");
        let tx_scheduler = self.scheduler_interface.tx.clone();
        let tx_db = self.db_interface.tx.clone();
        loop {
            tokio::select!(
                biased;
                _ = self.consensus.msg_interval.tick() => {
                    if self.consensus.handle_incoming_messages(&mut rx).is_err() {
                        self.consensus.handle_end_signal(tx_scheduler.clone(), tx_db.clone()).await;
                    }

                    self.consensus.handle_outgoing_messages(&tx_scheduler).await;

                    if !self.consensus.is_finished {
                        self.consensus
                            .handle_decided_entries(&tx_db, &tx_scheduler)
                            .await;
                    }
                },
                _ = self.consensus.tick_interval.tick() => {
                    self.consensus.omnipaxos.tick();
                },
                else => (),
            );
        }
    }
}

struct SchedulerInterface {
    tx: Sender<ConsensusToScheduler>,
    rx: Option<Receiver<SchedulerToConsensus>>, // NOTE: This option is required in order to move the rx to the thread
}

impl SchedulerInterface {
    fn new(tx: Sender<ConsensusToScheduler>, rx: Receiver<SchedulerToConsensus>) -> Self {
        SchedulerInterface { tx, rx: Some(rx) }
    }
}

struct DBInterface {
    tx: Sender<ConsensusToDB>,
}

impl DBInterface {
    fn new(tx: Sender<ConsensusToDB>) -> Self {
        DBInterface { tx }
    }
}

struct Consensus {
    is_finished: bool,
    decided_entries_count: u128,
    pid: u64,
    key_range: KeyRange,
    omnipaxos: OmniPaxosKV,
    msg_interval: time::Interval,
    tick_interval: time::Interval,
    last_decided_index: u64,
}

impl Consensus {
    pub fn new(pid: u64, nodes: Vec<u64>, key_range: KeyRange) -> Self {
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: 5,
            ..Default::default()
        };
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: nodes.clone(),
            ..Default::default()
        };
        let omnipaxos_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };
        let omnipaxos: OmniPaxosKV = omnipaxos_config
            .build(MemoryStorage::default())
            .expect("Failed to build OmniPaxos");

        let msg_interval = time::interval(time::Duration::from_millis(1));
        let tick_interval = time::interval(time::Duration::from_millis(10));

        let last_decided_index = 0;

        Consensus {
            is_finished: false,
            decided_entries_count: 0,
            pid,
            key_range,
            omnipaxos,
            msg_interval,
            tick_interval,
            last_decided_index,
        }
    }

    fn write_stats_to_file(&self) -> std::io::Result<()> {
        std::fs::write(
            format!(
                "/volumes/server_{}_{}-{}.txt",
                self.pid, self.key_range.0, self.key_range.1
            ),
            format!("Number of decided_entries: {}", self.decided_entries_count),
        )?;
        Ok(())
    }

    async fn handle_end_signal(
        &mut self,
        tx_scheduler: Sender<ConsensusToScheduler>,
        tx_db: Sender<ConsensusToDB>,
    ) {
        self.write_stats_to_file().unwrap();
        self.is_finished = true;
        tx_scheduler
            .send(Payload::AckEndExperiment(self.pid))
            .await
            .expect("Sends acknowledgment successfully");
        let id = uuid::Uuid::new_v4();
        let cmd_type = CmdType::Write;
        let command = Command::new(id, cmd_type, 0, self.pid);

        tx_db.send(command).await.expect("It sends successfully");
    }

    fn handle_incoming_messages(
        &mut self,
        rx: &mut Receiver<SchedulerToConsensus>,
    ) -> Result<(), &str> {
        let mut messages: Vec<SchedulerToConsensus> = Vec::new();

        while let Ok(msg) = rx.try_recv() {
            trace!("Process incoming message {:?}", msg);
            messages.push(msg);
        }

        for msg in messages {
            match msg {
                Payload::ServerMessage((_key, msg)) => self.omnipaxos.handle_incoming(msg),
                Payload::ClientRequest(cmd) => {
                    self.omnipaxos.append(cmd).expect("Appends succesfully")
                }
                Payload::EndExperiment(_) => return Err("End of experiment"),
                Payload::AckEndExperiment(_) => unreachable!(),
                Payload::ClientResponse(_) => unreachable!(),
            }
        }

        Ok(())
    }

    async fn handle_outgoing_messages(&mut self, tx: &Sender<ConsensusToScheduler>) {
        let op_messages = self.omnipaxos.outgoing_messages();
        for op_msg in op_messages {
            let payload = Payload::ServerMessage((self.key_range.0, op_msg));
            tx.send(payload).await.unwrap();
        }
    }

    async fn handle_decided_entries(
        &mut self,
        tx_db: &Sender<ConsensusToDB>,
        tx_scheduler: &Sender<ConsensusToScheduler>,
    ) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.last_decided_index < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.last_decided_index)
                .unwrap();
            for entry in decided_entries {
                match entry {
                    Decided(cmd) => {
                        self.decided_entries_count += 1;
                        tx_db.send(cmd.clone()).await.unwrap();

                        if cmd.coordinator_id == self.pid {
                            let payload = Payload::ClientResponse(cmd);
                            tx_scheduler.send(payload).await.unwrap();
                        }
                    }
                    _ => panic!("Should not be not decided"),
                };
            }
            self.last_decided_index = new_decided_idx;
        }
    }
}
