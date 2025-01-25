use crate::common::message::ConsensusToDB;
use log::{debug, info, trace};
use simple_db::SimpleDB;
use tokio::sync::mpsc::Receiver;

pub struct DBLayer {
    db: DB, // NOTE: This option is required in order to move the db to the thread
    consensus_interface: ConsensusInterface,
}

impl DBLayer {
    pub fn new(rx: Receiver<ConsensusToDB>) -> Self {
        DBLayer {
            consensus_interface: ConsensusInterface::new(rx),
            db: DB::new(),
        }
    }

    pub async fn run(&mut self) {
        let rx = self.consensus_interface.rx.take().unwrap();
        self.db.listen(rx).await;
    }
}

struct ConsensusInterface {
    rx: Option<Receiver<ConsensusToDB>>, // NOTE: This option is required in order to move the rx to the thread
}

impl ConsensusInterface {
    fn new(rx: Receiver<ConsensusToDB>) -> Self {
        ConsensusInterface { rx: Some(rx) }
    }
}

struct DB {
    db: SimpleDB,
}

impl DB {
    fn new() -> Self {
        DB {
            db: SimpleDB::new(),
        }
    }

    async fn listen(&mut self, mut rx: Receiver<ConsensusToDB>) {
        info!("Database is listening");

        let db = &mut self.db;
        loop {
            let message = rx.recv().await.unwrap();
            db.apply_cmd(&message);

            debug!("Applied CMD: {:?}", message);
            trace!("Database: {:?}", db);
        }
    }
}
