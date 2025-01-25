use crate::common::message::{NetworkToScheduler, SchedulerToNetwork};
use bincode;
use log::{error, info, trace};
use network::{Address, Endpoint, Payload};
use std::{net::Ipv4Addr, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
};

struct SchedulerInterface {
    rx: Option<Receiver<SchedulerToNetwork>>, // NOTE: This option is required in order to move the rx to the thread
    tx: Sender<NetworkToScheduler>,
}

impl SchedulerInterface {
    fn new(rx: Receiver<SchedulerToNetwork>, tx: Sender<NetworkToScheduler>) -> Self {
        SchedulerInterface { rx: Some(rx), tx }
    }
}

pub struct NetworkLayer {
    network: Arc<Network>,
    scheduler_interface: SchedulerInterface,
}

impl NetworkLayer {
    pub fn new(
        port: u16,
        nodes: Vec<u64>,
        tx: Sender<NetworkToScheduler>,
        rx: Receiver<SchedulerToNetwork>,
    ) -> Self {
        NetworkLayer {
            network: Arc::new(Network::new(port, nodes)),
            scheduler_interface: SchedulerInterface::new(rx, tx),
        }
    }

    pub async fn run(&mut self) {
        let tx = self.scheduler_interface.tx.clone();

        let listen_handle = tokio::spawn({
            let network = Arc::clone(&self.network);
            async move {
                network.listen_incoming(tx).await;
            }
        });

        let network = Arc::clone(&self.network);
        let rx = self.scheduler_interface.rx.take();

        let outgoing_handle = tokio::spawn({
            let network = Arc::clone(&network);
            async move {
                network.send_outgoing(rx.expect("Should not be None")).await;
            }
        });

        outgoing_handle.await.unwrap();
        listen_handle.await.unwrap();
    }
}

struct Network {
    port: u16,
    nodes: Vec<u64>,
}

impl Network {
    fn new(port: u16, nodes: Vec<u64>) -> Self {
        Network { port, nodes }
    }

    async fn listen_incoming(&self, tx: Sender<NetworkToScheduler>) {
        let endpoint = Endpoint::new(Address::IP(Ipv4Addr::new(0, 0, 0, 0)), self.port);
        let listener = TcpListener::bind(&endpoint.to_string()).await.unwrap();

        info!("Listening on {}", &endpoint);

        loop {
            let (socket, connecting_addr) = listener.accept().await.unwrap();
            trace!("Connection received from {}", connecting_addr.ip());
            tokio::spawn(Network::handle_connection(socket, tx.clone()))
                .await
                .unwrap();
        }
    }

    async fn send_outgoing(&self, mut rx: Receiver<SchedulerToNetwork>) {
        while let Some(payload) = rx.recv().await {
            match payload {
                Payload::ClientResponse(ref cmd) => {
                    let receiver = cmd.sender_id.to_string();
                    let domain = format!("cc_client{}", receiver);
                    let endpoint = Endpoint::new(Address::Domain(domain), 8080);

                    tokio::spawn(async move {
                        network::send(&payload, endpoint)
                            .await
                            .expect("Sends payload successfully over the network")
                    });
                }
                Payload::ServerMessage((ref _key, ref op_msg)) => {
                    let receiver = op_msg.get_receiver().to_string();
                    let domain = format!("cc_server{}", receiver);
                    let endpoint = Endpoint::new(Address::Domain(domain), 8080);
                    tokio::spawn(async move {
                        network::send(&payload, endpoint)
                            .await
                            .expect("Sends payload successfully over the network")
                    });
                }
                Payload::AckEndExperiment(_) => {
                    for node in self.nodes.clone() {
                        let domain = format!("cc_client{}", node);
                        let endpoint = Endpoint::new(Address::Domain(domain), 8080);

                        network::send(&payload, endpoint)
                            .await
                            .expect("Sends payload successfully over the network");
                    }
                }
                Payload::EndExperiment(_) => panic!("Server can not initiate end of experiment!"),
                Payload::ClientRequest(_) => panic!("Server can not initiate a client request!"),
            };
        }
    }

    async fn handle_connection(mut socket: TcpStream, tx: Sender<NetworkToScheduler>) {
        let mut buf = Vec::new();
        match socket.read_to_end(&mut buf).await {
            Ok(_) => {
                let payload = bincode::deserialize(&buf[..]).unwrap();
                trace!("Forwarding Payload to Scheduler: {:?}", payload);
                tx.send(payload)
                    .await
                    .expect("Should be able to send successfully");
            }
            Err(e) => {
                error!("Failed to read from Socket: {}", e);
            }
        };
    }
}
