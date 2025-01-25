use bincode;
use log::trace;
use omnipaxos::messages::Message as OPMessage;
use serde::{Deserialize, Serialize};
use simple_db::{Command, Key};
use std::{fmt, net::Ipv4Addr};
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub type Recipient = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    ServerMessage((Key, OPMessage<Command>)),
    ClientRequest(Command),
    ClientResponse(Command),
    EndExperiment(u64),
    AckEndExperiment(u64),
}

#[derive(PartialEq, Hash, Eq)]
pub enum Address {
    IP(Ipv4Addr),
    Domain(String),
}

#[derive(Eq, Hash, PartialEq)]
pub struct Endpoint {
    address: Address,
    port: u16,
}

impl Endpoint {
    pub fn new(address: Address, port: u16) -> Self {
        Endpoint { address, port }
    }
    fn format_endpoint(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.address {
            Address::IP(ip) => write!(f, "{}:{}", ip, self.port),
            Address::Domain(domain) => write!(f, "{}:{}", domain, self.port),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format_endpoint(f)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format_endpoint(f)
    }
}

pub async fn send(payload: &Payload, to: Endpoint) -> Result<(), std::io::Error> {
    trace!("Sending to {}: {:?}", to.to_string(), payload);

    let mut stream = TcpStream::connect(to.to_string()).await?;
    let payload = bincode::serialize(payload).unwrap();

    stream.write_all(&payload).await
}
