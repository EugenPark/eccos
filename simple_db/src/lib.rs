use log::debug;
use omnipaxos::macros::Entry;
use rand::{distributions::WeightedIndex, rngs::ThreadRng, Rng};
use rand_distr::{Distribution, Normal};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub type Key = char;

pub struct SimpleDB {
    db: HashMap<char, i8>,
}

impl SimpleDB {
    // INFO: This is a simple database which has keys from a to z and values from 0 to 10
    pub fn new() -> Self {
        let mut db = HashMap::new();
        for c in 'a'..='z' {
            db.insert(c, 0);
        }
        SimpleDB { db }
    }

    pub fn apply_cmd(&mut self, cmd: &Command) {
        match &cmd.cmd_type {
            CmdType::Put(kv) => {
                self.db.insert(kv.key, kv.value);
            }
            CmdType::Write => {
                debug!("Simple DB: {}", self);
            }
        };
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", "{")?;
        for c in 'a'..='z' {
            writeln!(
                f,
                "{}:{}",
                c,
                self.db.get(&c).expect("Every char should be one key")
            )?;
        }
        writeln!(f, "{}", "}")
    }
}

impl std::fmt::Debug for SimpleDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f)
    }
}

impl std::fmt::Display for SimpleDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Entry)]
pub struct Command {
    pub id: Uuid,
    pub cmd_type: CmdType,
    pub sender_id: u64,
    pub coordinator_id: u64,
}

impl Command {
    pub fn new(id: Uuid, cmd_type: CmdType, sender_id: u64, coordinator_id: u64) -> Self {
        Command {
            id,
            cmd_type,
            sender_id,
            coordinator_id,
        }
    }

    pub fn get_key(&self) -> char {
        match &self.cmd_type {
            CmdType::Put(kv) => kv.key,
            CmdType::Write => unimplemented!(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum CmdType {
    Put(KeyValue),
    Write,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: char,
    pub value: i8,
}

impl KeyValue {
    pub fn new(key: char, value: i8) -> Self {
        KeyValue { key, value }
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", content = "weights")]
pub enum DistributionType {
    Uniform,
    Tail([i32; 26]),
    Normal,
}

// Generate a uniformly distributed random character (A-Z)
fn generate_uniform_char(rng: &mut ThreadRng) -> char {
    rng.gen_range('a'..='z') // Random ASCII character from A to Z
}

// Generate a character based on Zipf's law approximation
fn generate_tail_char(rng: &mut ThreadRng, weights: &[i32; 26]) -> char {
    let dist = WeightedIndex::new(weights).unwrap();
    let idx = dist.sample(rng); // INFO: idx is a number between 0 and 25 (corresponding to a-z)

    // INFO: Convert the index to a character
    (b'a' + idx as u8) as char
}

// Generate a character based on Normal distribution
fn generate_normal_char(rng: &mut ThreadRng) -> char {
    let normal_dist = Normal::new(13.0, 5.0).unwrap(); // Mean at 'M' (13), std deviation of 5

    loop {
        let val: f64 = normal_dist.sample(rng);
        let val = val.round() as i32;
        if val >= 0 && val < 26 {
            // INFO: Convert the index to a character
            return (b'a' + val as u8) as char;
        }
    }
}

pub fn generate_random_kv(distribution: &DistributionType) -> KeyValue {
    let mut rng = rand::thread_rng();
    let value = rng.gen_range(0..=10);

    let key = match distribution {
        DistributionType::Uniform => generate_uniform_char(&mut rng),
        DistributionType::Tail(weights) => generate_tail_char(&mut rng, weights),
        DistributionType::Normal => generate_normal_char(&mut rng),
    };

    KeyValue::new(key, value)
}
