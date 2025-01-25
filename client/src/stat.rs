use std::{
    collections::HashMap,
    fmt,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub struct Timer {
    start_time: Instant,
    end_time: Option<Instant>,
}

impl Timer {
    fn start(start_time: Instant) -> Self {
        Timer {
            start_time,
            end_time: None,
        }
    }

    fn stop(&mut self) -> Result<(), String> {
        if self.end_time.is_some() {
            return Err("Already received a response".to_string());
        }

        self.end_time = Some(Instant::now());
        Ok(())
    }

    fn get_latency(&self) -> Result<Duration, &str> {
        match self.end_time {
            Some(end_time) => Ok(end_time.duration_since(self.start_time)),
            None => Err("Can't calculate the duration since the request was not completed"),
        }
    }
}

pub struct Stats {
    data: HashMap<Uuid, Timer>,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Stats {{")?;
        for (uuid, timer) in &self.data {
            write!(f, "\n  id({}): latency({})", uuid, timer)?;
        }
        write!(f, "\n}}\n")?;
        match self.get_average_latency_as_micros() {
            Ok(avg) => write!(f, "\n Average latency in micro seconds: {}", avg),
            Err(_) => write!(f, "\nNo request completed unable to calculate avg latency"),
        }
    }
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} micro seconds",
            self.get_latency()
                .unwrap_or(Duration::from_millis(0))
                .as_micros()
        )
    }
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            data: HashMap::new(),
        }
    }

    pub fn send_request(&mut self, id: Uuid) {
        let timer = Timer::start(Instant::now());
        self.data.insert(id, timer);
    }

    pub fn receive_request(&mut self, id: &Uuid) {
        self.data
            .get_mut(id)
            .expect("Request was sent")
            .stop()
            .unwrap();
    }

    pub fn write_to_file(&self, pid: u64) -> std::io::Result<()> {
        std::fs::write(format!("/volumes/client_{}.txt", pid), self.to_string())?;
        Ok(())
    }

    pub fn get_average_latency_as_micros(&self) -> Result<u128, String> {
        let summed_latency = self
            .data
            .iter()
            .map(|kv| kv.1)
            .filter(|&timer| timer.end_time.is_some())
            .fold(Duration::new(0, 0), |acc, e| acc + e.get_latency().unwrap());
        let number_of_completed_requests = match self
            .data
            .iter()
            .map(|kv| kv.1)
            .filter(|&timer| timer.end_time.is_some())
            .count()
        {
            0 => return Err("No request was completed".to_string()),
            count => count as f64,
        };

        Ok(summed_latency
            .div_f64(number_of_completed_requests)
            .as_micros())
    }
}
