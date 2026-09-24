//! Optional perf-stat FIFO handshake. Counters stay disabled during warm-up.
//! Small nonblocking reads with a fixed failure bound: a dead sampler cannot
//! strand the benchmark or silently produce an allegedly valid zero counter.
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::time::{Duration, Instant};

pub struct PerfControl {
    control: File,
    ack: File,
}

impl PerfControl {
    pub fn from_env() -> Option<Self> {
        let control = std::env::var_os("MEMBERSHIP_PERF_CONTROL");
        let ack = std::env::var_os("MEMBERSHIP_PERF_ACK");
        match (control, ack) {
            (None, None) => None,
            (Some(control), Some(ack)) => {
                let flags = i32::try_from(rustix::fs::OFlags::NONBLOCK.bits()).unwrap();
                Some(Self {
                    control: OpenOptions::new()
                        .write(true)
                        .custom_flags(flags)
                        .open(control)
                        .expect("perf control FIFO must already have a reader"),
                    ack: OpenOptions::new()
                        .read(true)
                        .custom_flags(flags)
                        .open(ack)
                        .expect("perf acknowledgement FIFO"),
                })
            }
            _ => panic!("both MEMBERSHIP_PERF_CONTROL and MEMBERSHIP_PERF_ACK are required"),
        }
    }

    pub fn command(&mut self, command: &[u8]) {
        assert!(command == b"enable\n" || command == b"disable\n");
        self.control
            .write_all(command)
            .expect("perf control command failed");
        expect_ack(&mut self.ack);
    }
}

pub fn expect_ack(reader: &mut impl Read) {
    let deadline = Instant::now() + Duration::from_secs(5);
    // perf's control protocol writes the terminating NUL as well as the newline.
    // Leaving it unread would desynchronize the next (disable) acknowledgement.
    let mut ack = [0; 5];
    let mut used = 0;
    while used < ack.len() {
        assert!(
            Instant::now() < deadline,
            "perf did not acknowledge within five seconds"
        );
        match reader.read(&mut ack[used..]) {
            Ok(0) => panic!("perf acknowledgement channel closed"),
            Ok(n) => used += n,
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                // Bounded polling of an external sampler, outside measured work.
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => panic!("perf acknowledgement failed: {e}"),
        }
    }
    assert_eq!(&ack, b"ack\n\0", "unexpected perf acknowledgement");
}
