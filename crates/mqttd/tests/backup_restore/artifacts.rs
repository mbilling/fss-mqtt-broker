//! Keep the full evidence from a failed restore, not just the last few log lines.
use std::path::Path;

pub(super) struct Artifacts(Option<tempfile::TempDir>);

impl Artifacts {
    pub(super) fn new() -> Self {
        Self(Some(tempfile::tempdir().expect("restore test directory")))
    }

    pub(super) fn path(&self) -> &Path {
        self.0.as_ref().expect("live test directory").path()
    }
}

impl Drop for Artifacts {
    fn drop(&mut self) {
        if std::thread::panicking() || std::env::var_os("MQTTD_TEST_KEEP_RESTORE").is_some() {
            let path = self.0.take().expect("live test directory").keep();
            eprintln!("restore test artifacts retained at {}", path.display());
        }
    }
}
