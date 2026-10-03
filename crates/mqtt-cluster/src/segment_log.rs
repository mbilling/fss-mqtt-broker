//! An append-only segment log (ADR 0078 T1).
//!
//! The durable store a replica needs is a *change log*, not an index:
//! `ReplicaState` holds every live entry in memory and reads its file once, at
//! open. So the file only has to record, in order, what changed — and the only
//! cost a commit should pay is the device flush. This module is that log:
//! payload-agnostic records (a kind byte and bytes) appended in batches, one
//! `write` and one `fdatasync` per batch, replayed in order at open.
//!
//! ## Format
//!
//! A log is a directory of **segments**, `seg-<first-lsn>.log`, each written
//! full of zeros and flushed before its first record, so its unwritten tail
//! reads as zeros. Zeros are WRITTEN, not reserved: a record then overwrites
//! blocks that are already allocated and inside the file size, so its
//! `fdatasync` flushes data alone. A sparse (`set_len`) or `fallocate`d
//! segment makes every flush also commit the filesystem journal — for the
//! block allocation or the unwritten-extent conversion — which measured (#693,
//! ext4) as 3 device writes per batch against 2, and 2–4× the flush tail.
//! Segments start at [`FIRST_SEGMENT_BYTES`] and double on each roll up to the
//! configured size: a small log costs a small file, and a node under a tight
//! per-file limit comes up before its first segment is anywhere near it. The
//! next segment is zeroed on a background thread once the active one is half
//! full, so a roll is a rename, not a 64 MiB write on the ack path. A segment
//! is a run of records:
//!
//! ```text
//! [len: u32 BE][crc32c: u32 BE][kind: u8][lsn: u64 BE][payload: len bytes]
//! ```
//!
//! `crc32c` covers `kind ++ lsn ++ payload`. `kind` 0 is reserved: a zero header
//! is the end of the written region. LSNs are consecutive across the whole log.
//!
//! ## Crash safety
//!
//! - **Persist before ack.** [`SegmentLog::append`] returns only after the
//!   batch is flushed; a caller acks after that, exactly as a redb
//!   `Durability::Immediate` commit.
//! - **Torn tail.** Replay stops at the first record that is not valid — bad
//!   CRC, an LSN out of sequence, a length past the segment. In the LAST
//!   segment that is a torn batch, never acked, and the log resumes there. In
//!   any earlier segment it is corruption and the open fails.
//! - **No ghosts.** A crash can persist a later part of an unacked batch and
//!   not its start. Records beyond the torn point can then be intact and, with
//!   equal record sizes, sit exactly where the next valid record would — an
//!   LSN check alone would replay them. Replayed, a ghost append could overwrite
//!   a newer entry. So recovery zeroes the rest of the segment from the valid
//!   end, and flushes that, before anything is written again.
//! - **Durable files.** A new segment is zeroed, flushed, named and its
//!   directory flushed, before any record is written into it: an acked batch
//!   can never live in a file whose own creation was not durable. A segment
//!   being prepared lives under a name the open ignores and removes.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Log sequence number: the position of a record in the whole log, from 1.
pub type Lsn = u64;

/// Record header: `len` (4) + `crc32c` (4) + `kind` (1) + `lsn` (8).
pub const HEADER_BYTES: usize = 17;

/// The largest payload one record may carry. A bound so a corrupt `len` can
/// never make replay allocate or seek absurdly.
pub const MAX_PAYLOAD: usize = 64 << 20;

/// The default segment size (ADR 0078 §1): the size segments grow to.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 << 20;

/// The size of a log's first segment; each roll doubles it, up to the
/// configured segment size.
pub const FIRST_SEGMENT_BYTES: u64 = 1 << 20;

/// The name a segment has while it is being zeroed, before a roll renames it.
const SPARE_NAME: &str = "seg-spare.tmp";

/// Zeros written per call when preparing a segment.
const ZERO_CHUNK: usize = 1 << 20;

/// Errors from the segment log.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// An I/O error, with what was being done.
    #[error("segment log {context}: {source}")]
    Io {
        /// What the log was doing.
        context: String,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// A record before the log's tail failed validation — real corruption, not
    /// a torn write.
    #[error("segment log corrupt in {segment}: {detail} at byte {offset}")]
    Corrupt {
        /// The segment file.
        segment: String,
        /// The byte offset of the bad record.
        offset: u64,
        /// What was wrong.
        detail: String,
    },
    /// Kind 0 is reserved for "unwritten".
    #[error("record kind 0 is reserved")]
    ReservedKind,
    /// A payload over [`MAX_PAYLOAD`].
    #[error("record payload of {0} bytes exceeds the {MAX_PAYLOAD}-byte limit")]
    TooLarge(usize),
}

fn io(context: impl Into<String>) -> impl FnOnce(std::io::Error) -> LogError {
    let context = context.into();
    move |source| LogError::Io { context, source }
}

/// CRC-32C (Castagnoli), table-driven — in-house so the log adds no dependency.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_update(0, data)
}

fn crc32c_update(crc: u32, data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in (0u32..).zip(t.iter_mut()) {
            let mut c = i;
            for _ in 0..8 {
                c = if c & 1 == 1 {
                    0x82F6_3B78 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    });
    let mut c = !crc;
    for b in data {
        c = table[((c ^ u32::from(*b)) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

fn record_crc(kind: u8, lsn: Lsn, payload: &[u8]) -> u32 {
    let mut head = [0u8; 9];
    head[0] = kind;
    head[1..].copy_from_slice(&lsn.to_be_bytes());
    crc32c_update(crc32c(&head), payload)
}

fn encode_into(buf: &mut Vec<u8>, kind: u8, lsn: Lsn, payload: &[u8]) {
    let len = u32::try_from(payload.len()).expect("payload bounded by MAX_PAYLOAD");
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&record_crc(kind, lsn, payload).to_be_bytes());
    buf.push(kind);
    buf.extend_from_slice(&lsn.to_be_bytes());
    buf.extend_from_slice(payload);
}

fn segment_name(first: Lsn) -> String {
    format!("seg-{first:020}.log")
}

fn parse_segment_name(name: &str) -> Option<Lsn> {
    name.strip_prefix("seg-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

fn sync_dir(dir: &Path) -> Result<(), LogError> {
    // A new or removed directory entry is durable only once its directory is
    // flushed. Unix exposes that; elsewhere the entry is as durable as the
    // filesystem makes it.
    #[cfg(unix)]
    File::open(dir)
        .and_then(|d| durable_sync(&d, true))
        .map_err(io(format!("syncing directory {}", dir.display())))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// One segment the log holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfo {
    /// The LSN of the segment's first record.
    pub first: Lsn,
    /// Bytes of valid records in it.
    pub bytes: u64,
    /// The file.
    pub path: PathBuf,
}

/// What [`SegmentLog::open`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Records replayed.
    pub records: u64,
    /// Bytes of a torn batch discarded at the tail (0 after a clean shutdown).
    pub torn_bytes: u64,
    /// Empty trailing segments removed (created, never written, before a crash).
    pub empty_segments_removed: usize,
}

/// An append-only segment log. One writer: not `Sync` by design — the caller
/// (the shard's writer task) owns it.
#[derive(Debug)]
pub struct SegmentLog {
    dir: PathBuf,
    segment_bytes: u64,
    /// Every segment, oldest first; the last one is active.
    segments: Vec<SegmentInfo>,
    active: File,
    /// The active segment's size: a batch that would pass it rolls.
    active_bytes: u64,
    next_lsn: Lsn,
    buf: Vec<u8>,
    /// The next segment, being zeroed on a background thread.
    spare: Option<std::thread::JoinHandle<Result<Spare, LogError>>>,
}

/// A zeroed, flushed segment waiting under [`SPARE_NAME`] for a roll.
#[derive(Debug)]
struct Spare {
    file: File,
    bytes: u64,
}

impl SegmentLog {
    /// Open (or create) the log in `dir`, replaying every valid record, in LSN
    /// order, into `visit(lsn, kind, payload)`.
    ///
    /// # Errors
    /// I/O failures, corruption before the tail, a segment sequence with a gap,
    /// or an error returned by `visit` (which aborts the open).
    pub fn open(
        dir: impl AsRef<Path>,
        segment_bytes: u64,
        mut visit: impl FnMut(Lsn, u8, &[u8]) -> Result<(), LogError>,
    ) -> Result<(Self, Recovery), LogError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(io(format!("creating {}", dir.display())))?;
        let segment_bytes = segment_bytes.max(HEADER_BYTES as u64 * 4);
        let first_bytes = FIRST_SEGMENT_BYTES.min(segment_bytes);
        // A segment a crash left half-zeroed: never named, so never written.
        match std::fs::remove_file(dir.join(SPARE_NAME)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(io(format!("removing {SPARE_NAME}"))(e));
            }
            _ => {}
        }
        let mut firsts: Vec<Lsn> = std::fs::read_dir(&dir)
            .map_err(io(format!("listing {}", dir.display())))?
            .filter_map(Result::ok)
            .filter_map(|e| e.file_name().to_str().and_then(parse_segment_name))
            .collect();
        firsts.sort_unstable();

        let mut recovery = Recovery::default();
        let mut segments: Vec<SegmentInfo> = Vec::with_capacity(firsts.len());
        let mut expected: Option<Lsn> = None;
        let count = firsts.len();
        for (i, first) in firsts.into_iter().enumerate() {
            let last = i + 1 == count;
            let path = dir.join(segment_name(first));
            if let Some(exp) = expected {
                if first != exp {
                    return Err(LogError::Corrupt {
                        segment: path.display().to_string(),
                        offset: 0,
                        detail: format!(
                            "segment starts at LSN {first}, the log continues at {exp}"
                        ),
                    });
                }
            }
            let mut bytes = Vec::new();
            File::open(&path)
                .and_then(|mut f| f.read_to_end(&mut bytes))
                .map_err(io(format!("reading {}", path.display())))?;
            let (valid, next, n, stop) = scan(&bytes, first, &mut visit)?;
            recovery.records += n;
            if let Some(detail) = stop {
                if !last {
                    return Err(LogError::Corrupt {
                        segment: path.display().to_string(),
                        offset: valid as u64,
                        detail,
                    });
                }
            }
            if last {
                // Anything non-zero past the valid end of the last segment is a
                // torn batch — whether the scan stopped on a bad record OR on a
                // zero header. The second is the sector-reorder shape: a crash
                // persisted a batch's later sectors and not its first, so the
                // valid end reads as a clean end of log while intact, unacked
                // records sit beyond it. Counting them makes the open zero them.
                let end = bytes[valid..]
                    .iter()
                    .rposition(|b| *b != 0)
                    .map_or(valid, |p| valid + p + 1);
                recovery.torn_bytes = (end - valid) as u64;
            }
            if last && n == 0 && i > 0 {
                // Created by a roll that crashed before its first flush: nothing
                // in it was ever acked. Drop it; the previous segment is active.
                std::fs::remove_file(&path).map_err(io(format!("removing {}", path.display())))?;
                sync_dir(&dir)?;
                recovery.empty_segments_removed += 1;
                break;
            }
            segments.push(SegmentInfo {
                first,
                bytes: valid as u64,
                path,
            });
            expected = Some(next);
        }

        let next_lsn = expected.unwrap_or(1);
        let (active, active_bytes) = if let Some(seg) = segments.last() {
            reopen_active(seg, recovery.torn_bytes, first_bytes)?
        } else {
            (create_segment(&dir, next_lsn, first_bytes)?, first_bytes)
        };
        if segments.is_empty() {
            segments.push(SegmentInfo {
                first: next_lsn,
                bytes: 0,
                path: dir.join(segment_name(next_lsn)),
            });
        }
        Ok((
            Self {
                dir,
                segment_bytes,
                segments,
                active,
                active_bytes,
                next_lsn,
                buf: Vec::new(),
                spare: None,
            },
            recovery,
        ))
    }

    /// The LSN the next appended record will get.
    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Every segment, oldest first; the last is the one being appended to.
    #[must_use]
    pub fn segments(&self) -> &[SegmentInfo] {
        &self.segments
    }

    /// The size segments grow to (the first is smaller; see
    /// [`FIRST_SEGMENT_BYTES`]).
    #[must_use]
    pub fn segment_bytes(&self) -> u64 {
        self.segment_bytes
    }

    /// Total bytes of valid records across every segment.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.bytes).sum()
    }

    /// Bytes `records` would take encoded.
    #[must_use]
    pub fn encoded_len(records: &[(u8, &[u8])]) -> u64 {
        records
            .iter()
            .map(|(_, p)| (HEADER_BYTES + p.len()) as u64)
            .sum()
    }

    /// Whether appending `bytes` more would start a new segment.
    #[must_use]
    pub fn would_roll(&self, bytes: u64) -> bool {
        let used = self.segments.last().map_or(0, |s| s.bytes);
        used > 0 && used + bytes > self.active_bytes
    }

    /// The size the next segment is created at: double the active one, up to
    /// the configured size.
    fn next_bytes(&self) -> u64 {
        self.active_bytes
            .saturating_mul(2)
            .min(self.segment_bytes)
            .max(FIRST_SEGMENT_BYTES.min(self.segment_bytes))
    }

    /// The next segment: the spare if one of the right size was prepared,
    /// else one zeroed here (which also surfaces a failed preparation's error).
    fn next_segment(&mut self, first: Lsn, bytes: u64) -> Result<File, LogError> {
        let spare = self.spare.take().map(std::thread::JoinHandle::join);
        let spare_path = self.dir.join(SPARE_NAME);
        if let Some(Ok(Err(e))) = &spare {
            // The inline zeroing below retries it and surfaces a real failure.
            tracing::warn!(error = %e, "preparing the next segment in the background failed");
        }
        if let Some(Ok(Ok(spare))) = spare {
            if spare.bytes == bytes {
                let path = self.dir.join(segment_name(first));
                std::fs::rename(&spare_path, &path)
                    .map_err(io(format!("naming {}", path.display())))?;
                sync_dir(&self.dir)?;
                return Ok(spare.file);
            }
        }
        let _ = std::fs::remove_file(&spare_path);
        create_segment(&self.dir, first, bytes)
    }

    /// Start zeroing the segment after the active one, if it is not already
    /// being zeroed.
    fn prepare_spare(&mut self) {
        if self.spare.is_some() {
            return;
        }
        let path = self.dir.join(SPARE_NAME);
        let bytes = self.next_bytes();
        // No thread (resource exhaustion) just means the roll zeroes inline.
        self.spare = std::thread::Builder::new()
            .name("segment-zero".into())
            .spawn(move || {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
                    .map_err(io(format!("creating {}", path.display())))?;
                zero_fill(&file, 0, bytes, true)
                    .and_then(|()| durable_sync(&file, true))
                    .map_err(io(format!("zeroing {}", path.display())))?;
                Ok(Spare { file, bytes })
            })
            .ok();
    }

    /// Start a new segment and write `head` as its first records, flushed.
    /// A caller that must re-state metadata at every segment head (ADR 0078
    /// T3) passes it here. Returns the new segment's first LSN.
    ///
    /// # Errors
    /// As [`append`](Self::append).
    pub fn roll(&mut self, head: &[(u8, &[u8])]) -> Result<Lsn, LogError> {
        let first = self.next_lsn;
        let bytes = self.next_bytes();
        self.active = self.next_segment(first, bytes)?;
        self.active_bytes = bytes;
        self.segments.push(SegmentInfo {
            first,
            bytes: 0,
            path: self.dir.join(segment_name(first)),
        });
        if !head.is_empty() {
            self.write_batch(head)?;
        }
        Ok(first)
    }

    /// Append `records` as one batch — one write, one flush — and return the
    /// LSN of the last. Nothing is acknowledged by the log before the flush;
    /// the caller acks after this returns. An empty batch is a no-op.
    ///
    /// Rolls to a new segment first when the batch would overflow the active
    /// one (with no head records — use [`would_roll`](Self::would_roll) and
    /// [`roll`](Self::roll) to control that).
    ///
    /// # Errors
    /// A reserved kind or oversized payload (nothing written), or an I/O
    /// failure — after which the batch must be treated as NOT durable.
    pub fn append(&mut self, records: &[(u8, &[u8])]) -> Result<Lsn, LogError> {
        if records.is_empty() {
            return Ok(self.next_lsn - 1);
        }
        for (kind, payload) in records {
            if *kind == 0 {
                return Err(LogError::ReservedKind);
            }
            if payload.len() > MAX_PAYLOAD {
                return Err(LogError::TooLarge(payload.len()));
            }
        }
        if self.would_roll(Self::encoded_len(records)) {
            self.roll(&[])?;
        }
        self.write_batch(records)
    }

    fn write_batch(&mut self, records: &[(u8, &[u8])]) -> Result<Lsn, LogError> {
        self.buf.clear();
        let mut lsn = self.next_lsn;
        for (kind, payload) in records {
            encode_into(&mut self.buf, *kind, lsn, payload);
            lsn += 1;
        }
        let seg = self
            .segments
            .last_mut()
            .expect("the log always has an active segment");
        write_all_at(&self.active, &self.buf, seg.bytes)
            .map_err(io(format!("writing {}", seg.path.display())))?;
        let synced = std::time::Instant::now();
        durable_sync(&self.active, false)
            .map_err(io(format!("flushing {}", seg.path.display())))?;
        crate::stage_timing::record(crate::stage_timing::Stage::Fsync, synced.elapsed());
        seg.bytes += self.buf.len() as u64;
        let half_full = seg.bytes.saturating_mul(2) >= self.active_bytes;
        self.next_lsn = lsn;
        if half_full {
            self.prepare_spare();
        }
        Ok(lsn - 1)
    }

    /// Delete every segment whose first LSN is below `first` — a PREFIX of the
    /// log, never the active segment (ADR 0078 T3 decides when that is safe).
    /// Returns how many were removed.
    ///
    /// # Errors
    /// I/O failures removing a file or flushing the directory.
    pub fn drop_before(&mut self, first: Lsn) -> Result<usize, LogError> {
        let keep_from = self
            .segments
            .iter()
            .position(|s| s.first >= first)
            .unwrap_or(self.segments.len())
            .min(self.segments.len() - 1);
        let doomed: Vec<SegmentInfo> = self.segments.drain(..keep_from).collect();
        for s in &doomed {
            std::fs::remove_file(&s.path).map_err(io(format!("removing {}", s.path.display())))?;
        }
        if !doomed.is_empty() {
            sync_dir(&self.dir)?;
        }
        Ok(doomed.len())
    }
}

/// Reopen the last segment to append to, and its size. Everything after the
/// valid end — ghosts included — is zeroed by writing zeros, so the blocks
/// stay allocated; a segment shorter than a first one (a crash while it was
/// being zeroed) is filled out. Both are flushed before any append. (A torn
/// count from an empty last segment the open removed lands on this one's zero
/// tail: harmless, and never past its end.)
fn reopen_active(seg: &SegmentInfo, torn: u64, first_bytes: u64) -> Result<(File, u64), LogError> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&seg.path)
        .map_err(io(format!("opening {}", seg.path.display())))?;
    let len = f.metadata().map_err(io("stat"))?.len();
    let torn_end = (seg.bytes + torn).min(len);
    let full = len.max(first_bytes);
    if torn_end > seg.bytes || full > len {
        zero_fill(&f, seg.bytes, torn_end.max(full), false)
            .map_err(io(format!("zeroing {}", seg.path.display())))?;
        durable_sync(&f, true).map_err(io("flushing a recovered segment"))?;
    }
    Ok((f, full))
}

fn create_segment(dir: &Path, first: Lsn, segment_bytes: u64) -> Result<File, LogError> {
    let path = dir.join(segment_name(first));
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(io(format!("creating {}", path.display())))?;
    zero_fill(&f, 0, segment_bytes, false).map_err(io(format!("zeroing {}", path.display())))?;
    durable_sync(&f, true).map_err(io(format!("flushing {}", path.display())))?;
    sync_dir(dir)?;
    Ok(f)
}

/// Write zeros over `[from, to)`: allocated, written blocks, unlike a hole.
/// `trickle` flushes each chunk as it goes — for zeroing beside a live
/// writer, whose next flush would otherwise queue behind the whole segment.
fn zero_fill(f: &File, from: u64, to: u64, trickle: bool) -> std::io::Result<()> {
    let zeros = vec![0u8; ZERO_CHUNK];
    let mut at = from;
    while at < to {
        let n = usize::try_from(to - at).map_or(ZERO_CHUNK, |left| left.min(ZERO_CHUNK));
        write_all_at(f, &zeros[..n], at)?;
        if trickle {
            durable_sync(f, false)?;
        }
        at += n as u64;
    }
    Ok(())
}

impl Drop for SegmentLog {
    fn drop(&mut self) {
        // No zeroing may outlive the log: a reopen removes the spare's name.
        if let Some(spare) = self.spare.take() {
            let _ = spare.join();
        }
    }
}

#[cfg(unix)]
/// Every data/metadata sync in the segment log goes through here, so the diagnostic
/// stand-in (`diag-simulated-fsync`, see `stage_timing`) covers all of them — a hot path
/// on a simulated barrier racing background syncs on the real device would measure the
/// device again. A normal build compiles this to the plain call.
fn durable_sync(f: &File, all: bool) -> std::io::Result<()> {
    if let Some(d) = crate::stage_timing::simulated_fsync() {
        std::thread::sleep(d);
        return Ok(());
    }
    if all {
        f.sync_all()
    } else {
        f.sync_data()
    }
}

fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.write_all_at(buf, offset)
}

#[cfg(not(unix))]
fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = f;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(buf)
}

/// Scan one segment's bytes from `first`. Returns the valid length, the next
/// LSN, the number of records visited, and — if the scan stopped on an invalid
/// record rather than at the zero tail — why.
#[allow(clippy::type_complexity)]
fn scan(
    bytes: &[u8],
    first: Lsn,
    visit: &mut impl FnMut(Lsn, u8, &[u8]) -> Result<(), LogError>,
) -> Result<(usize, Lsn, u64, Option<String>), LogError> {
    let mut pos = 0usize;
    let mut lsn = first;
    let mut n = 0u64;
    loop {
        let rest = &bytes[pos..];
        if rest.len() < HEADER_BYTES || rest[..HEADER_BYTES].iter().all(|b| *b == 0) {
            return Ok((pos, lsn, n, None));
        }
        let len = u32::from_be_bytes(rest[0..4].try_into().expect("4")) as usize;
        let crc = u32::from_be_bytes(rest[4..8].try_into().expect("4"));
        let kind = rest[8];
        let rec_lsn = Lsn::from_be_bytes(rest[9..17].try_into().expect("8"));
        let why = if kind == 0 {
            Some("a reserved kind".to_string())
        } else if len > MAX_PAYLOAD || HEADER_BYTES + len > rest.len() {
            Some(format!("a length of {len} past the segment"))
        } else if rec_lsn != lsn {
            Some(format!("LSN {rec_lsn} where {lsn} was due"))
        } else if record_crc(kind, rec_lsn, &rest[HEADER_BYTES..HEADER_BYTES + len]) != crc {
            Some("a checksum mismatch".to_string())
        } else {
            None
        };
        if let Some(why) = why {
            return Ok((pos, lsn, n, Some(why)));
        }
        visit(lsn, kind, &rest[HEADER_BYTES..HEADER_BYTES + len])?;
        pos += HEADER_BYTES + len;
        lsn += 1;
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};

    type Rec = (Lsn, u8, Vec<u8>);

    fn open(dir: &Path, seg: u64) -> (SegmentLog, Recovery, Vec<Rec>) {
        let mut seen = Vec::new();
        let (log, rec) = SegmentLog::open(dir, seg, |l, k, p| {
            seen.push((l, k, p.to_vec()));
            Ok(())
        })
        .unwrap();
        (log, rec, seen)
    }

    fn payload(i: u64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|j| (i.wrapping_mul(31).wrapping_add(j as u64) & 0xff) as u8 | 1)
            .collect()
    }

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn batches_replay_in_order_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(
            (rec, seen.len(), log.next_lsn()),
            (Recovery::default(), 0, 1)
        );
        let a = payload(1, 40);
        let b = payload(2, 0); // an empty payload is a valid record
        assert_eq!(log.append(&[(1, &a), (2, &b)]).unwrap(), 2);
        assert_eq!(log.append(&[(3, &a)]).unwrap(), 3);
        assert_eq!(log.append(&[]).unwrap(), 3, "an empty batch is a no-op");
        drop(log);
        let (log, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(rec.records, 3);
        assert_eq!(rec.torn_bytes, 0);
        assert_eq!(seen, vec![(1, 1, a.clone()), (2, 2, b), (3, 3, a)]);
        assert_eq!(log.next_lsn(), 4);
    }

    #[test]
    fn a_log_rolls_segments_and_replays_across_them_with_heads() {
        let dir = tempfile::tempdir().unwrap();
        let seg = 256;
        let (mut log, _, _) = open(dir.path(), seg);
        let mut want = Vec::new();
        for i in 0..20u64 {
            let p = payload(i, 30);
            if log.would_roll(SegmentLog::encoded_len(&[(1, &p)])) {
                let head = b"head".to_vec();
                let first = log.roll(&[(9, &head)]).unwrap();
                want.push((first, 9u8, head));
            }
            let l = log.append(&[(1, &p)]).unwrap();
            want.push((l, 1, p));
        }
        assert!(
            log.segments().len() > 3,
            "a 256 B segment holds a few 47 B records"
        );
        drop(log);
        let (log, _, seen) = open(dir.path(), seg);
        assert_eq!(seen, want);
        assert_eq!(log.next_lsn(), want.last().unwrap().0 + 1);
    }

    #[test]
    fn a_batch_larger_than_a_segment_still_lands_whole() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), 256);
        let big = payload(7, 1000);
        log.append(&[(1, b"small")]).unwrap();
        log.append(&[(1, &big)]).unwrap();
        drop(log);
        let (_, _, seen) = open(dir.path(), 256);
        assert_eq!(seen[1], (2, 1, big));
    }

    /// Cut the last batch mid-write, as a crash would: the log resumes at the
    /// last whole record, and what was torn is gone, not half-replayed.
    #[test]
    fn a_torn_tail_is_discarded_and_the_log_resumes_there() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        log.append(&[(1, &payload(1, 50))]).unwrap();
        let end = log.segments()[0].bytes;
        log.append(&[(1, &payload(2, 50)), (1, &payload(3, 50))])
            .unwrap();
        let path = log.segments()[0].path.clone();
        drop(log);
        // The second batch's first record loses its last byte.
        corrupt(&path, end + HEADER_BYTES as u64 + 49);
        let (mut log, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(seen.len(), 1);
        assert!(rec.torn_bytes > 0);
        assert_eq!(log.next_lsn(), 2);
        log.append(&[(1, &payload(4, 50))]).unwrap();
        drop(log);
        let (_, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(rec.torn_bytes, 0, "the recovered log is clean");
        assert_eq!(seen.iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(seen[1].2, payload(4, 50));
    }

    /// The case an LSN check cannot catch: equal-sized records, the FIRST of a
    /// torn batch lost, the rest intact on disk. After recovery appends fewer
    /// records, the old intact ones sit exactly where the next record would be,
    /// with exactly the next LSN. They were never acked and must never replay.
    #[test]
    fn intact_records_after_a_torn_one_never_come_back() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        log.append(&[(1, &payload(1, 64))]).unwrap();
        let end = log.segments()[0].bytes;
        let ghosts: Vec<Vec<u8>> = (10..14).map(|i| payload(i, 64)).collect();
        let refs: Vec<(u8, &[u8])> = ghosts.iter().map(|g| (1u8, g.as_slice())).collect();
        log.append(&refs).unwrap(); // LSN 2..=5
        let path = log.segments()[0].path.clone();
        drop(log);
        corrupt(&path, end + 5); // record 2's checksum: torn
        let (mut log, _, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(seen.len(), 1);
        log.append(&[(1, &payload(20, 64))]).unwrap(); // the new LSN 2
        drop(log);
        let (_, _, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(
            seen.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![1, 2],
            "old records 3..=5 would have lined up and replayed without the zeroing"
        );
        assert_eq!(seen[1].2, payload(20, 64));
    }

    /// The sector-reorder shape: a crash persisted a batch's LATER records and
    /// not its first, which reads as all zeros. The valid end then looks like a
    /// clean end of log — no bad record to stop on — while intact, unacked
    /// records sit past a zero gap, on exactly the next LSNs once a shorter
    /// append follows. They must be zeroed all the same.
    #[test]
    fn records_past_a_zeroed_first_record_never_come_back() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        log.append(&[(1, &payload(1, 64))]).unwrap();
        let end = log.segments()[0].bytes;
        let ghosts: Vec<Vec<u8>> = (10..14).map(|i| payload(i, 64)).collect();
        let refs: Vec<(u8, &[u8])> = ghosts.iter().map(|g| (1u8, g.as_slice())).collect();
        log.append(&refs).unwrap(); // LSN 2..=5
        let path = log.segments()[0].path.clone();
        drop(log);
        zero(&path, end, (HEADER_BYTES + 64) as u64); // record 2 never reached the disk
        let (mut log, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(seen.len(), 1);
        assert!(
            rec.torn_bytes > 0,
            "the records past the zero gap are torn, not absent"
        );
        log.append(&[(1, &payload(20, 64))]).unwrap(); // the new LSN 2
        drop(log);
        let (_, _, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(seen.iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(seen[1].2, payload(20, 64));
    }

    #[test]
    fn corruption_before_the_tail_fails_the_open() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), 256);
        for i in 0..12 {
            log.append(&[(1, &payload(i, 40))]).unwrap();
        }
        assert!(log.segments().len() >= 3);
        let first = log.segments()[0].path.clone();
        drop(log);
        corrupt(&first, HEADER_BYTES as u64 + 3);
        let err = SegmentLog::open(dir.path(), 256, |_, _, _| Ok(())).unwrap_err();
        assert!(matches!(err, LogError::Corrupt { .. }), "{err}");
    }

    #[test]
    fn an_empty_segment_from_a_crashed_roll_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        log.append(&[(1, b"kept")]).unwrap();
        drop(log);
        // A roll that created and extended the next file, then crashed.
        let stray = dir.path().join(segment_name(2));
        File::create(&stray).unwrap().set_len(4096).unwrap();
        let (mut log, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!(rec.empty_segments_removed, 1);
        assert_eq!(seen.len(), 1);
        assert!(!stray.exists());
        assert_eq!(log.append(&[(1, b"next")]).unwrap(), 2);
    }

    #[test]
    fn a_gap_between_segments_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), 256);
        for i in 0..12 {
            log.append(&[(1, &payload(i, 40))]).unwrap();
        }
        let middle = log.segments()[1].path.clone();
        drop(log);
        std::fs::remove_file(middle).unwrap();
        assert!(matches!(
            SegmentLog::open(dir.path(), 256, |_, _, _| Ok(())).unwrap_err(),
            LogError::Corrupt { .. }
        ));
    }

    #[test]
    fn dropping_a_prefix_keeps_the_rest_replayable() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), 256);
        for i in 0..20 {
            log.append(&[(1, &payload(i, 40))]).unwrap();
        }
        let segs = log.segments().to_vec();
        assert!(segs.len() >= 4);
        let cut = segs[2].first;
        assert_eq!(log.drop_before(cut).unwrap(), 2);
        assert_eq!(
            log.drop_before(Lsn::MAX).unwrap(),
            segs.len() - 3,
            "never the active one"
        );
        let active_first = log.segments()[0].first;
        let next = log.next_lsn();
        drop(log);
        let (log, _, seen) = open(dir.path(), 256);
        assert_eq!(seen.first().map(|r| r.0), Some(active_first));
        assert_eq!(log.next_lsn(), next);
    }

    #[test]
    fn reserved_kinds_and_oversized_payloads_are_refused_unwritten() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert!(matches!(
            log.append(&[(0, b"x")]),
            Err(LogError::ReservedKind)
        ));
        let huge = vec![1u8; MAX_PAYLOAD + 1];
        assert!(matches!(
            log.append(&[(1, &huge)]),
            Err(LogError::TooLarge(_))
        ));
        assert_eq!(log.next_lsn(), 1);
    }

    /// Bytes of `path` backed by allocated blocks — below its length when the
    /// file has holes (a `set_len` segment), not when it was zeroed.
    #[cfg(unix)]
    fn allocated(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).unwrap().blocks() * 512
    }

    fn len(path: &Path) -> u64 {
        std::fs::metadata(path).unwrap().len()
    }

    /// #693: a segment is written full of zeros, not left a hole, so an append
    /// overwrites allocated blocks and its flush commits no allocation. A torn
    /// tail is zeroed the same way: recovery must not punch the hole back.
    #[cfg(unix)]
    #[test]
    fn segments_are_zeroed_not_sparse_even_after_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        let path = log.segments()[0].path.clone();
        assert_eq!(len(&path), FIRST_SEGMENT_BYTES);
        assert!(
            allocated(&path) >= FIRST_SEGMENT_BYTES,
            "a hole at creation"
        );
        log.append(&[(1, &payload(1, 50))]).unwrap();
        let end = log.segments()[0].bytes;
        log.append(&[(1, &payload(2, 50))]).unwrap();
        drop(log);
        corrupt(&path, end + 5);
        let (_, rec, _) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert!(rec.torn_bytes > 0);
        assert_eq!(len(&path), FIRST_SEGMENT_BYTES);
        assert!(
            allocated(&path) >= FIRST_SEGMENT_BYTES,
            "a hole after recovery"
        );
    }

    /// Segments start small and double on each roll up to the configured size;
    /// every one is prepared ahead and named at the roll, never left behind.
    #[test]
    fn segments_double_from_the_first_size_up_to_the_configured_one() {
        let dir = tempfile::tempdir().unwrap();
        let cap = 4 * FIRST_SEGMENT_BYTES;
        let (mut log, _, _) = open(dir.path(), cap);
        let p = payload(1, 64 << 10);
        let mut want = Vec::new();
        while log.segments().len() < 5 {
            want.push(log.append(&[(1, &p)]).unwrap());
        }
        let sizes: Vec<u64> = log.segments().iter().map(|s| len(&s.path)).collect();
        let mib = FIRST_SEGMENT_BYTES;
        assert_eq!(sizes, vec![mib, 2 * mib, 4 * mib, 4 * mib, 4 * mib]);
        drop(log);
        assert!(
            !dir.path().join(SPARE_NAME).exists() || len(&dir.path().join(SPARE_NAME)) == cap,
            "a spare is only ever the next segment's size"
        );
        let (log, _, seen) = open(dir.path(), cap);
        assert_eq!(seen.iter().map(|r| r.0).collect::<Vec<_>>(), want);
        assert!(!dir.path().join(SPARE_NAME).exists(), "the open removes it");
        assert_eq!(
            len(&log.segments()[4].path),
            cap,
            "reopened at its own size"
        );
    }

    /// A crash while the first segment was being zeroed leaves it short and
    /// empty; the open fills it out rather than appending into a stub.
    #[test]
    fn a_first_segment_cut_short_while_zeroing_is_filled_out() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join(segment_name(1));
        File::create(&stub).unwrap().set_len(4096).unwrap();
        std::fs::write(dir.path().join(SPARE_NAME), b"half").unwrap();
        let (mut log, rec, seen) = open(dir.path(), DEFAULT_SEGMENT_BYTES);
        assert_eq!((rec.records, seen.len()), (0, 0));
        assert_eq!(len(&stub), FIRST_SEGMENT_BYTES);
        assert!(!dir.path().join(SPARE_NAME).exists());
        assert_eq!(log.append(&[(1, b"first")]).unwrap(), 1);
    }

    fn zero(path: &Path, at: u64, len: u64) {
        use std::io::Write;
        let mut f = OpenOptions::new().write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(&vec![0u8; usize::try_from(len).unwrap()])
            .unwrap();
    }

    fn corrupt(path: &Path, at: u64) {
        use std::io::Write;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        let mut b = [0u8; 1];
        f.read_exact(&mut b).unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(&[b[0] ^ 0xff]).unwrap();
    }
}
