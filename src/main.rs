use std::{
    path::PathBuf,
    fmt::{self, Formatter, Display},
    fs::{self, File},
    collections::{hash_map, HashMap},
    io::{self, Read, Seek, SeekFrom, BufReader, BufWriter},
    time
};

use structopt::StructOpt;
use rand::{
    prelude::*,
    distributions::Alphanumeric
};
use serde::{Serializer, ser::SerializeSeq};
use either::Either;
use failure::{self, format_err, bail, Fallible};
use log::{warn, debug};

use fsoperation::{FSOperation, SeekMode, SeekPos, NUM_FS_OPERATIONS};

#[derive(Debug, StructOpt)]
struct GrindArguments {
    #[structopt(short = "l", long, parse(from_os_str))]
    /// Specifies the path the log file shall be written to. If not specified, output to stdout.
    log_output: Option<PathBuf>,
    #[structopt(parse(from_os_str))]
    /// Path to the file system that shall be assessed.
    test_path: PathBuf,
    #[structopt(parse(from_os_str))]
    /// Path that shall serve as the reference file system (i.e. a mature file system that can
    /// be trusted).
    reference_path: PathBuf,
    #[structopt(short = "t", long, required_unless = "rounds")]
    /// Time in seconds the grinding shall be executed.
    ///
    /// At least one of 'timeout' or 'rounds' must be specified.
    timeout: Option<usize>,
    #[structopt(short = "r", long, required_unless = "timeout")]
    /// Number of operations the grinding shall execute.
    ///
    /// At least one of 'timeout' or 'rounds' must be specified.
    rounds: Option<usize>,
    #[structopt(long)]
    /// Maximum number of open files at the same time.
    ///
    /// If this number is reached, no additional files will be opened. If files are closed
    /// again, opening will also be resumed until the maximum number is reached again.
    max_open_files: Option<usize>,
    #[structopt(long)]
    /// Maximum number of files that are to be created during the grind oepration.
    ///
    /// If this number is reached, no new files will be created for the duration of the
    /// assessment.
    max_created_files: Option<usize>,
    #[structopt(long, default_value = "1024")]
    /// Maximum size of one write.
    ///
    /// fsgrinder will randomly write between 1 and max_write_size bytes per write operation.
    max_write_size: usize,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "fsgrinder")]
/// Randomly executes file system operations on two paths and compares whether the files and
/// content are the same.
///
/// At certain points during the execution (e.g. when reading from files), the file systems are
/// checked for consistency. If they're inconsistent, the evaluation ends and it's possible to
/// investigate both trees for reasons for the inconsistency.
///
/// The tool supports random grinding as well as reprocessing a recorded log from previous random
/// grinds. See the subcommand documentation for further details.
enum Arguments {
    /// Randomly execute commands from a set of supported file operations on two file systems.
    ///
    /// The log can optionally be recorded to act as an input for later replay runs.
    Grind(GrindArguments),

    /// Replay a previously recorded (or hand crafted) file system action log.
    ///
    /// Replay ends when the first inconsistency or operation failure is detected.
    Replay {
        #[structopt(parse(from_os_str))]
        /// Log to replay on the given file systems.
        log_path: PathBuf,
        #[structopt(parse(from_os_str))]
        /// Path to the file system that shall be assessed.
        test_path: PathBuf,
        #[structopt(parse(from_os_str))]
        /// Path that shall serve as the reference file system (i.e. a mature file system that can
        /// be trusted).
        reference_path: PathBuf
    }
}

enum GrindDurationKeeper {
    Timeout {
        start: time::Instant,
        timeout: time::Duration
    },
    Rounds  {
        elapsed: usize,
        max: usize
    }
}

impl GrindDurationKeeper {
    fn with_rounds(rounds: usize) -> Self {
        GrindDurationKeeper::Rounds {
            max: rounds,
            elapsed: 0
        }
    }

    fn with_timeout(timeout: usize) -> Self {
        GrindDurationKeeper::Timeout {
            start: time::Instant::now(),
            timeout: time::Duration::from_secs(timeout as u64)
        }
    }

    fn elapsed(&mut self) -> bool {
        match self {
            GrindDurationKeeper::Timeout { timeout, start } =>
                time::Instant::now() - *start > *timeout,
            GrindDurationKeeper::Rounds { elapsed, max } => {
                *elapsed += 1;
                max < elapsed
            }
        }
    }
}

struct RandomReader<R: Rng>(R);

impl<R: Rng> Read for RandomReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.fill_bytes(buf);
        Ok(buf.len())
    }
}

#[derive(Debug, Clone)]
struct FileDescriptor {
    path: PathBuf
}

#[derive(Debug, Clone)]
struct OpenFileDescriptor {
    file_desc: FileDescriptor
}

struct RandFSOpGenerator<R: Rng> {
    rng: R,
    max_open_files: Option<usize>,
    max_created_files: Option<usize>,
    open_files: Vec<OpenFileDescriptor>,
    existing_files: Vec<FileDescriptor>,
    max_write_size: usize,
}

trait RemoveRandomEntry {
    type Item;
    fn remove_random<R: Rng>(&mut self, rng: &mut R) -> Option<Self::Item>;
}

impl<T> RemoveRandomEntry for Vec<T> {
    type Item = T;

    fn remove_random<R: Rng>(&mut self, rng: &mut R) -> Option<Self::Item> {
        if self.len() > 0 {
            let random_index = rng.gen_range(0, self.len());
            Some(self.remove(random_index))
        } else {
            None
        }
    }
}

impl<R: Rng> Iterator for RandFSOpGenerator<R> {
    type Item = FSOperation;

    fn next(&mut self) -> Option<Self::Item> {
        let operation = loop {
            let operation_index = self.rng.gen_range(0, NUM_FS_OPERATIONS);
            match operation_index {
                0 => if let Some(OpenFileDescriptor { file_desc: closed_file }) =
                self.open_files.remove_random(&mut self.rng) {
                    let result = FSOperation::CloseFile { file_path: closed_file.path.clone() };
                    self.existing_files.push(closed_file);
                    break result;
                },
                1 => if !self.max_created_files_reached() {
                    let name_len = self.rng.gen_range(6, 32);
                    let name = (&mut self.rng).sample_iter(Alphanumeric).take(name_len).collect::<String>();
                    let path = PathBuf::from(name);
                    self.open_files.push(OpenFileDescriptor {
                        file_desc: FileDescriptor { path: path.clone(), }
                    });

                    break FSOperation::CreateFile { file_path: path };
                },
                2 => if !self.max_open_files_reached() {
                    if let Some(open_file) = self.existing_files.remove_random(&mut self.rng) {
                        let result = FSOperation::OpenFile { file_path: open_file.path.clone() };
                        self.open_files.push(OpenFileDescriptor {
                            file_desc: open_file
                        });
                        break result;
                    }
                },
                3 => if let Some(file) = self.open_files.choose_mut(&mut self.rng) {
                    let seek_mode = match self.rng.gen_range(0, 3) {
                        0 => SeekMode::Start,
                        1 => SeekMode::Current,
                        2 => SeekMode::End,
                        _ => unreachable!()
                    };
                    let rel_seek_pos = self.rng.gen_range(0u32, 2 * 1048576) as f32 / 1048576.0;
                    break FSOperation::SeekOpenFile {
                        file_path: file.file_desc.path.clone(),
                        seek_pos: SeekPos { seek_mode, rel_seek_pos } };
                },
                4 => if let Some(file) = self.open_files.choose_mut(&mut self.rng) {
                    let written_size = self.rng.gen_range(1, self.max_write_size) as u64;
                    break FSOperation::WriteRandomData {
                        file_path: file.file_desc.path.clone(),
                        written_size,
                    };
                },
                5 => if let Some(file) = self.open_files.choose_mut(&mut self.rng) {
                    let read_size = self.rng.gen_range(1, 2048);
                    break FSOperation::ReadData {
                        read_size,
                        file_path: file.file_desc.path.clone()
                    };
                }
                _ => panic!("NUM_FS_OPERATIONS larger than number of FSOperarion variants")
            }
        };

        Some(operation)
    }
}

impl<R: Rng> RandFSOpGenerator<R> {
    fn new(rng: R, max_write_size: usize, max_open_files: Option<usize>,
           max_created_files: Option<usize>) -> Self {
        Self {
            rng,
            max_open_files,
            max_created_files,
            max_write_size,
            open_files: Default::default(),
            existing_files: Default::default()
        }
    }

    fn max_open_files_reached(&self) -> bool {
        self.max_open_files
            .map(|of| self.open_files.len() >= of)
            .unwrap_or(false)
    }

    fn max_created_files_reached(&self) -> bool {
        self.max_created_files
            .map(|cf| {
                debug!("open_files.len: {}, existing_files.len: {}", self.open_files.len(), self.existing_files.len());
                self.open_files.len() + self.existing_files.len() >= cf
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum FSOperationResult {
    Done,
    Position(u64),
    Data(Vec<u8>)
}

impl Display for FSOperationResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            FSOperationResult::Done => write!(f, "Finished without result data"),
            FSOperationResult::Data(data) =>
                write!(f, "Finished with {} bytes of raw data", data.len()),
            FSOperationResult::Position(pos) =>
                write!(f, "Finished at position {}", pos),
        }
    }
}

trait FSOperationExecutor {
    type Error: Display;

    fn execute_fs_operation(&mut self, operation: FSOperation) -> Result<FSOperationResult, Self::Error>;
}

struct FSOperationComparator<R, A> {
    reference_fs: R,
    assessed_fs: A
}

impl<R, A> FSOperationComparator<R, A> {
    fn new(reference_fs: R, assessed_fs: A) -> Self {
        Self {
            reference_fs,
            assessed_fs
        }
    }
}

impl<R, A> FSOperationExecutor for FSOperationComparator<R, A>
    where A: FSOperationExecutor,
          R: FSOperationExecutor,
          R::Error: From<A::Error>+From<failure::Error> {
    type Error = R::Error;

    fn execute_fs_operation(&mut self, operation: FSOperation) -> Result<FSOperationResult, Self::Error> {
        self.reference_fs.execute_fs_operation(operation.clone())
            .and_then(|reference_result| {
                let assessed_result = self.assessed_fs.execute_fs_operation(operation);
                if let Ok(ref assessed_op_result) = assessed_result {
                    if *assessed_op_result != reference_result {
                        return Err(format_err!(
r"Reference FS has different behavior than assessed FS.

Result of reference FS
======================

{}

Result of assessed FS
=====================

{}", &reference_result, assessed_op_result).into());
                    }
                }

                assessed_result.map_err(Into::into)
            })
    }
}

struct GrinderFile {
    file: File,
}

struct FSGrinder<R> {
    base_path: PathBuf,
    open_files: HashMap<PathBuf, GrinderFile>,
    rng: R
}

impl<R: Rng> FSOperationExecutor for FSGrinder<R> {
    type Error = failure::Error;

    fn execute_fs_operation(&mut self, operation: FSOperation) -> Fallible<FSOperationResult> {
        match operation {
            FSOperation::SeekOpenFile { seek_pos, ref file_path } => {
                let file = self.open_files.get_mut(file_path)
                    .ok_or(format_err!("Unable to find open file {}", file_path.display()))?;
                let file_pos = file.file.seek(SeekFrom::Current(0)).unwrap() as u64;
                let file_size = file.file.seek(SeekFrom::End(0)).unwrap() as u64;
                // restore previous position
                file.file.seek(SeekFrom::Start(file_pos)).unwrap();
                let seek_from = seek_pos.into_seek_from(file_pos, file_size);
                debug!("Converted seek_pos for {} from {}/{}: {:?}",
                       file_path.display(), file_pos, file_size, &seek_from);
                file.file.seek(seek_from).unwrap();
                let new_file_pos = file.file.seek(SeekFrom::Current(0)).unwrap() as u64;
                Ok(FSOperationResult::Position(new_file_pos))
            }

            FSOperation::CloseFile { ref file_path } =>
                self.open_files.remove(file_path)
                    .ok_or(format_err!("File {} wasn't open", file_path.display()))
                    .map(|_| FSOperationResult::Done),

            FSOperation::CreateFile { file_path } => self.open_file(file_path, true),

            FSOperation::OpenFile { file_path } => self.open_file(file_path, false),

            FSOperation::WriteRandomData { ref file_path, written_size } => {
                let file = self.open_files.get_mut(file_path).ok_or(
                    format_err!("Unable to find open file {} for writing", file_path.display()))?;
                let mut reader = RandomReader(&mut self.rng).take(written_size);
                io::copy(&mut reader, &mut file.file)
                    .map(|_| FSOperationResult::Done)
                    .map_err(|e| format_err!("Failed to write {} bytes to {}: {}", written_size, file_path.display(), e))
            }

            FSOperation::ReadData { read_size, ref file_path } => {
                let file = self.open_files.get_mut(file_path).ok_or(
                    format_err!("Unable to find open finle {} for reading", file_path.display()))?;
                let file_pos = file.file.seek(SeekFrom::Current(0)).unwrap();
                let file_size = file.file.seek(SeekFrom::End(0)).unwrap();
                // restore previous position
                file.file.seek(SeekFrom::Start(file_pos)).unwrap();
                let data_size = if file_size > file_pos {
                    read_size.min(file_size - file_pos) as usize
                } else {
                    0usize
                };
                debug!("Read {} bytes from {} at {}/{}", data_size, file_path.display(), file_pos, file_size);
                let mut data = Vec::with_capacity(read_size as usize);
                data.resize(data_size, 0);
                file.file.read_exact(data.as_mut())
                    .map(move |_| FSOperationResult::Data(data))
                    .map_err(|e| format_err!("Unable to read {} bytes from {}: {}", read_size, file_path.display(), e))
            }
        }
    }
}

impl<R: Rng> FSGrinder<R> {
    fn new<P: Into<PathBuf>>(base_path: P, rng: R) -> Self {
        Self {
            open_files: HashMap::new(),
            rng,
            base_path: base_path.into()
        }
    }

    fn open_file(&mut self, file_path: PathBuf, create: bool) -> Fallible<FSOperationResult> {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(self.base_path.join(&file_path))
            .map_err(failure::Error::from)
            .and_then(|file| match self.open_files.entry(file_path) {
                hash_map::Entry::Vacant(entry) => {
                    let file = GrinderFile {
                        file
                    };
                    entry.insert(file);
                    Ok(FSOperationResult::Done)
                }

                hash_map::Entry::Occupied(entry) =>
                    bail!("File {} already opened", entry.key().display())
            })
    }
}

struct FSOperationSerializer<S, F> where S: SerializeSeq {
    ser: Option<S>,
    inner: F
}

impl<S, F> FSOperationSerializer<S, F> where S: SerializeSeq,
                                                  F: FSOperationExecutor,
                                                  F::Error: From<S::Error> {
    fn new<Ser>(ser: Ser, executor: F) -> Result<Self, Ser::Error>
        where Ser: Serializer<SerializeSeq=S, Ok=S::Ok, Error=S::Error> {
        ser.serialize_seq(None)
            .map(move |ser|
                Self {
                    ser: Some(ser),
                    inner: executor
                })
    }
}

impl<S, F> FSOperationExecutor for FSOperationSerializer<S, F>
    where S: SerializeSeq,
          F: FSOperationExecutor,
          F::Error: From<S::Error> {
    type Error = F::Error;

    fn execute_fs_operation(&mut self, operation: FSOperation)
        -> Result<FSOperationResult, Self::Error> {
        self.ser.as_mut().unwrap().serialize_element(&operation)?;
        self.inner.execute_fs_operation(operation)
    }
}

impl<S, F> Drop for FSOperationSerializer<S, F> where S: SerializeSeq {
    fn drop(&mut self) {
        if let Err(e) = self.ser.take().unwrap().end() {
            warn!("Unable to end log sequence: {}", e);
        }
    }
}

fn grind(grind_args: GrindArguments) -> Fallible<()> {
    let GrindArguments {
        log_output,
        test_path,
        reference_path,
        timeout,
        rounds,
        max_created_files,
        max_open_files,
        max_write_size
    } = grind_args;

    let mut duration_keeper = if let Some(timeout) = timeout {
        GrindDurationKeeper::with_timeout(timeout)
    } else {
        GrindDurationKeeper::with_rounds(rounds.unwrap())
    };

    let mut fs_op_rng = SmallRng::from_entropy();

    let reference_rng = SmallRng::from_rng(&mut fs_op_rng)?;
    let assessed_rng = reference_rng.clone();

    let assessed_fs_grinder = FSGrinder::new(test_path, assessed_rng);
    let reference_fs_grinder = FSGrinder::new(reference_path, reference_rng);
    let operation_executor = FSOperationComparator::new(reference_fs_grinder, assessed_fs_grinder);

    let output = if let Some(log_output_path) = log_output {
        let file = BufWriter::new(File::create(log_output_path)?);
        Either::Left(file)
    } else {
        Either::Right(io::sink())
    };

    let mut serializer = serde_json::Serializer::pretty(output);
    let mut op_log_and_execute = FSOperationSerializer::new(&mut serializer, operation_executor)?;

    let fs_op_gen =
        RandFSOpGenerator::new(fs_op_rng, max_write_size, max_open_files, max_created_files);

    for op in fs_op_gen.take_while(move |_| !duration_keeper.elapsed()) {
        op_log_and_execute.execute_fs_operation(op)?;
    }

    Ok(())
}

fn replay(log_path: PathBuf, reference_path: PathBuf, assessed_path: PathBuf) -> Fallible<()> {
    let log_file = BufReader::new(File::open(log_path)?);
    let log: Vec<FSOperation> = serde_json::from_reader(log_file)?;

    let reference_rng = SmallRng::from_entropy();
    let assessed_rng = reference_rng.clone();

    let assessed_fs_grinder = FSGrinder::new(assessed_path, assessed_rng);
    let reference_fs_grinder = FSGrinder::new(reference_path, reference_rng);
    let mut operation_executor = FSOperationComparator::new(reference_fs_grinder, assessed_fs_grinder);

    for op in log {
        operation_executor.execute_fs_operation(op)?;
    }

    Ok(())
}

fn main() {
    flexi_logger::Logger::with_env().start().unwrap();

    let args: Arguments = Arguments::from_args();
    match args {
        Arguments::Grind(grind_args) => grind(grind_args),
        Arguments::Replay { log_path, test_path, reference_path } =>
            replay(log_path, reference_path, test_path)
    }.unwrap();
}
