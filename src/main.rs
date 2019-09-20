use std::{
    io::{SeekFrom, stdout},
    path::PathBuf,
    fmt::{self, Formatter, Display},
    fs::{self, File},
    collections::{hash_map, HashMap},
    cell::RefCell,
    io::{self, Read, Seek}
};

use structopt::StructOpt;
use rand::{
    prelude::*,
    distributions::Alphanumeric
};
use serde::{Serialize, Serializer, ser::SerializeSeq};
use either::Either;
use failure::{self, format_err, bail, Fallible};

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
    Grind {
        #[structopt(short = "l", long, parse(from_os_str))]
        /// Specifies the path the log file shall be written to. If not specified, output to stdout.
        log_output: Option<PathBuf>,
        #[structopt(parse(from_os_str))]
        /// Path to the file system that shall be assessed.
        test_path: PathBuf,
        #[structopt(parse(from_os_str))]
        /// Path that shall serve as the reference file system (i.e. a mature file system that can
        /// be trusted).
        reference_path: PathBuf
    },

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

#[derive(Clone, Copy)]
struct RefCellRng<'a, R: RngCore>(&'a RefCell<R>);

impl<'a, R: RngCore> RngCore for RefCellRng<'a, R> {
    fn next_u32(&mut self) -> u32 {
        self.0.borrow_mut().next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.borrow_mut().next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.borrow_mut().fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.0.borrow_mut().try_fill_bytes(dest)
    }
}

struct RandomReader<R: Rng>(R);

impl<R: Rng> Read for RandomReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.fill_bytes(buf);
        Ok(buf.len())
    }
}

mod seekfrom_serde {
    use std::{
        borrow::Cow,
        io::SeekFrom,
        convert::TryInto
    };

    use serde::{Serialize, Deserialize, Serializer, Deserializer, de::Error};

    #[derive(Debug, Serialize, Deserialize)]
    struct SeekPos {
        seek_mode: Cow<'static, str>,
        seek_offset: i64
    }

    impl TryInto<SeekFrom> for SeekPos {
        type Error = String;

        fn try_into(self) -> Result<SeekFrom, Self::Error> {
            match self.seek_mode.as_ref() {
                "begin" => Ok(SeekFrom::Start(self.seek_offset as u64)),
                "cur" => Ok(SeekFrom::Current(self.seek_offset)),
                "end" => Ok(SeekFrom::End(self.seek_offset)),
                _ => Err(format!("Unable to translate \"{}\" into a seek operation", &self.seek_mode))
            }
        }
    }

    impl From<SeekFrom> for SeekPos {
        fn from(val: SeekFrom) -> Self {
            let (seek_mode, seek_offset) = match val {
                SeekFrom::Start(offset) => ("start", offset as i64),
                SeekFrom::Current(offset) => ("cur", offset),
                SeekFrom::End(offset) => ("end", offset),
            };

            Self {
                seek_mode: Cow::Borrowed(seek_mode),
                seek_offset
            }
        }
    }

    pub fn serialize<S: Serializer>(val: &SeekFrom, serializer: S) -> Result<S::Ok, S::Error> {
        let seekpos = SeekPos::from(val.clone());
        seekpos.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SeekFrom, D::Error> {
        SeekPos::deserialize(deserializer)
            .and_then(|pos|
                pos.try_into().map_err(D::Error::custom))
    }
}

#[derive(Debug, Serialize, Clone)]
enum FSOperation {
    OpenFile {
        file_path: PathBuf
    },
    CreateFile {
        file_path: PathBuf
    },
    SeekOpenFile {
        file_path: PathBuf,
        #[serde(with = "seekfrom_serde")]
        seek_pos: SeekFrom
    },
    CloseFile {
        file_path: PathBuf
    },
    ReadData {
        file_path: PathBuf,
        read_size: u64
    },
    WriteRandomData {
        file_path: PathBuf,
        written_size: u64
    }
}

const NUM_FS_OPERATIONS: usize = 6;

struct SeekFromDisplay<'a>(&'a SeekFrom);

impl<'a> Display for SeekFromDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.0 {
            SeekFrom::Start(offset) =>
                write!(f, "{} bytes from start", offset),
            SeekFrom::Current(offset) =>
                write!(f, "{} bytes from the current position", offset),
            SeekFrom::End(offset) =>
                write!(f, "{} bytes back from the end", -*offset),
        }
    }
}

impl Display for FSOperation {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            FSOperation::WriteRandomData { file_path, written_size } =>
                write!(f, "Write {} random bytes to {}", written_size, file_path.display()),
            FSOperation::SeekOpenFile { file_path, seek_pos } =>
                write!(f, "Seek to {} in file {}", SeekFromDisplay(seek_pos), file_path.display()),
            FSOperation::OpenFile { file_path } =>
                write!(f, "Open {}", file_path.display()),
            FSOperation::CreateFile { file_path } =>
                write!(f, "Create file {}", file_path.display()),
            FSOperation::CloseFile { file_path } =>
                write!(f, "Close file {}", file_path.display()),
            FSOperation::ReadData { file_path, read_size } =>
                write!(f, "Read {} bytes from file {}", read_size, file_path.display())
        }
    }
}

#[derive(Debug, Clone)]
struct FileDescriptor {
    path: PathBuf,
    size: u64
}

#[derive(Debug, Clone)]
struct OpenFileDescriptor {
    file_desc: FileDescriptor,
    pos: u64
}

impl Seek for OpenFileDescriptor {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let newpos = match pos {
            SeekFrom::Start(newpos) => newpos as i64,
            SeekFrom::Current(offset) => self.pos as i64 + offset,
            SeekFrom::End(offset) => self.file_desc.size as i64 + offset,
        };

        if newpos < 0 {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                               "Attempt to seek before start of file"))
        } else {
            self.file_desc.size = self.file_desc.size.max(newpos as u64);
            self.pos = newpos as u64;
            Ok(self.pos)
        }
    }
}

struct RandFSOpGenerator<R: Rng> {
    rng: R,
    open_files: Vec<OpenFileDescriptor>,
    existing_files: Vec<FileDescriptor>
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
                0 => if let Some(OpenFileDescriptor { file_desc: closed_file, pos: _ }) =
                self.open_files.remove_random(&mut self.rng) {
                    let result = FSOperation::CloseFile { file_path: closed_file.path.clone() };
                    self.existing_files.push(closed_file);
                    break result;
                },
                1 => {
                    let name_len = self.rng.gen_range(6, 32);
                    let name = (&mut self.rng).sample_iter(Alphanumeric).take(name_len).collect::<String>();
                    let path = PathBuf::from(name);
                    self.open_files.push(OpenFileDescriptor {
                        file_desc: FileDescriptor {
                            path: path.clone(),
                            size: 0
                        },
                        pos: 0
                    });
                    break FSOperation::CreateFile { file_path: path };
                },
                2 => if let Some(open_file) = self.existing_files.remove_random(&mut self.rng) {
                    let result = FSOperation::OpenFile { file_path: open_file.path.clone() };
                    self.open_files.push(OpenFileDescriptor {
                        file_desc: open_file,
                        pos: 0
                    });
                    break result;
                },
                3 => if let Some(file) = self.open_files.choose_mut(&mut self.rng) {
                    if file.file_desc.size > 0 {
                        let seek_pos = match self.rng.gen_range(0, 3) {
                            0 => SeekFrom::Start(self.rng.gen_range(1, file.file_desc.size)),
                            1 => SeekFrom::Current(self.rng.gen_range(-(file.pos as i64), (file.file_desc.size-file.pos) as i64)),
                            2 => SeekFrom::End(self.rng.gen_range(-(file.file_desc.size as i64), -1)),
                            _ => unreachable!()
                        };
                        file.seek(seek_pos).unwrap();
                        break FSOperation::SeekOpenFile { file_path: file.file_desc.path.clone(), seek_pos };
                    }
                },
                4 => if let Some(file) = self.open_files.choose_mut(&mut self.rng) {
                    let written_size = self.rng.gen_range(1, 2048);
                    file.pos += written_size;
                    file.file_desc.size = file.pos.max(file.file_desc.size);
                    break FSOperation::WriteRandomData { file_path: file.file_desc.path.clone(), written_size };
                },
                5 => if let Some(file) = self.open_files.choose_mut(&mut self.rng) {
                    let possible_read_size = file.file_desc.size - file.pos;
                    if possible_read_size > 0 {
                        let read_size = self.rng.gen_range(1, possible_read_size);
                        file.pos += read_size;
                        break FSOperation::ReadData {
                            read_size,
                            file_path: file.file_desc.path.clone()
                        };
                    }
                }
                _ => panic!("NUM_FS_OPERATIONS larger than number of FSOperarion variants")
            }
        };

        Some(operation)
    }
}

impl<R: Rng> RandFSOpGenerator<R> {
    fn new(rng: R) -> Self {
        Self {
            rng,
            open_files: Default::default(),
            existing_files: Default::default()
        }
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

struct FSGrinder<R> {
    base_path: PathBuf,
    open_files: HashMap<PathBuf, File>,
    rng: R
}

impl<R: Rng> FSOperationExecutor for FSGrinder<R> {
    type Error = failure::Error;

    fn execute_fs_operation(&mut self, operation: FSOperation) -> Fallible<FSOperationResult> {
        match operation {
            FSOperation::SeekOpenFile { seek_pos, ref file_path } => {
                let file = self.open_files.get_mut(file_path)
                    .ok_or(format_err!("Unable to find open file {}", file_path.display()))?;
                file.seek(seek_pos)
                    .map(FSOperationResult::Position)
                    .map_err(Into::into)
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
                io::copy(&mut reader, file)
                    .map(|_| FSOperationResult::Done)
                    .map_err(|e| format_err!("Failed to write {} bytes to {}: {}", written_size, file_path.display(), e))
            }

            FSOperation::ReadData { read_size, ref file_path } => {
                let file = self.open_files.get_mut(file_path).ok_or(
                    format_err!("Unable to find open file {} for reading", file_path.display()))?;
                let mut data = Vec::with_capacity(read_size as usize);
                file.read_exact(data.as_mut())
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
            eprintln!("Unable to end log sequence: {}", e);
        }
    }
}

fn grind(log_output: Option<PathBuf>, reference_path: PathBuf, assessed_path: PathBuf)
    -> Fallible<()> {
    let mut fs_op_rng = SmallRng::from_entropy();

    let reference_rng = SmallRng::from_rng(&mut fs_op_rng)?;
    let assessed_rng = reference_rng.clone();

    let assessed_fs_grinder = FSGrinder::new(assessed_path, assessed_rng);
    let reference_fs_grinder = FSGrinder::new(reference_path, reference_rng);
    let operation_executor = FSOperationComparator::new(reference_fs_grinder, assessed_fs_grinder);

    let output = if let Some(log_output_path) = log_output {
        let file = File::create(log_output_path)?;
        Either::Left(file)
    } else {
        Either::Right(stdout())
    };

    let mut serializer = serde_json::Serializer::pretty(output);
    let mut op_log_and_execute = FSOperationSerializer::new(&mut serializer, operation_executor)?;

    let fs_op_gen = RandFSOpGenerator::new(fs_op_rng);

    for op in fs_op_gen.take(2048) {
        op_log_and_execute.execute_fs_operation(op)?;
    }

    Ok(())
}

fn main() {
    let args: Arguments = Arguments::from_args();
    match args {
        Arguments::Grind {
            log_output,
            test_path,
            reference_path
        } => grind(log_output, reference_path, test_path),
        Arguments::Replay { .. } => unimplemented!("replay command still to be done...")
    }.unwrap();
}
