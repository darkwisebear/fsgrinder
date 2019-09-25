use std::{
    io::SeekFrom,
    path::PathBuf,
    fmt::{self, Formatter, Display}
};

use serde::{Serialize, Deserialize};

#[serde(rename_all = "lowercase")]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SeekMode {
    Start,
    Current,
    End
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SeekPos {
    pub seek_mode: SeekMode,
    pub rel_seek_pos: f32
}

impl SeekPos {
    pub fn into_seek_from(self, file_pos: u64, file_size: u64) -> SeekFrom {
        let newpos = (self.rel_seek_pos * (file_size as f32)) as i64;
        match self.seek_mode {
            SeekMode::Start => SeekFrom::Start(newpos as u64),
            SeekMode::Current => SeekFrom::Current(newpos - (file_pos as i64)),
            SeekMode::End => SeekFrom::End(newpos - (file_size as i64))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FSOperation {
    OpenFile {
        file_path: PathBuf
    },
    CreateFile {
        file_path: PathBuf
    },
    SeekOpenFile {
        file_path: PathBuf,
        seek_pos: SeekPos
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

pub const NUM_FS_OPERATIONS: usize = 6;

impl Display for SeekPos {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mode = match self.seek_mode {
            SeekMode::Start => "start",
            SeekMode::Current => "current position",
            SeekMode::End => "end",
        };
        write!(f, "{}% from the {}", self.rel_seek_pos * 100.0, mode)
    }
}

impl Display for FSOperation {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            FSOperation::WriteRandomData { file_path, written_size } =>
                write!(f, "Write {} random bytes to {}", written_size, file_path.display()),
            FSOperation::SeekOpenFile { file_path, seek_pos } =>
                write!(f, "Seek to {} in file {}", seek_pos, file_path.display()),
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
