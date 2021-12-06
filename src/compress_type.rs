use std::{
	fmt,
	io::{self, Error, ErrorKind},
	path::Path,
};	

use crate::tools::*;

#[derive(Debug, Copy, Clone)]
pub enum CompressThreads {
	Default,
	Set(usize),
	NCores,
	NPhysCores,	
}

impl Default for CompressThreads {
	fn default() -> Self { Self::Default }
}

impl CompressThreads {
	pub fn n_threads(&self) -> Option<usize> {
		match self {
			Self::Default => None,
			Self::Set(x) => Some(*x),
			Self::NCores => Some(num_cpus::get()),
			Self::NPhysCores => Some(num_cpus::get_physical()),
		}
	}	
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum CompressType {
	Gzip,
	Bgzip,
	Compress,
	Bzip2,
	Xz,
	Lz4,
	Lzma,
	Zstd,
	NoFilter,
	Unknown,
}

impl Default for CompressType {
	fn default() -> Self { Self::Unknown }
}

impl fmt::Display for CompressType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", match self {
			Self::Gzip => "gzip",
			Self::Bgzip => "bgzip",
			Self::Compress => "compress",
			Self::Bzip2 => "bzip2",
			Self::Xz => "xz",
			Self::Lz4 => "lz4",
			Self::Lzma => "lzma",
			Self::Zstd => "zstd",
			Self::NoFilter => "no filter",
			Self::Unknown => "unknown",
		})
	}	
}

impl CompressType {
	
	pub fn from_suffix<P: AsRef<Path>>(path: P) -> Self {
		match path.as_ref().extension().map(|ext| ext.to_str()).flatten() {
			Some("gz") => Self::Gzip,
			Some("Z") => Self::Compress,
			Some("bz2") => Self::Bzip2,
			Some("xz") => Self::Xz,
			Some("lz4") => Self::Lz4,
			Some("lzma") => Self::Lzma,
			Some("zst") => Self::Zstd,
			Some(_) | None => Self::NoFilter,
		}
	}
	
	pub fn from_str(s: &str) -> Option<Self> {
		let s = s.to_ascii_uppercase();
		match s.as_str() {
			"gzip" => Some(Self::Gzip),
			"bzip2" => Some(Self::Bzip2),
			"bgzip" => Some(Self::Bgzip),
			"xz" => Some(Self::Xz),
			"lz4" => Some(Self::Lz4),
			"lzma" => Some(Self::Lzma),
			"zstd" => Some(Self::Zstd),
			 _ => None,
		}
	}
	
	pub fn suffix(&self) -> &'static str {
		match self {
			Self::Gzip => "gz",
			Self::Bgzip => "gz",
			Self::Compress => "Z",
			Self::Bzip2 => "bz2",
			Self::Xz => "xz",
			Self::Lzma => "lzma",
			Self::Lz4 => "lz4",
			Self::Zstd => "zst",
			_ => "",
		}
	}

	pub fn get_decompress_tool(&self) -> io::Result<&Tool> {
		get_decompress_tool(*self).ok_or_else(|| Error::new(ErrorKind::Other, format!("Can not find program to decompress {} files", self)))
	} 

	pub fn get_compress_tool(&self) -> io::Result<&Tool> {
		get_compress_tool(*self).ok_or_else(|| Error::new(ErrorKind::Other, format!("Can not find program to compress {} files", self)))
	} 
}

/// Guess file type if possible by reading first 6 bytes and looking for magic numbers
/// Check magic numbers from first 6 bytes of buf
/// Will panic if buf length < 6 
pub(crate) fn get_ctype(buf: &[u8]) -> CompressType {
	
	assert!(buf.len() >= 6);
	
	let mut ctype = CompressType::NoFilter;
	
	if buf[0] == 0x1f {
		if buf[1] == 0x9d {
			ctype = CompressType::Compress
		} else if buf[1] == 0x8b && buf[2] == 0x08 {
			ctype = if (buf[3] & 4) == 0 { CompressType::Gzip } else { CompressType::Bgzip }
		}
	} else if buf[0] == b'B' && buf[1] == b'Z' && buf[2] == b'h' && buf[3] >= b'0' && buf[3] <= b'9' {
		ctype = CompressType::Bzip2
	} else if buf[0] == 0xfd && buf[1] == b'7' && buf[2] == b'z' && buf[3] == b'X' && buf[4] == b'Z' && buf[5] == 0x00 {
		ctype = CompressType::Xz
	} else if buf[0] == 0x28 && buf[1] == 0xB5 && buf[2] == 0x2F && buf[3] == 0xFD {
		ctype = CompressType::Zstd
	} else if buf[0] == 0x04 && buf[1] == 0x22 && buf[2] == 0x4D && buf[3] == 0x18 {
		ctype = CompressType::Lz4
	} else if buf[0] == 0x5D && buf[1] == 0x0 && buf[2] == 0x0 {
		ctype = CompressType::Lzma
	}
	ctype	 
}


