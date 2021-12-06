use std::{
	path::{Path, PathBuf},
	io::{self, Read, stdin},
	ffi::{OsString, OsStr, CString},
	os::unix::ffi::OsStrExt,
	fs::File,
	env,
	ops::{Deref, DerefMut},
};

use crate::compress_type::CompressType;

fn access(p: &Path) -> Result<bool, String> {
	let cstr = CString::new(p.as_os_str().as_bytes()).map_err(|e| format!("access(): error converting {}: {}", p.display(), e))?;
	unsafe { Ok(libc::access(cstr.as_ptr(), libc::X_OK) == 0) }
}

pub fn find_exec_path<S: AsRef<OsStr>>(prog: S) -> Option<PathBuf> {
	let search_path = env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/usr/local/bin"));
	for path in env::split_paths(&search_path) {
		let candidate = path.join(prog.as_ref());
		if candidate.exists() {
			if let Ok(true) = access(&candidate) { return Some(candidate) }
		}
	}
	None
}

fn add_ext_to_path<S: AsRef<OsStr>>(p: &Path, ext: S) -> PathBuf {
	let p: &OsStr = p.as_ref();
	let mut buf = p.to_os_string();
	buf.push(".");
	buf.push(ext);
	PathBuf::from(buf)	
}

///  Add suffix to path if not already there
pub fn cond_add_suffix<P: AsRef<Path>, Q: AsRef<Path>>(path: P, suffix: Q) -> PathBuf {
	let path = path.as_ref();
	let suffix = suffix.as_ref();
	match path.extension() {
		Some(s) if s == suffix => path.to_owned(),
		_ => add_ext_to_path(path, suffix)
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckBuf(Vec<u8>);

impl Default for CheckBuf {
	fn default() -> Self { Self(vec!(0; 6)) }
}

impl Deref for CheckBuf {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for CheckBuf {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl CheckBuf {
	pub fn new() -> Self { Self::default() }

	pub fn clear(&mut self) {
		self.0.clear()
	}
}

pub fn check_read_ctype<P: AsRef<Path>>(name: Option<P>, ctype: CompressType, buf: Option<&mut CheckBuf>) -> io::Result<CompressType> {
	if matches!(ctype, CompressType::Unknown) {
		if let Some(s) = name {
			guess_ctype_from_file(s.as_ref(), buf)
		} else {
			if let Some(b) = buf {
				guess_ctype_from_handle(&mut stdin(), b)
			} else {
				Ok(CompressType::NoFilter)
			}
		}		
	} else {
		if let Some(b) = buf { b.clear() }
		Ok(ctype)
	}
}


pub fn guess_ctype_from_file<P: AsRef<Path>>(path: P, buf: Option<&mut CheckBuf>) -> io::Result<CompressType> {
	let mut f = File::open(path.as_ref())?;

	if let Some(b) = buf {
		guess_ctype_from_handle(&mut f, b)
	} else {
		let mut buf = CheckBuf::default();
		guess_ctype_from_handle(&mut f, &mut buf)
	}
}

fn guess_ctype_from_handle<R: Read>(f: &mut R, buf: &mut CheckBuf) -> io::Result<CompressType> {
	Ok(if f.read(buf)? == 6 {
		crate::compress_type::get_ctype(buf)
	} else {
		CompressType::NoFilter
	})
}
