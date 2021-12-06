use std::{
	path::{Path, PathBuf},
	ffi::OsStr,
};

use super::compress_type::CompressType;

#[derive(Debug)]
pub struct FilterSpec {
	path: PathBuf,
	args: Vec<Box<OsStr>>,
	compress_type: Option<CompressType>,
}

impl FilterSpec {
	pub fn new<P: AsRef<Path>, I, S>(path: P, args: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>, 	 
	{
		let path = path.as_ref().to_owned();
		let args: Vec<_> = args.into_iter().map(|s| Box::from(s.as_ref())).collect();
		Self{path, args, compress_type: None}
	}

	pub fn new_compress<P: AsRef<Path>, I, S>(path: P, args: I, ctype: CompressType) -> Self
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>, 	 
	{
		let path = path.as_ref().to_owned();
		let args: Vec<_> = args.into_iter().map(|s| Box::from(s.as_ref())).collect();
		Self{path, args, compress_type: Some(ctype)}
	}
	
	pub fn cond_add_suffix<P: AsRef<Path>>(&self, name: P) -> PathBuf {
		match self.compress_type {
			Some(ct) => super::path_utils::cond_add_suffix(name, ct.suffix()),
			None => name.as_ref().to_owned()
		}
	}
	
	pub fn compress_type(&self) -> Option<CompressType> { self.compress_type }
	pub fn path(&self) -> &Path { &self.path}
	pub fn args(&self) -> &[Box<OsStr>] { &self.args}
}

