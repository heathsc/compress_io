use std::{
	path::{Path, PathBuf},
	process::{Child, ChildStdin, ChildStdout, Command, Stdio},
	io::{self, Read, Write, BufReader, BufWriter, BufRead, Stdout, stdin, stdout, Error, ErrorKind},
	fs::File,
	thread,
};

use crate::compress_type::{CompressThreads, CompressType};
use crate::filter_spec:: FilterSpec;
use crate::path_utils::*;

use os_pipe::{pipe, PipeReader};

#[derive(Debug)]
pub enum Filter {
	NoFilter,
	Filter(FilterSpec),
}

fn piped_stdin(buf: CheckBuf) -> PipeReader {

	let (reader, mut wr) = pipe().expect("Couldn't create pipe");
	thread::spawn(move || {
		let mut tbuf = [0; 65536];
		let mut rd = io::stdin();
		wr.write_all(&buf).expect("Error writing to pipe");
		while let Ok(n) = rd.read(&mut tbuf) {
			if n > 0 {
				assert!(n <= tbuf.len());
				wr.write_all(&tbuf[..n]).expect("Error writing to pipe");
			} else {
				break
			}
		}
	});
	reader
}

impl Filter {
	pub fn new_read_filter<P: AsRef<Path>>(&self, name: Option<P>, buf: CheckBuf) -> io::Result<Box<dyn Read>> {

		let pipe = if name.is_none() && !buf.is_empty() { Some(piped_stdin(buf))  } else { None };
		Ok(match self {
			Filter::NoFilter => if let Some(s) = name {
				Box::new(File::open(s.as_ref())?)
			} else if let Some(p) = pipe {
				Box::new(p)
			} else {
				Box::new(stdin())
			}
			Filter::Filter(f)=> if let Some(s) = name {
				Box::new(open_read_filter(f, Some(File::open(s.as_ref())?))?)
			} else {
				Box::new(open_read_filter(f, pipe)?)
			},
		})
	}

	pub fn new_bufread_filter<P: AsRef<Path>>(&self, name: Option<P>, buf: CheckBuf) -> io::Result<Box<dyn BufRead>> {

		let pipe = if name.is_none() && !buf.is_empty() { Some(piped_stdin(buf))  } else { None };
		Ok(match self {
			Filter::NoFilter => if let Some(s) = name {
				Box::new(BufReader::new(File::open(s.as_ref())?))
			} else if let Some(p) = pipe {
				Box::new(BufReader::new(p))
			} else {
				Box::new(BufReader::new(stdin()))
			},
			Filter::Filter(f)=> if let Some(s) = name {
				Box::new(BufReader::new(open_read_filter(f, Some(File::open(s.as_ref())?))?))
			} else {
				Box::new(BufReader::new(open_read_filter(f, pipe)?))
			},
		})
	}
	
	pub fn new_write_filter<P: AsRef<Path>>(&self, name: Option<P>, bufwriter: bool, fix_path: bool) -> io::Result<Box<dyn Write>> {

		// Add compression suffix if required (and not already present and fix_path is not set)
		let name = match (name, self) {
			(Some(p), Filter::Filter(f)) => if fix_path { Some(p.as_ref().to_owned()) } else { Some(f.cond_add_suffix(p.as_ref())) },
			(Some(p), _) =>  Some(p.as_ref().to_owned()),
			_ => None,
		};

		if bufwriter {
			Ok(match self {
				Filter::NoFilter => if let Some(s) = name {
					Box::new(BufWriter::new(Writer::from_file(File::create(&s)?)))
				} else {
					Box::new(BufWriter::new(Writer::from_stdout()))
				},
				Filter::Filter(f) => if let Some(s) = name {
					Box::new(BufWriter::new(Writer::from_child(open_write_filter(f, Some(File::create(&s)?))?)))
				} else {
					let none: Option<File> = None;
					Box::new(BufWriter::new(Writer::from_child(open_write_filter(f, none)?)))
				},
			})
				
		} else {				
			Ok(match self {

				Filter::NoFilter => if let Some(s) = name {
					Box::new(Writer::from_file(File::create(&s)?))
				} else {
					Box::new(Writer::from_stdout())
				},
				Filter::Filter(f) => if let Some(s) = name {
					Box::new(Writer::from_child(open_write_filter(f, Some(File::create(&s)?))?))
				} else {
					let none: Option<File> = None;
					Box::new(Writer::from_child(open_write_filter(f, none)?))
				},
			})
		}
	}	
	pub fn new_decompress_filter(ctype: CompressType) -> io::Result<Self> {
		
		Ok(match ctype {
			CompressType::NoFilter => Filter::NoFilter,
			_ => {
				let tool = ctype.get_decompress_tool()?;
				
				// Neither of the two statements below should panic unless something has gone wrong...
				let path = tool.path().expect("Unknown path for selected tool");
				let service = tool.get_decompress(ctype).expect("tool does not support selected decompress type");
				
				// Threads only have an effect on compression, so we leave them at their defaults here
				Filter::Filter(FilterSpec::new_compress(path, service.args(CompressThreads::Default), ctype))
			},
		})
	}

	pub fn new_compress_filter(ctype: CompressType, cthreads: CompressThreads) -> io::Result<Self> {
		let tool = ctype.get_compress_tool()?;
			
		// Neither of the two statements below should panic unless something has gone wrong...
		let path = tool.path().expect("Unknown path for selected tool");
		let service = tool.get_compress(ctype).expect("tool does not support selected compress type");

		Ok(Filter::Filter(FilterSpec::new_compress(path, service.args(cthreads), ctype)))
	}
}

impl Default for Filter {
	fn default() -> Self { Self::NoFilter }
}

pub fn open_read_filter<T: Into<Stdio>>(f: &FilterSpec, input: Option<T>) -> io::Result<ChildStdout> {
	let mut com = Command::new(f.path());
	let com = match input {
		Some(s) => com.stdin(s),
		None => com.stdin(Stdio::inherit()),
	};
	match com.args(f.args()).stdout(Stdio::piped()).spawn() {
		Ok(proc) => Ok(proc.stdout.expect("pipe problem")),
		Err(error) => Err(Error::new(ErrorKind::Other, format!("Error executing pipe command '{}': {}", f.path().display(), error))),
	}
}

pub fn open_write_filter<T: Into<Stdio> + std::fmt::Debug>(f: &FilterSpec, output: Option<T>) -> io::Result<Child> {	
	let mut com = Command::new(f.path());
	let com = match output {
		Some(s) => com.stdout(s),
		None => com.stdout(Stdio::inherit()),
	};
	match com.args(f.args()).stdin(Stdio::piped()).spawn() {
		Ok(proc) => Ok(proc),
		Err(error) => Err(Error::new(ErrorKind::Other, format!("Error executing pipe command '{}': {}", f.path().display(), error))),
	}
}	

enum WriterType {
	File(File),
	ChildStdin(ChildStdin),
	Stdout(Stdout),
}

struct Writer {
	child: Option<Child>,
	wrt: Option<WriterType>,
}

impl Write for Writer {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		match &mut self.wrt {
			Some(WriterType::File(f)) => f.write(buf),
			Some(WriterType::ChildStdin(c)) => c.write(buf),
			Some(WriterType::Stdout(s)) => s.write(buf),
			None => Ok(0),
		} 
	}
	fn flush(&mut self) -> io::Result<()> {
		match &mut self.wrt {
			Some(WriterType::File(f)) => f.flush(),
			Some(WriterType::ChildStdin(c)) => c.flush(),
			Some(WriterType::Stdout(s)) => s.flush(),
			None => Ok(()),
		} 		
	}
}

impl Drop for Writer {
	fn drop(&mut self) {
		if let Some(mut child) = self.child.take() {
			drop(self.wrt.take());	
			let _ = child.wait();
		}
	}
}

impl Writer {
	fn from_child(mut child: Child) -> Self {
		let wrt = child.stdin.take().expect("Pipe error");
		Self{child: Some(child), wrt: Some(WriterType::ChildStdin(wrt))}
	}

	fn from_file(file: File) -> Self {
		Self{child: None, wrt: Some(WriterType::File(file))}
	}

	fn from_stdout() -> Self {
		Self{child: None, wrt: Some(WriterType::Stdout(stdout()))}
	}
}	

#[derive(Default, Debug)]
pub struct CompressIo {
	path: Option<PathBuf>,
	ctype: CompressType,
	cthreads: CompressThreads,
	fix_path: bool,
}

impl CompressIo {
	pub fn new() -> Self { Self::default() }

	pub fn path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self
	{
		self.path = Some(path.as_ref().to_owned());
		self
	}

	pub fn opt_path<P: AsRef<Path>>(&mut self, path: Option<P>) -> &mut Self
	{
		self.path = path.map(|p| p.as_ref().to_owned());
		self
	}

	pub fn ctype(&mut self, ctype: CompressType) -> &mut Self {
		self.ctype = ctype;
		self
	}

	pub fn cthreads(&mut self, cthreads: CompressThreads) -> &mut Self {
		self.cthreads = cthreads;
		self
	}

	pub fn fix_path(&mut self, x: bool) -> &mut Self {
		self.fix_path = x;
		self
	}

	pub fn reader(&self) -> io::Result<Box<dyn Read>> {
		let (filter, buf) = self.make_decompress_filter()?;
		filter.new_read_filter(self.path.as_ref(), buf)
	}

	pub fn bufreader(&self) -> io::Result<Box<dyn BufRead>> {
		let (filter, buf) = self.make_decompress_filter()?;
		filter.new_bufread_filter(self.path.as_ref(), buf)
	}

	pub fn writer(&self) -> io::Result<Box<dyn Write>> { self.make_writer(false) }

	pub fn bufwriter(&self) -> io::Result<Box<dyn Write>> { self.make_writer(true) }

	pub fn make_writer(&self, bufwriter: bool) -> io::Result<Box<dyn Write>> {
		let ctype = if self.ctype == CompressType::Unknown {
			if let Some(p) = self.path.as_ref() {
				CompressType::from_suffix(p)
			} else {
				CompressType::NoFilter
			}
		} else {
			self.ctype
		};
		let filter = Filter::new_compress_filter(ctype, self.cthreads)?;
		filter.new_write_filter(self.path.as_ref(), bufwriter, self.fix_path)
	}

	fn make_decompress_filter(&self) -> io::Result<(Filter, CheckBuf)> {
		let mut buf = CheckBuf::default();
		Filter::new_decompress_filter(check_read_ctype(self.path.as_ref(), self.ctype, Some(&mut buf))?)
			.map(|f| (f, buf))
	}
}


