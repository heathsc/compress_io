use tokio::{
	process::{Child, ChildStdin, ChildStdout, Command},
	io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt, AsyncWrite, BufReader, BufWriter, Stdout, stdin, stdout, Error, ErrorKind },
	fs::File,
	runtime::Runtime,
};

use std::{
	pin::Pin,
	task::{Context, Poll},
	process::Stdio,
	path::{Path, PathBuf},
	thread,
};

use tokio_pipe::pipe;

use crate::{
	compress_type::{CompressThreads, CompressType},
	filter_spec::FilterSpec,
	path_utils::*,
};

#[derive(Debug)]
pub enum Filter {
	NoFilter,
	Filter(FilterSpec),
}

fn piped_stdin<W: AsyncWriteExt + Unpin + Send + 'static>(buf: CheckBuf, mut wr: W) {
	thread::spawn(move || {
		let rt = Runtime::new().expect("Error creating new runtime");
		rt.block_on(async {
			let mut tbuf = [0; 65536];
			let mut rd = io::stdin();
			wr.write_all(&buf).await.expect("Error writing to pipe");
			while let Ok(n) = rd.read(&mut tbuf).await {
				if n > 0 {
					assert!(n <= tbuf.len());
					wr.write_all(&tbuf[..n]).await.expect("Error writing to pipe");
				} else {
					break
				}
			}
		});
	});
}

#[derive(Debug)]
pub enum PipeType {
	Stdio(Stdio),
	Pipe(CheckBuf),
	Stdin,
}

impl Default for PipeType {
	fn default() -> Self { Self::Stdin }
}

impl Filter {
	pub async fn new_read_filter<P: AsRef<Path>>(&self, name: Option<P>, buf: CheckBuf) -> io::Result<Box<dyn AsyncRead + Unpin>> {

		let buf = if name.is_none() && !buf.is_empty() { Some(buf) } else { None };
		Ok(match self {
			Filter::NoFilter => if let Some(s) = name {
				Box::new(File::open(s.as_ref()).await?)
			} else if let Some(b) = buf {
				let (rd, wr) = pipe().expect("Couldn't open pipe");
				piped_stdin(b, wr);
				Box::new(rd)
			} else {
				Box::new(stdin())
			},
			Filter::Filter(f)=> if let Some(s) = name {
				let input = PipeType::Stdio(Stdio::from(File::open(s.as_ref()).await?.into_std().await));
				Box::new(open_read_filter(f, input).await?)
			} else {
				let input = if let Some(b) = buf { PipeType::Pipe(b) } else { PipeType::Stdin };
				Box::new(open_read_filter(f, input).await?)
			},
		})
	}
	
	pub async fn new_write_filter<P: AsRef<Path>>(&self, name: Option<P>, fix_path: bool) -> io::Result<Box<dyn AsyncWrite + Unpin>> {

		// Add compression suffix if required (and not already present and fix_path is not set)
		let name = match (name, self) {
			(Some(p), Filter::Filter(f)) => if fix_path { Some(p.as_ref().to_owned()) } else { Some(f.cond_add_suffix(p.as_ref())) },
			(Some(p), _) =>  Some(p.as_ref().to_owned()),
			_ => None,
		};
		
		Ok(match self {
			Filter::NoFilter => if let Some(s) = name {
				Box::new(Writer::from_file(File::create(&s).await?))
			} else {
				Box::new(Writer::from_stdout())
			},
			Filter::Filter(f) => if let Some(s) = name {
				Box::new(Writer::from_child(open_write_filter(f, Some(Stdio::from(File::create(&s).await?.into_std().await))).await?))
			} else {
				Box::new(Writer::from_child(open_write_filter(f, None).await?))
			},
		})
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

pub async fn open_read_filter(f: &FilterSpec, input: PipeType) -> io::Result<ChildStdout> {
	let mut com = Command::new(f.path());

	let (com, buf) = match input {
		PipeType::Stdio(s) => (com.stdin(s), None),
		PipeType::Stdin => (com.stdin(Stdio::inherit()), None),
		PipeType::Pipe(buf) => (com.stdin(Stdio::piped()), Some(buf)),
	};

	match com.args(f.args()).stdout(Stdio::piped()).spawn() {
		Ok(mut proc) => {
			if let Some(b) = buf {
				let wr = proc.stdin.take().expect("pipe problems getting stdin");
				piped_stdin(b, wr)
			}
			Ok(proc.stdout.expect("pipe problem"))
		},
		Err(error) => Err(Error::new(ErrorKind::Other, format!("Error executing pipe command '{}': {}", f.path().display(), error))),
	}
}

pub async fn open_write_filter(f: &FilterSpec, output: Option<Stdio>) -> io::Result<Child> {	
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

struct WriterType<T: AsyncWrite + Unpin> {
	inner: T,
}

impl <T: AsyncWrite + Unpin>AsyncWrite for WriterType<T> {
	fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, src: &[u8]) -> Poll<io::Result<usize>> {	
		Pin::new(&mut self.as_mut().inner).poll_write(cx, src)
	}	
	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
		Pin::new(&mut self.as_mut().inner).poll_flush(cx)
	}
	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
		self.poll_flush(cx)
	}
}

struct Writer<T: AsyncWrite + Unpin> {
	child: Option<Child>,
	wrt: Option<WriterType<T>>,
}

impl <T: AsyncWrite + Unpin>AsyncWrite for Writer<T> {
	fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, src: &[u8]) -> Poll<io::Result<usize>> {	
		match self.wrt.as_mut() {
			Some(mut wt) => Pin::new(&mut wt).poll_write(cx, src),
			None => Poll::Ready(Ok(0)),
		}
	}	
	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
		match self.wrt.as_mut() {
			Some(mut wt) => Pin::new(&mut wt).poll_flush(cx),
			None => Poll::Ready(Ok(())),
		}
	}
	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
		self.poll_flush(cx)
	}
}

impl <T: AsyncWrite + Unpin>Drop for Writer<T> {
	fn drop(&mut self) {
		if let Some(mut child) = self.child.take() {
			drop(self.wrt.take());	
			let _ = child.wait();
		}
	}
}

impl Writer<ChildStdin> {
	fn from_child(mut child: Child) -> Self {
		let wrt = child.stdin.take().expect("Pipe error");
		Self{child: Some(child), wrt: Some(WriterType{ inner: wrt }) }
	}
}

impl Writer<File> {
	fn from_file(file: File) -> Self {
		Self{child: None, wrt: Some(WriterType {inner: file})}
	}
}

impl Writer<Stdout> {
	fn from_stdout() -> Self {
		Self{child: None, wrt: Some(WriterType {inner: stdout()})}
	}
}	

#[derive(Default, Debug)]
pub struct AsyncCompressIo {
	path: Option<PathBuf>,
	ctype: CompressType,
	cthreads: CompressThreads,
	fix_path: bool,
}

impl AsyncCompressIo {
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

	pub async fn reader(&self) -> io::Result<Box<dyn AsyncRead + Unpin>> {
		let mut buf = CheckBuf::default();
		let filter = Filter::new_decompress_filter(check_read_ctype(self.path.as_ref(), self.ctype, Some(&mut buf))?)?;
		filter.new_read_filter(self.path.as_ref(), buf).await
	}

	pub async fn bufreader(&self) -> io::Result<BufReader<Box<dyn AsyncRead + Unpin>>> {
		self.reader().await.map(|r| BufReader::new(r))
	}

	pub async fn writer(&self) -> io::Result<Box<dyn AsyncWrite + Unpin>> {
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
		filter.new_write_filter(self.path.as_ref(), self.fix_path).await
	}

	pub async fn bufwriter(&self) -> io::Result<BufWriter<Box<dyn AsyncWrite + Unpin>>> {
		self.writer().await.map(|w| BufWriter::new(w))
	}
}
