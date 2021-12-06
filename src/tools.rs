use std::{
	fmt,
	collections::HashMap,
	cmp::Ordering,
	path::{PathBuf, Path},
};

use crate::compress_type::{CompressType, CompressThreads};
use crate::path_utils::find_exec_path;

#[derive(Debug, Hash)]
pub struct ToolKey {
	ix: usize,
	priority: usize,
}

impl ToolKey {
	pub fn new(ix: usize, priority: usize) -> Self {
		Self { ix, priority }
	}
}

impl Ord for ToolKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

impl PartialOrd for ToolKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ToolKey {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for ToolKey { }

#[derive(Debug, Default)]
pub struct ToolRegister {
	decompress_tools: HashMap<CompressType, Vec<ToolKey>>,
	compress_tools: HashMap<CompressType, Vec<ToolKey>>,
	tools: Vec<Tool>,
}

impl ToolRegister {
	pub fn new() -> Self { Self::default() }

	fn add(mut self, mut tool: Tool) -> Self {
		tool.add_path();
		let ix = self.tools.len();
		for service in tool.decompress_services() {
			let v = self.decompress_tools.entry(service.compress_type).or_insert_with(Vec::new);
			v.push(ToolKey::new(ix, service.priority));
			v.sort_unstable()
		}
		for service in tool.compress_services() {
			let v = self.compress_tools.entry(service.compress_type).or_insert_with(Vec::new);
			v.push(ToolKey::new(ix, service.priority));
			v.sort_unstable()
		}
		self.tools.push(tool);
		self
	} 	
	
	fn get_tool(&self, vt: Option<&Vec<ToolKey>>) -> Option<&Tool> {
		match vt {	
			Some(v) => {	
				let mut ix = None;
				for tk in v.iter() {
					if self.tools[tk.ix].path().is_some() {
						ix = Some(tk.ix);
						break;
					}
				}; 
				ix.map(|x| &self.tools[x])
			},
			None => None,
		}	
	}
	
	pub fn get_compress_tool(&self, ct: CompressType) -> Option<&Tool> {
		self.get_tool(self.compress_tools.get(&ct))
	}
	
	pub fn get_decompress_tool(&self, ct: CompressType) -> Option<&Tool> {
		self.get_tool(self.decompress_tools.get(&ct))
	}	
}

#[derive(Default, Debug)]
struct ToolMap {
	decompress: Vec<Service>,
	compress: Vec<Service>,
	decompress_services: HashMap<CompressType, usize>,
	compress_services: HashMap<CompressType, usize>,
}

impl ToolMap {
	fn add_decompress(&mut self, service: Service) {
		let ix = self.decompress.len();
		let ct = service.compress_type();
		self.decompress_services.insert(ct, ix);
		self.decompress.push(service);
	}	
	fn add_compress(&mut self, service: Service) {
		let ix = self.compress.len();
		let ct = service.compress_type();
		self.compress_services.insert(ct, ix);
		self.compress.push(service);
	}
	fn get_decompress(&self, ct: CompressType) -> Option<&Service> {
		self.decompress_services.get(&ct).map(|i| &self.decompress[*i])
	}
	fn get_compress(&self, ct: CompressType) -> Option<&Service> {
		self.compress_services.get(&ct).map(|i| &self.compress[*i])
	}
	fn decompress_services(&self) -> &[Service] {
		&self.decompress
	}

	fn compress_services(&self) -> &[Service] {
		&self.compress
	}
}

#[derive(Debug)]
pub struct Tool {
	name: Box<str>,
	inner: ToolMap,
	path: Option<PathBuf>,
}

impl Tool {

	pub fn new<S: AsRef<str>>(name: S) -> Self { 
		Self{ name: Box::from(name.as_ref()), inner: ToolMap::default(), path: None }
	}
	pub fn decompress(mut self, service: Service) -> Self {
		self.inner.add_decompress(service);
		self
	}		
	pub fn compress(mut self, service: Service) -> Self {
		self.inner.add_compress(service);
		self
	}	

	pub fn name(&self) -> &str {
		&self.name
	}
	pub fn get_decompress(&self, ct: CompressType) -> Option<&Service> {
		self.inner.get_decompress(ct)
	}	
	pub fn get_compress(&self, ct: CompressType) -> Option<&Service> {
		self.inner.get_compress(ct)
	}		
	pub fn decompress_services(&self) -> &[Service] {
		self.inner.decompress_services()
	}
	pub fn compress_services(&self) -> &[Service] {
		self.inner.compress_services()
	}
	pub fn add_path(&mut self) {
		self.path = find_exec_path(self.name.as_ref())	
	}
	
	pub fn path(&self) -> Option<&Path> {
		self.path.as_deref()
	}
}

#[derive(Debug)]
pub struct Service {
	compress_type: CompressType,
	options: Vec<ToolOpt>,
	thread_option: Option<ToolOpt>,
	priority: usize,
}

impl Service {
	pub fn new(compress_type: CompressType) -> Self {
		Self{compress_type, options: Vec::new(), thread_option: None, priority: 0}
	}
	
	pub fn option(mut self, opt: ToolOpt) -> Self {
		self.options.push(opt);
		self
	}
	
	pub fn thread_option(mut self, opt: ToolOpt) -> Self {
		self.thread_option = Some(opt);
		self
	}
	
	pub fn priority(mut self, priority: usize) -> Self {
		self.priority = priority;
		self
	}
	pub fn compress_type(&self) -> CompressType { self.compress_type }	
	pub fn args(&self, threads: CompressThreads) -> Vec<String> {
		let mut v: Vec<_> = self.options.iter().map(|o| format!("{}", o)).collect();
		if let (Some(o), Some(n)) = (self.thread_option.as_ref(), threads.n_threads()) {
			match o {
				ToolOpt::Short(s) => v.push(format!("-{}{}", s, n)),
				ToolOpt::Long(s) => {
					v.push(format!("--{}", s));
					v.push(format!("{}", n))
				}
			}	
		}
		v
	}
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum ToolOpt {
	Short(Box<str>),
	Long(Box<str>),
}

impl fmt::Display for ToolOpt {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Short(s) => write!(f, "-{}", s),
			Self::Long(s) => write!(f, "--{}", s),
		}
	}	
}

impl ToolOpt {
	pub fn short<S: AsRef<str>>(opt: S) -> Self { 
		Self::Short(Box::from(opt.as_ref())) 
	}
	pub fn long<S: AsRef<str>>(opt: S) -> Self { 
		Self::Long(Box::from(opt.as_ref()))
	}
}

pub fn get_decompress_tool(ct: CompressType) -> Option<&'static Tool> {
	TOOLS.get_decompress_tool(ct)
}

pub fn get_compress_tool(ct: CompressType) -> Option<&'static Tool> {
	TOOLS.get_compress_tool(ct)
}

lazy_static! {
	static ref TOOLS: ToolRegister = {
		ToolRegister::default()
			.add(Tool::new("uncompress")
				.decompress(Service::new(CompressType::Compress).priority(10)))
			.add(Tool::new("gzip")
				.decompress(Service::new(CompressType::Gzip).option(ToolOpt::short("dcf")).priority(10))
				.decompress(Service::new(CompressType::Bgzip).option(ToolOpt::short("dcf")).priority(5))
				.decompress(Service::new(CompressType::Compress).option(ToolOpt::short("dcf")).priority(5))
				.compress(Service::new(CompressType::Gzip).priority(5)))
			.add(Tool::new("pigz")
				.decompress(Service::new(CompressType::Gzip).option(ToolOpt::short("dcf")))
				.decompress(Service::new(CompressType::Compress).option(ToolOpt::short("dcf")))
				.decompress(Service::new(CompressType::Bgzip).option(ToolOpt::short("dcf")))
				.compress(Service::new(CompressType::Gzip).thread_option(ToolOpt::long("processes")).priority(10)))
			.add(Tool::new("bgzip")
				.decompress(Service::new(CompressType::Bgzip).option(ToolOpt::short("dcf")).priority(10))
				.decompress(Service::new(CompressType::Gzip).option(ToolOpt::short("dcf")))
				.compress(Service::new(CompressType::Bgzip).thread_option(ToolOpt::long("threads")).priority(10))
				.compress(Service::new(CompressType::Gzip))) // Compression with bgzip will give a Bgzip file, but this is compatible with gzip format so we can use this as a last resort
			.add(Tool::new("bzip2")
				.decompress(Service::new(CompressType::Bzip2).option(ToolOpt::short("dcf")).priority(10))
				.compress(Service::new(CompressType::Bzip2).priority(5)))
			.add(Tool::new("pbzip2")
				.decompress(Service::new(CompressType::Bzip2).option(ToolOpt::short("dcf")).priority(5))
				.compress(Service::new(CompressType::Bzip2).thread_option(ToolOpt::short("dcf")).priority(10)))	
			.add(Tool::new("xz")
				.decompress(Service::new(CompressType::Xz).option(ToolOpt::short("dcf")).priority(10))
				.decompress(Service::new(CompressType::Lzma).option(ToolOpt::short("dcf")).priority(10))
				.compress(Service::new(CompressType::Xz).thread_option(ToolOpt::long("threads")).priority(10))
				.compress(Service::new(CompressType::Lzma).option(ToolOpt::long("format=lzma")).thread_option(ToolOpt::long("threads")).priority(10)))
			.add(Tool::new("lzma")
				.decompress(Service::new(CompressType::Lzma).option(ToolOpt::short("dcf")).priority(5))
				.compress(Service::new(CompressType::Lzma).priority(5)))
			.add(Tool::new("lz4")
				.decompress(Service::new(CompressType::Lzma).option(ToolOpt::short("dcfm")).priority(5))
				.compress(Service::new(CompressType::Lzma).priority(5)))				
			.add(Tool::new("zstd")
				.decompress(Service::new(CompressType::Zstd).option(ToolOpt::short("dcf")).priority(10))
				.decompress(Service::new(CompressType::Gzip).option(ToolOpt::short("dcf")))
				.decompress(Service::new(CompressType::Xz).option(ToolOpt::short("dcf")))
				.decompress(Service::new(CompressType::Lzma).option(ToolOpt::short("dcf")))
				.decompress(Service::new(CompressType::Lz4).option(ToolOpt::short("dcf")))
				.compress(Service::new(CompressType::Zstd).thread_option(ToolOpt::short("T")).priority(10))
				.compress(Service::new(CompressType::Gzip).option(ToolOpt::long("format=gzip")))
				.compress(Service::new(CompressType::Xz).option(ToolOpt::long("format=xz")))
				.compress(Service::new(CompressType::Lzma).option(ToolOpt::long("format=lzma")))
				.compress(Service::new(CompressType::Lz4).option(ToolOpt::long("format=lz4")))
			)
	};
}
