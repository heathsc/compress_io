use std::{
    fs::File,
    io::{self, stdin, stdout, BufReader, BufWriter, Error, ErrorKind, Read, Stdin, Stdout, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    thread,
};

use crate::compress_type::{CompressThreads, CompressType};
use crate::filter_spec::FilterSpec;
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
                break;
            }
        }
    });
    reader
}

impl Filter {
    pub fn reader<P: AsRef<Path>>(&self, name: Option<P>, buf: CheckBuf) -> io::Result<Reader> {
        let pipe = if name.is_none() && !buf.is_empty() {
            Some(piped_stdin(buf))
        } else {
            None
        };
        Ok(match self {
            Filter::NoFilter => {
                if let Some(s) = name {
                    Reader::from_file(File::open(s.as_ref())?)
                } else if let Some(p) = pipe {
                    Reader::from_pipe_reader(p)
                } else {
                    Reader::from_stdin()
                }
            }
            Filter::Filter(f) => {
                if let Some(s) = name {
                    Reader::from_child(open_read_filter(f, Some(File::open(s.as_ref())?))?)
                } else {
                    Reader::from_child(open_read_filter(f, pipe)?)
                }
            }
        })
    }

    pub fn writer<P: AsRef<Path>>(
        &self,
        name: Option<P>,
        fix_path: bool,
        no_wait: bool,
    ) -> io::Result<Writer> {
        // Add compression suffix if required (and not already present and fix_path is not set)
        let name = match (name, self) {
            (Some(p), Filter::Filter(f)) => {
                if fix_path {
                    Some(p.as_ref().to_owned())
                } else {
                    Some(f.cond_add_suffix(p.as_ref()))
                }
            }
            (Some(p), _) => Some(p.as_ref().to_owned()),
            _ => None,
        };

        Ok(match self {
            Filter::NoFilter => {
                if let Some(s) = name {
                    Writer::from_file(File::create(&s)?)
                } else {
                    Writer::from_stdout()
                }
            }
            Filter::Filter(f) => {
                if let Some(s) = name {
                    Writer::from_child(open_write_filter(f, Some(File::create(&s)?))?, no_wait)
                } else {
                    let none: Option<File> = None;
                    Writer::from_child(open_write_filter(f, none)?, no_wait)
                }
            }
        })
    }

    pub fn new_decompress_filter(ctype: CompressType) -> io::Result<Self> {
        Ok(match ctype {
            CompressType::NoFilter => Filter::NoFilter,
            _ => {
                let tool = ctype.get_decompress_tool()?;

                // Neither of the two statements below should panic unless something has gone wrong...
                let path = tool.path().expect("Unknown path for selected tool");
                let service = tool
                    .get_decompress(ctype)
                    .expect("tool does not support selected decompress type");

                // Threads only have an effect on compression, so we leave them at their defaults here
                Filter::Filter(FilterSpec::new_compress(
                    path,
                    service.args(CompressThreads::Default),
                    ctype,
                ))
            }
        })
    }

    pub fn new_compress_filter(ctype: CompressType, cthreads: CompressThreads) -> io::Result<Self> {
        Ok(match ctype {
            CompressType::NoFilter => Filter::NoFilter,
            _ => {
                let tool = ctype.get_compress_tool()?;
                // Neither of the two statements below should panic unless something has gone wrong...
                let path = tool.path().expect("Unknown path for selected tool");
                let service = tool
                    .get_compress(ctype)
                    .expect("tool does not support selected compress type");
                Filter::Filter(FilterSpec::new_compress(
                    path,
                    service.args(cthreads),
                    ctype,
                ))
            }
        })
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::NoFilter
    }
}

pub fn open_read_filter<T: Into<Stdio>>(f: &FilterSpec, input: Option<T>) -> io::Result<Child> {
    let mut com = Command::new(f.path());
    let com = match input {
        Some(s) => com.stdin(s),
        None => com.stdin(Stdio::inherit()),
    };
    match com.args(f.args()).stdout(Stdio::piped()).spawn() {
        //Ok(proc) => Ok(proc.stdout.expect("pipe problem")),
        Ok(proc) => Ok(proc),
        Err(error) => Err(Error::new(
            ErrorKind::Other,
            format!(
                "Error executing pipe command '{}': {}",
                f.path().display(),
                error
            ),
        )),
    }
}

pub fn open_write_filter<T: Into<Stdio> + std::fmt::Debug>(
    f: &FilterSpec,
    output: Option<T>,
) -> io::Result<Child> {
    let mut com = Command::new(f.path());
    let com = match output {
        Some(s) => com.stdout(s),
        None => com.stdout(Stdio::inherit()),
    };
    match com.args(f.args()).stdin(Stdio::piped()).spawn() {
        Ok(proc) => Ok(proc),
        Err(error) => Err(Error::new(
            ErrorKind::Other,
            format!(
                "Error executing pipe command '{}': {}",
                f.path().display(),
                error
            ),
        )),
    }
}

/// A compressed writer generated (normally) by [`CompressIo::writer`] or
/// [`CompressIo::bufwriter`].
///
#[derive(Debug)]
pub enum Writer {
    /// Writer created from a [`std::fs::File`]
    File(File),

    /// Writer created from a [`std::process::Child`] which will be waited on
    /// when the instance is dropped
    Child(Option<ChildStdin>, Option<Child>),

    /// Writer created from a [`std::process::ChildStdin`] which will be not waited on
    /// when the instance is dropped
    ChildStdin(ChildStdin),

    /// Writer created from a [`std::io::Stdout`]
    Stdout(Stdout),
}

impl Writer {
    /// Create a writer from a [`std::process::Child`].  If `no_wait` is false create
    /// a [`Writer::Child`] instance that will wait for the child process
    /// to end when the writer is dropped, otherwise it will create a
    /// [`Writer::ChildStdin`] instance that will not wait.  In this case
    /// care must be taken if the output file is to be opened for reading
    /// immediately after the writer is dropped as there is no assurance that
    /// the data has been completely written to disk
    pub fn from_child(mut child: Child, no_wait: bool) -> Self {
        let cs = child.stdin.take().expect("Pipe error");
        if no_wait {
            Self::ChildStdin(cs)
        } else {
            Self::Child(Some(cs), Some(child))
        }
    }

    /// Returns the [`std::process::Child`] instance from a [`Writer::Child`].  Has no effect
    /// on other variants.  If called then the child process will *not* be waited
    /// on when the writer is dropped, and the caller can wait for the child to
    /// finish when required.
    ///
    /// Important! If `wait` is called before the writer is dropped then
    /// the wait call can block.
    pub fn take_child(&mut self) -> Option<Child> {
        match self {
            Self::Child(_, ch) => ch.take(),
            _ => None,
        }
    }

    pub fn from_file(file: File) -> Self {
        Self::File(file)
    }

    pub fn from_stdout() -> Self {
        Self::Stdout(stdout())
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::File(f) => f.write(buf),
            Self::Child(Some(c), _) => c.write(buf),
            Self::ChildStdin(c) => c.write(buf),
            Self::Stdout(s) => s.write(buf),
            _ => Ok(0),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::File(f) => f.flush(),
            Self::Child(Some(c), _) => c.flush(),
            Self::ChildStdin(c) => c.flush(),
            Self::Stdout(s) => s.flush(),
            _ => Ok(()),
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Self::Child(cs, ch) = self {
            if let Some(mut child) = ch.take() {
                drop(cs.take());
                let _ = child.wait();
            }
        }
    }
}

/// A compressed reader generated (normally) by [`CompressIo::reader`] or
/// [`CompressIo::bufreader`].
///

#[derive(Debug)]
pub enum Reader {
    /// Reader created from a [`std::fs::File`]
    File(File),

    /// Reader created from a [`std::process::ChildStdout`]
    // ChildStdout(ChildStdout),
    Child(Child, ChildStdout),

    /// Reader created from [`std::io::Stdin`]
    Stdin(Stdin),

    /// Reader created from a [`os_pipe::PipeReader`]
    PipeReader(PipeReader),
}

impl Drop for Reader {
    fn drop(&mut self) {
        if let Self::Child(c, _) = self {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Reader {
    pub fn from_file(file: File) -> Self {
        Self::File(file)
    }

    pub fn from_stdin() -> Self {
        Self::Stdin(stdin())
    }

    pub fn from_child(mut c: Child) -> Self {
        let cs = c.stdout.take().expect("Erro getting child stdout");
        Self::Child(c, cs)
    }

    pub fn from_pipe_reader(pr: PipeReader) -> Self {
        Self::PipeReader(pr)
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::Child(_, cs) => cs.read(buf),
            Self::Stdin(s) => s.read(buf),
            Self::PipeReader(pr) => pr.read(buf),
        }
    }
}

/// A compressed reader or writer builder, giving control as to how the reader is generated.
///
/// A default config can be generated using [`CompressIo::new`] followed by [`CompressIo::reader`],
/// [`CompressIo::bufreader`], [`CompressIo::writer`] or [`CompressIo::bufwriter`] to make the
/// reader or writer.  Additional commands can be used to set the file name, specify the compression
/// to be used, or set additional options prior to opening the reader or writer.
///
/// # Examples
///
/// Open an xz compressed file `foo.xz`, read the contents into a string, and write out
/// the contents to a gzip compressed file `bar.gz`
///
/// ```no_run
///  use std::io::{self, prelude::*};
///  use compress_io::compress::CompressIo;
///
///  fn main() -> io::Result<()> {
///    let mut rd = CompressIo::new().path("foo.xz").reader()?;
///    let mut buffer = String::new();
///    rd.read_to_string(&mut buffer)?;
///
///    let mut wrt = CompressIo::new().path("bar.gz").writer()?;
///    write!(wrt, "{}", buffer)
///  }
/// ```
///
#[derive(Default, Debug)]
pub struct CompressIo {
    path: Option<PathBuf>,
    ctype: CompressType,
    cthreads: CompressThreads,
    fix_path: bool,
    no_wait: bool,
}

impl CompressIo {
    /// Creates a new instance of [`CompressIo`] wih the default arguments:
    /// * No associated file path so a reader will be connected to `stdin` and a writer to `stdout`
    /// * Compression set to [`CompressType::Unknown`] so a reader will use the first bytes
    ///     from the file/stream and a writer will use the file path extension to determine the
    ///     compression type to use.  Note that if the compression type can not be determined then
    ///     no compression/decompression will be applied
    /// * The file path for a writer will be modified by the addition of the suffix corresponding
    ///     to the chosen compression format if necessary
    /// * Default arguments will be used for threading of compression utilities
    ///
    /// # Examples
    ///
    /// ```rust
    /// use compress_io::compress::CompressIo;
    /// // Create reader from `stdin`.  Compression format will be determined from initial
    /// // bytes from the stream
    /// let mut rd = CompressIo::new().reader();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the file path associated with a reader or writer
    ///
    /// # Examples
    ///
    /// Create reader from file `foo.gz`.  Compression format will be determined from initial
    /// bytes from the file.  Note that the file suffix is *not* considered i.e., if the
    /// file was actually in `bzip2` compression format it would be decompressed with
    /// [bzip2] even though it has a suffix of `.gz`.
    /// ```no_run
    /// use compress_io::compress::CompressIo;
    ///
    /// let mut rd = CompressIo::new().path("foo.gz").reader()
    ///   .expect("Error opening input file");
    /// ```
    ///
    /// Create writer sending data to 'bar.zst'.  By default the stream will be compressed
    /// using [zstd] to correspond with the file extension.
    ///
    /// ```no_run
    ///  use compress_io::compress::CompressIo;
    ///  let mut wrt = CompressIo::new().path("bar.zst").writer()
    ///    .expect("Error opening output file");
    /// ```
    /// [bzip2]: https://sourceware.org/bzip2/
    /// [zstd]: https://facebook.github.io/zstd/
    pub fn path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.path = Some(path.as_ref().to_owned());
        self
    }

    /// Similar to [`CompressIo::path`] but with an `Option` argument.  If called with `None`
    /// this is the same as the default situation where no file path has been specified.
    ///
    /// # Examples
    ///
    /// Create reader from `stdin` and a writer to file `foo.gz`
    ///
    /// ```no_run
    ///  use compress_io::compress::CompressIo;
    ///  let mut rd = CompressIo::new().reader()
    ///    .expect("Error opening input stream");
    ///  let mut wrt = CompressIo::new().opt_path(Some("foo.gz")).writer()
    ///    .expect("Error opening output file");
    /// ```
    pub fn opt_path<P: AsRef<Path>>(&mut self, path: Option<P>) -> &mut Self {
        self.path = path.map(|p| p.as_ref().to_owned());
        self
    }

    /// Sets the compression type for the file/stream.  By default this is set to
    /// [`CompressType::Unknown`] so a reader will use the first bytes
    /// from the file/stream and a writer will use the file path extension to determine the
    /// compression type to use.  Using this function allows the compression type
    /// to be fixed.  See [`CompressType`] to see the list of possible values and the
    /// types of compression that are supported.
    ///
    /// # Examples
    ///
    /// Open a `gzip` compressed stream from stdin and write a `bgzip` compressed
    /// stream to `foo.txt.gz`.  Note that the `gz` extension is automatically added to
    /// the output filename.
    ///
    /// ```no_run
    /// use compress_io::compress::CompressIo;
    /// use compress_io::compress_type::CompressType;
    ///  let mut rd = CompressIo::new().ctype(CompressType::Gzip).reader()
    ///    .expect("Error opening input stream");
    ///  let mut wrt = CompressIo::new().path("foo.txt")
    ///    .ctype(CompressType::Bgzip).writer()
    ///    .expect("Error opening output file");
    /// ```
    pub fn ctype(&mut self, ctype: CompressType) -> &mut Self {
        self.ctype = ctype;
        self
    }

    /// Sets the threading options for compression.  By default no threading options
    /// are applied to compression (i.e., each utility is run with the default threading
    /// options), but by using this function the threading behaviour can be modified,
    /// Note that setting this option for a reader is not an error but currently has no
    /// effect.
    ///
    /// # Examples
    ///
    /// Open a `gzip` compressed output to file `foo.gz` setting the thread options to request
    /// all available cores.  By default in this situation the [pigz] utility would be chosen as
    /// it is multithreaded as opposed to the standard [gzip] utility.
    ///
    /// [gzip]: http://www.gzip.org/
    /// [pigz]: https://www.zlib.net/pigz/
    ///
    /// ```no_run
    ///  use compress_io::compress::CompressIo;
    ///  use compress_io::compress_type::CompressThreads;
    ///  let mut wrt = CompressIo::new().path("foo.gz")
    ///    .cthreads(CompressThreads::NCores).writer()
    ///    .expect("Error opening output file");
    /// ```
    pub fn cthreads(&mut self, cthreads: CompressThreads) -> &mut Self {
        self.cthreads = cthreads;
        self
    }

    /// Prevents the file path for writers being modified by the addition of a compression suffix.
    /// Has no effect on readers.  By default when a writer or bufwriter is generated (with
    /// [`CompressIo::writer`] or [`CompressIo::bufwriter`]) and if a file path has been set (with
    /// [`CompressIo::path`]) then the appropriate file suffix is added to the file name unless it
    /// is already present (i.e., `gz` for `gzip` format files).  By calling `fix_path` this
    /// behaviour is prevented and the file name is not modified.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use compress_io::compress::CompressIo;
    /// use compress_io::compress_type::CompressType;
    ///  // Generate ouput file foo.bz2 with compression suffix
    ///  let mut wrt1 = CompressIo::new().path("foo").ctype(CompressType::Bzip2)
    ///    .writer().expect("Error opening output file");
    ///
    ///  // Generate ouput file bar without compression suffix
    ///  let mut wrt2 = CompressIo::new().path("bar").ctype(CompressType::Bzip2).fix_path()
    ///    .writer().expect("Error opening output file");
    /// ```
    pub fn fix_path(&mut self) -> &mut Self {
        self.fix_path = true;
        self
    }

    /// Don't wait for child process to finish when writer is dropped.
    /// Has no effect on readers.  By default when a writer or bufwriter is dropped and
    /// is closed, the child process will be waited on until the child process terminates.
    /// This avoids problems that can occur if the file is re-opened for reading directly
    /// after it is closed before the child process has finished writing to disk.
    ///
    /// While waiting is the right thing to do in most cases, it can lead to unexpected pauses
    /// as the thread can block when the writer is dropped.  If this behaviour is not wanted then
    /// `no_wait()` can be used to prevent waiting on dropping.
    ///
    /// An alternative to `no_wait()` is to use [`Writer::take_child`] on the writer.  This returns
    /// the child process so that the calling thread can choose when to wait for the child process.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use compress_io::compress::CompressIo;
    /// use compress_io::compress_type::CompressType;
    ///  // Generate ouput file foo.bz2 with compression suffix
    ///  let mut wrt1 = CompressIo::new().path("foo").ctype(CompressType::Bzip2).no_wait()
    ///    .writer().expect("Error opening output file");
    /// ```
    pub fn no_wait(&mut self) -> &mut Self {
        self.no_wait = true;
        self
    }

    /// Generates a [`Read`] instance using the supplied settings.  This will return [`io::Error`]
    /// on failure which could be due to various reasons such as the source file not existing or
    /// not being accessible, or a suitable utility for decompressing not being available in the
    /// user's `$PATH`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::io::Read;
    /// use compress_io::compress::CompressIo;
    ///
    /// fn main() -> std::io::Result<()> {
    ///   let mut rd = CompressIo::new().path("foo.xz").reader()?;
    ///   let mut contents = String::new();
    ///   let len = rd.read_to_string(&mut contents)?;
    ///   println!("{} bytes read from file", len);
    ///   Ok(())
    /// }
    /// ```
    pub fn reader(&self) -> io::Result<Reader> {
        let mut buf = CheckBuf::default();
        let filter = Filter::new_decompress_filter(check_read_ctype(
            self.path.as_ref(),
            self.ctype,
            Some(&mut buf),
        )?)?;
        filter.reader(self.path.as_ref(), buf)
    }

    /// Generates a [`BufReader`] instance using the supplied settings.  This will return [`io::Error`]
    /// on failure which could be due to various reasons such as the source file not existing or
    /// not being accessible, or a suitable utility for decompressing not being available in the
    /// user's `$PATH`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::io::BufRead;
    /// use compress_io::compress::CompressIo;
    ///
    /// fn main() -> std::io::Result<()> {
    ///   let mut rd = CompressIo::new().path("foo.xz").bufreader()?;
    ///   let mut line = String::new();
    ///   let len = rd.read_line(&mut line)?;
    ///   println!("First line has {} bytes", len);
    ///   Ok(())
    /// }
    /// ```
    pub fn bufreader(&self) -> io::Result<BufReader<Reader>> {
        self.reader().map(BufReader::new)
    }

    /// Generates a [`Write'] instance using the supplied settings.  This will return [`io::Error`]
    /// on failure which could be due to various reasons such as the destination not existing or
    /// not being writable, or a suitable utility for the requested compression not being available
    /// in the user's `$PATH`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::io::Write;
    /// use compress_io::compress::CompressIo;
    ///
    /// fn main() -> std::io::Result<()> {
    ///   let mut wrt = CompressIo::new().path("foo.bz2").writer()?;
    ///   writeln!(wrt, "Hello world")?;
    ///   Ok(())
    /// }
    /// ```
    pub fn writer(&self) -> io::Result<Writer> {
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
        filter.writer(self.path.as_ref(), self.fix_path, self.no_wait)
    }

    /// Generates a [`BufWriter'] instance using the supplied settings.  This will return
    /// [`io::Error`] on failure which could be due to various reasons such as the destination not
    /// existing or not being writable, or a suitable utility for the requested compression not
    /// being available in the user's `$PATH`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::io::Write;
    /// use compress_io::compress::CompressIo;
    ///
    /// fn main() -> std::io::Result<()> {
    ///   let mut wrt = CompressIo::new().path("foo.zst").bufwriter()?;
    ///   writeln!(wrt, "Hello world")?;
    ///   Ok(())
    /// }
    /// ```
    pub fn bufwriter(&self) -> io::Result<BufWriter<Writer>> {
        self.writer().map( BufWriter::new)
    }
}
