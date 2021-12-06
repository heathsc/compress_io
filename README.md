# compress_io
Convenience library for reading and writing compressed files/streams

The aim of `compress_io` is to make
it simple for an application to support multiple compression formats with a minimal effort
from the developer and also from the user (i.e., an application can accept uncompressed
or compressed input in a range of different formats and neither the developer nor the user
have to specify which formats have been used).  `compress_io` does not provide the compression/decompression itself but uses external utilities
such as [gzip], [bzip2] or [zstd] as read or write filters.

* [Documentation](https://docs.rs/compress_io)
* [Usage](https://docs.rs/compress_io#usage)

[gzip]: http://www.gzip.org/
[bzip2]: https://sourceware.org/bzip2/
[zstd]: https://facebook.github.io/zstd/
