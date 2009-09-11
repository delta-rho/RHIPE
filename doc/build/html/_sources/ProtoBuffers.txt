Protobuffer and R
=================

A package called rprotos which implements a simple serialization using Googles
protocol buffers[1].  The package also includes some miscellaneous functions for
writing/reading variable length encoded integers, and Base64 encoding/decoding
related functions.  The package can be downloaded from
http://ml.stat.purdue.edu/rproto_1.0.tar.gz it requires one to install libproto
(Googles protobuffer library)

*Brief Description*

The R objects that can be serialized are numerics,integers,strings, logicals,
raw,nulls and lists.  Attributes of the aforementioned are preserved. NA is also
preserved(for the above) As such, the objects include factors and matrices.  The proto file can be
found in the source.

*Todo*: 

Very possible though needs some reading on my part: it is possible to
extend this to serialize functions, expressions, environments and several
other objects.  However that is some time in the future.


*Download*
http://ml.stat.purdue.edu/rproto_1.0.tar.gz

[1] http://code.google.com/apis/protocolbuffers/docs/overview.html
