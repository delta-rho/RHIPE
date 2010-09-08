Protobuffer and R
=================

A package called rprotobuf which implements a simple serialization using Googles
protocol buffers[1].  The package also includes some miscellaneous functions for
writing/reading variable length encoded integers, and Base64 encoding/decoding
related functions.  The package can be downloaded from
http://ml.stat.purdue.edu/rpackages/rprotobuf_1.1.tar.gz . It requires one to install ``libproto``
(Googles protobuffer library)

**Requirements**

Google's Protocol Buffer library. See [1].

**Brief Description**

The R objects that can be serialized are numerics,complex,integers,strings, logicals,
raw,nulls and lists.  Attributes of the aforementioned are preserved. NA is also
preserved(for the above) As such, the objects include factors and matrices.  The proto file can be
found in the source.

Serialization/deserialization works perfectly for these types.

**Extras**

With version 1.1, ``rprotobuf`` will now serialize

     - SYMSXP,
     - LISTSXP
     - CLOSXP
     - ENVSXP ( no locking )
     - PROMSXP
     - LANGSXP
     - DOTSXP
     - S4SXP
     - EXPRSXP

Serialization/deserialization(for these extras SEXP types)  *appear* to work but I cannot prove that (one with a thorough knowledge of R internals needs to audit the code( ``src/message.cc`` )). They remain undocumented in the help pages.   


**Regrets**

``serialize.c`` (in R 2.9 sources) uses a hashtable to add references to previously added environments and symbols(instead of adding them again). This reduces the size of the serialized expresson. ``rprotobuf`` does not do any such thing. It ought to and in future it will.


**Download**


Package(with source) : http://ml.stat.purdue.edu/rpackages/rprotobuf_1.1.tar.gz

Install ::
    
    R CMD INSTALL rprotobuf_1.1.tar.gz


[1] http://code.google.com/apis/protocolbuffers/docs/overview.html
