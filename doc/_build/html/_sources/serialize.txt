.. highlight:: r
   :linenothreshold: 5

.. _rhipeserialize:

*********************
 RHIPE Serialization
*********************

About
=====
.. index:: serialization

The R serialization is verbose. Serialized objects have 22 bytes of header and
booleans are serialized to integers.  Best performance is achieved in Hadoop
when the size of the data exchanged is as small as possible. RHIPE implements
its own serialization using Google's `Protocol Buffers
<http://http://code.google.com/p/protobuf/>`_ . A benefit of using this is that
the data produced by RHIPE can be read in languages such as Python, C and Java
using the wrappers provided on the Google website.

However, a drawback of RHIPE's serialization is that not all R objects can be
seamlessly serialized. RHIPE can serialize the following

* Scalar vectors: integers, characters (including UTF8 strings), numerics,
  logicals, complex and raw. ``NA`` values are accepted.
* Lists of the above.
* Attributes of objects. RHIPE can serialize data frames, factors, matrices (including others like time series objects) since these are the above data structure with attributes.

Closures, environments and promises cannot be serialized. For example, to
serialize the output of ``xyplot``, wrap it in a call to ``serialize`` e.g.
::

	rhcollect(key, serialize(xyplot(a~b), NULL))



.. _serializationstringify:

.. index:: serialization;string representations, textouput;writing to text, rhmr

String Representations and TextOutput Format
============================================

RHIPE provides string representations of the above objects and is used when the
output format in ``rhmr`` is *text*. The stringfying rules expand all scalar
vectors and write them out as a line separated by
*mapred.field.separator*. Thus the vector ``c(1,2,3)`` is written out as
*1,2,3* if the value of *mapred.field.separator* is ",". The default value is
``SPACE``. Strings are surrounded by *rhipe_string_quote* (default is double
quote, to not surround strings set this to ''). Lists have their elements
written out consecutively on a single line.

In the text output format, keys are written if *mapred.textoutputformat.usekey*
is TRUE (default) and they are separated from the value by
*mapred.textoutputformat.separator* (default is ``TAB``). The options can be
passed to RHIPE in the ``mapred`` parameter of ``rhmr``.


Proto File
==========
::

  option java_package = "org.godhuli.rhipe";
  option java_outer_classname = "REXPProtos";
  message REXP {
    enum RClass {
      STRING = 0;
      RAW = 1;
      REAL = 2;
      COMPLEX = 3;
      INTEGER = 4;
      LIST = 5;
      LOGICAL = 6;
      NULLTYPE = 7;
    }
    enum RBOOLEAN {
      F=0;
      T=1;
      NA=2;
    }
  
    required RClass rclass = 1 ; 
    repeated double  realValue      = 2 [packed=true];
    repeated sint32  intValue       = 3 [packed=true];
    repeated RBOOLEAN booleanValue   = 4;
    repeated STRING  stringValue    = 5;
  
    optional bytes  rawValue      = 6;
    repeated CMPLX   complexValue   = 7;
    repeated REXP	   rexpValue	  = 8;
  
    repeated string attrName = 11;
    repeated REXP  attrValue   = 12;
  }
  message STRING {
    optional string strval = 1;
    optional bool isNA = 2 [default=false];
  }
  message CMPLX {
    optional double real = 1 [default=0];
    required double imag = 2;
  }





