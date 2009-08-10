FAQ
===

1. Local Testing?

Easily enough. In ``rhmr`` or ``rhlapply``, set ``mapred.job.tracker`` to
'local' in the ``hadoop.options`` option of the respective command. This will
use the local jobtracker to run your commands. 

However keep in mind,
``shared.files`` will not work, i.e those files will not be copied to the
working directory and side effect files will not be copied back.


2. Speed?

Not so good. A lot of data is transferred and because of R's serialization and
my effort to allow *any* R data type to be transferred, about 6 times more data
is transferred in the RHIPE wordcount example(see `Mainpage <../index.html>`_ )
compared to the Java version. As such, this was about 5 times slower for 9MM
lines.

3. Other Approaches?

- I will try using Unix pipes to feed data to R instead of Rserve.
- Also, I will just settle for raw,numeric, complex, character vectors and lists
  (whose elements are one of the aforementioned types) in conjunction with
  Protocol Buffers. This does reduce the transparency to the R user in that
  he/she will now have to serialize other structures (e.g objects like ``xyplot``
  objects or objects with special attributes
