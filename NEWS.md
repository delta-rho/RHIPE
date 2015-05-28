Version 0.75.1.6
----------------------------------------------------------------------

- provide serialization for simple environments
- update so that output to non-standard file URIs like s3 work in `rhwatch()`
- update build script
- update to be compatible with R 3.2
- update to handle nulls in input text files using modified utf-8
- update `readTextFile` to  be more flexible with file URI
- updated RHWrite and RHSequenceFileIterator to be more flexible with file URI
- as a consequence file reading/writing functions will work with file:// and s3:// URIs
- updated the R functions (e.g. rhcp/rhls/rhmkdir etc) to not depend on rhoptions()$clz$filesystem which has been removed)
- updated the test functions to work with this new method
- library.dynam nows makes internal C functions public and we made collect/counter/status extern "C" so other C modules can use them
- update all deprecated hadoop api calls
- update PersonalServer `ls` to handle non-default Hadoop URIs differently
- add `_meta` to ignored RHIPE files

Version 0.75.1
----------------------------------------------------------------------

- fix build.xml to be more linux-friendly
- `rherrors` can read output of `rhwatch(..., read=FALSE)` to read the errors folder using the new dump frames options
- `rhJobInfo`, `rhstatus` and `rhwatch` correctly get the jobid (uses the Java MapReduce API)
- `rhstatus` (inside `FileUtils`) to return jobid (no need to use regex parsing)
- version 0.75 will be Hadoop 2 compatible only
- start NEWS.md file (see git log for all previous history)
