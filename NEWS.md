Version 0.7.5.1
----------------------------------------------------------------------

- update all deprecated hadoop api calls
- update PersonalServer `ls` to handle non-default Hadoop URIs differently
- add `_meta` to ignored RHIPE files

Version 0.7.5
----------------------------------------------------------------------

- fix build.xml to be more linux-friendly
- `rherrors` can read output of `rhwatch(..., read=FALSE)` to read the errors folder using the new dump frames options
- `rhJobInfo`, `rhstatus` and `rhwatch` correctly get the jobid (uses the Java MapReduce API)
- `rhstatus` (inside `FileUtils`) to return jobid (no need to use regex parsing)
- version 0.75 will be Hadoop 2 compatible only
- start NEWS.md file (see git log for all previous history)
