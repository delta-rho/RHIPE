FAQ
===

1. Local Testing?

Easily enough. In ``rhmr`` or ``rhlapply``, set ``mapred.job.tracker`` to
'local' in the ``mapred`` option of the respective command. This will
use the local jobtracker to run your commands. 

However keep in mind,
``shared.files`` will not work, i.e those files will not be copied to the
working directory and side effect files will not be copied back.


2. Speed?

Similar to Hadoop Streaming. The bottlenecks are writing and reading to STDIN
pipes and R.



