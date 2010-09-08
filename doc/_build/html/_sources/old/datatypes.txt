Datatypes
=========

I have tried to make RHIPE as flexible as possible with regards to data types exchanged. As such the following can be sent 

- atomic vectors (raw, character, logical, complex, real, integer) (these include NA's)

- lists of the above and lists of lists. Names will be preserved.


So *officially* no matrices, data.frames, time series objects etc. If there are needed , serialize them using ``serialize`` .

*Unofficially*, several attributes of are serialized, hence you can send matrices (the dim and  dimnames attribute are involved) will be serialized.
However, with regards to data.frames the row.names attribute has caused me much grief. Keep it simple: scalar vectors (use ``as.vector`` to remove attributes) and lists (names are allowed).

I use data.frames too and it works. If you get a crash(error code 139,using version >= 0.53), email me.

If in doubt, serialize.



