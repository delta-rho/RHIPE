Datatypes
=========

I have tried to make RHIPE as flexible as possible with regards to data types exchanged. As such the following can be sent 

- atomic vectors without attributes (raw, character, logical, complex, real, integer) (these include NA's)
   - e.g names attribute will be lost

- lists of the above and lists of lists. Names will be preserved.


So *officially* no matrices, data.frames, time series objects etc. If there are needed serialized them using ``serialize`` .

*Unofficially*, several attributes of are serialized, hence you can send matrices (the dim and  dimnames attribute are involved) will be serialized.
However, with regards to data.frames the row.names attribute has caused me much grief. Hence, relying on these types can lead to many problems, crashes even.

If in doubt, serialize.



