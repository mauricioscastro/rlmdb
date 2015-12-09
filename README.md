#rlmdb
####Replicated LMDB: a [lmdb](http://symas.com/mdb/) + [bookkeeper](http://bookkeeper.apache.org) experiment

after finishing the initial version of [basex-lmdb](https://github.com/mauricioscastro/basex-lmdb) I figured 
single inserts resulting from XQuery updates faces long delays when huge bulks are being created (new large XML documents).

allied to this there's the idea of replicating basex data (now laying over lmdb), so after trying some options 
at hand like jgroups-raft, kafka and bookkeeper I will give bookkeeper a go for replicating the lmdb key/values. 

**this will be done for basex-lmdb specifically**.

the idea around bookkeeper's ledgers is that each represents a write transaction and its entries represents 
this transaction's operations originated in the leader node.


