#lldb
####a [lmdb](http://symas.com/mdb/) + [leveldb](https://github.com/google/leveldb) + [bookkeeper replication](http://bookkeeper.apache.org) experiment

this is too a way to start catching up with [gradle](http://gradle.org/).

after finishing the initial version of [basex-lmdb](https://github.com/mauricioscastro/basex-lmdb) I figured 
single inserts resulting from XQuery updates faces long delays when huge bulks are being created (new large XML documents).

allied to this there's the idea of replicating basex data (now laying over lmdb), so after trying some options 
at hand like jgroups-raft, kafka and bookkeeper I will give bookkeeper a go and put these 3 together to 
be used in basex-lmdb. 

in this case I will have leveldb bufering single inserts and small sized write batches and also to avoid 
stale reads while replicating through bookkeeper ledgers. this will be done for basex-lmdb specifically, 
the idea follows in a simple draft image.

![lldb](https://raw.githubusercontent.com/mauricioscastro/lldb/gh-pages/images/lldb_idea.png)


