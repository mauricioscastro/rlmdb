#lldb
###a (lmdb)[http://symas.com/mdb/] + (leveldb)[https://github.com/google/leveldb] wrapper experiment

this is too a way to start cathing up with (gradle)[http://gradle.org/].

after finishing the initial version of [basex-lmdb](http://mauricioscastro.github.io/basex-lmdb/) I figured single inserts resulting from XQuery updates faces long delays when huge bulks are being created (large XML documents).

allied to this there's the idea of replicating basex data (now laying over lmdb), so after trying some options at hand like jgroups-raft, kafka and bookkeeper I will give bookkeeper a go and put these 3 together to be used in basex-lmdb. 

in this case I will have leveldb bufering single inserts an small sized write batches and also to avoid stale reads while replicating through bookkeeper ledgers. this will be done for basex-lmdb specifically, the idea follows in a simple draft image.


![lldb](https://raw.githubusercontent.com/mauricioscastro/basex-lmdb/master/www/img/lldb_idea.png)


