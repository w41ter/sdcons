<img src="images/sdcons.png" alt="logo" width="300"/>

# sdcons

[![Build Status](https://travis-ci.com/PatrickNicholas/sdcons.svg?branch=master)](https://travis-ci.com/PatrickNicholas/sdcons)


`sdcons` is an implementation of geo-replicated distributed consensus algorithm. It is based on the paper:

> SDPaxos: Building Efficient Semi-Decentralized Geo-replicated State Machines (ACM Symposium on Cloud Computing 2018, SoCC '18)

Like original SDPaxos, `sdcons` provides one-round-trip latency reading and writing capabilities under realistic configurations (deployed with three or five nodes).

In addition, I ported some optimizations mentioned in the raft algorithm to SDPaxos. Including:

- log term
- leader election
- leadership transfering
- membership configuration (joint consensus)
- batched & pipelined log replication
- lease reading

These optimizations make SDPaxos easier to implement and verify.

However, the current `sdcons` has not yet implemented the **Straggler detection** mentioned in the SDPaxos paper.

Unlike SDPaxos, `sdcons` uses the term `channel` to represent the paxos commiting progress. The Ordering instance is represented as Index, the Command instance is represented as Entry. Because the term index is used to represent Ordering instance, I choose term id to represent the index in raft.

At present, sdcons has not performed benchmarking and related performance optimization.

## How to use

See `examples/`.

## Acknowledgments

Thanks etcd for providing the amazing Go implementation of raft!
