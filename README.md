# Banyan: A Task Distribution Library

![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rustc 1.32+](https://img.shields.io/badge/rustc-1.32+-lightgray.svg)
![Pipeline](https://gitlab.com/neachdainn/banyan/badges/master/pipeline.svg)

Banyan makes it easy to distribute long-running tasks across multiple processes, machines, or threads.
It takes advantage of [nng] to provide reliable connections between coordinator and worker nodes.
This also allows Banyan to be deployed in a wide variety of configurations via TLS, TCP, IPC, and intra-process communication channels.

### Rust Version Requirements

The current version requires **Rustc v1.32 or greater**.
In general, this crate should always be able to compile with the Rustc version available on the oldest Ubuntu LTS release.
Any change that requires a newer Rustc version will always be considered a breaking change and this crate's version number will be bumped accordingly.

[nng]: https://nanomsg.github.io/nng/
