# [signal](../README.md) code structure and todo
> PubSub/Get index market data for trading using QuestDB

## source code structure
| module                         | description          |
| ------------------------------ | -------------------- |
| [execution binary](./main.rs)  | runtime              |
| [api](./src/api)               | REST/WS API          |
| [adapter](./adapter/README.md) | external API adapter |
| [error](./src/error.rs)        | custom error         |

## todo
- AsyncAPI compatible API for WS
- selection reconstruction of Signal struct
- upsert data on QuestDB (use dedup instead)
- configurable filters (persistent)
- basic chart plotting on Grafana
- set up historic/streaming data types
- add support to [mindsdb](https://mindsdb.com/) analysis
- add a place holder for mathematical analysis on top of data

[changelog here](../CHANGELOG.md)
