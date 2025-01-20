# [signal](../README.md) code structure and todo
> PubSub/Get index market data for trading powered by QuestDB

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
- upsert data on QuestDB
- configurable filters (persistent)
- basic chart plotting on Grafana

[changelog here](../CHANGELOG.md)
