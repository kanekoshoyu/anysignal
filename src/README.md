# [signal](../README.md) code structure and todo
> PubSub/Get index market data for trading

## source code structure
| module                         | description          |
| ------------------------------ | -------------------- |
| [execution binary](./main.rs)  | runtime              |
| [api](./src/api)               | REST/WS API          |
| [adapter](./adapter/README.md) | external API adapter |
| [error](./src/error.rs)        | custom error         |

## todo
- AsyncAPI compatible API for WS
- modular 3rd party API code structure
- SignalGenerator trait
- configurable filters (persistent)
- database connection (mariaDB)

[changelog here](../CHANGELOG.md)
