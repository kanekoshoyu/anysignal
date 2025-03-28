# AnySignal Indexer
> PubSub/Get index market data for trading

A trading **strategy** generates **instances** each consisting **legs** based on **signals**.  
This project first focuses on 
- setting up framework for gathering signals, data visualization and backtesting via QuestDB
- study even driven market patterns based on correlation between market scenario, events and prices 

## signals available
| signal                            | purpose                                                 | type   | source                                                           | status  |
| --------------------------------- | ------------------------------------------------------- | ------ | ---------------------------------------------------------------- | ------- |
| crypto fear and greed index       | to study market sentiment on crypto                     | scalar | [coinmarketcap](https://pro.coinmarketcap.com)                   | ready   |
| bitcoin dominance index           | to study market sentiment on  BTC vs altcoin            | scalar | [coinmarketcap](https://pro.coinmarketcap.com)                   | WIP     |
| new token listing                 | to study new coin enlisting behaviour                   | text   | [coinmarketcap](https://pro.coinmarketcap.com)                   | WIP     |
| memecoin price                    | to study market sentiment on memecoin                   | scalar | [dexscreener](https://docs.dexscreener.com/api/reference)        | WIP     |
| YouTube live video closed caption | to obtain fist hand news events                         | text   | [youtube_data_v3](https://developers.google.com/youtube/v3)      | WIP     |
| news titles                       | to obtain generalized news events                       | text   | [newsapi](https://newsapi.org)                                   | WIP     |
| stock market orderbook            | to index stock market orderbook                         | text   | [polygonio](https://polygon.io)                                  | WIP     |
| news and market sentiments        | to study relationship between news and market sentiment | scalar | [alphavantage](https://www.alphavantage.co)                      | planned |
| microstrategy btc holding         | to index real time MSTR BTC holdings                    | text   | [strategy](https://www.strategy.com/purchases)                   | WIP     |
| US treasury yields                | to gather yield curve data for macro market analysis    | scalar | [ustreasury](https://fiscaldata.treasury.gov/api-documentation/) | WIP     |
| SEC filings                       | to index insider trading                                | scalar | [secapi](https://sec-api.io/docs/insider-ownership-trading-api/) | WIP     |

## setup
1. copy config_sample.toml as config.toml, add API keys into the config.toml
2. set up `questdb` and `grafana` docker containers within same network
```
// create a new network
docker network create grafana-questdb-network
```
```
// either create new image with network directly
docker run --name signal-grafana --network grafana-questdb-network -p 3030:3000 -d grafana/grafana-oss
docker run --name signal-questdb --network grafana-questdb-network -p 8812:8812 -p 9000:9000 -p 9008:9008 -d questdb/questdb
```
```
// or connect existing container to the network
docker network connect grafana-questdb-network signal-grafana
docker network connect grafana-questdb-network signal-questdb
```
when docker containers are connected to the same network, the hostname will be the container name. `signal-questdb` in this case.  

3. connect grafana with questdb
add questdb plugin, add datasource, configure as below
- host: `signal-questdb:8812` // hostname is container name in same network
- username: `admin`
- password: `quest`

## see also
- [code structure and todo](./src/README.md)
- [changelog](./CHANGELOG.md)
