# AnySignal Indexer
> PubSub/Get index market data for trading

A trading **strategy** generates **instances** each consisting **legs** based on **signals**.  
This project first focuses on 
- setting up framework for gathering signals, data visualization and backtesting via QuestDB
- study even driven market patterns based on correlation between market scenario, events and prices 

## backfill endpoint
`POST /backfill` — fetch historic data from S3 into QuestDB for a date range.

| `source` | Description | Extra fields |
|---|---|---|
| `HyperliquidAssetCtxs` | Daily asset-context snapshots (`market_data` table) | — |
| `HyperliquidL2Orderbook` | Hourly L2 orderbook snapshots (`l2_snapshot` table) | `coins` (required), `hours` (0–23, default all) |

```json
// Example — backfill BTC & ETH orderbook for one week, peak hours only
{
  "from": "2024-01-01",
  "to": "2024-01-07",
  "source": "HyperliquidL2Orderbook",
  "coins": ["BTC", "ETH"],
  "hours": [0, 6, 12, 18]
}
```

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

## running with docker

### build
```sh
docker build -t anysignal .
```

### run
Copy `.env.example` to `.env`, fill in your values, then:
```sh
docker run --rm \
  --env-file .env \
  -p 3000:3000 \
  anysignal
```

Or pass env vars inline:
```sh
docker run --rm \
  -e QUESTDB_ADDR=host:9000 \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e RUNNERS=hyperliquid \
  -p 3000:3000 \
  anysignal
```

The REST API is available at `http://localhost:3000`.

To connect to QuestDB running in another container, add both to the same network:
```sh
docker network create anysignal-net
docker run --name signal-questdb --network anysignal-net -p 9000:9000 -d questdb/questdb
docker run --rm --network anysignal-net --env-file .env -e QUESTDB_ADDR=signal-questdb:9000 -p 3000:3000 anysignal
```


## testing

Unit tests (no credentials required):
```sh
cargo test
```

Integration / screening tests (require `.env` with real AWS credentials and outbound network access):
```sh
cargo test -- --ignored
```

## see also
- [code structure and todo](./src/README.md)
- [changelog](./CHANGELOG.md)
