# Signal MFT Indexer
> PubSub/Get index market data for trading

A trading **strategy** generates **instances** each consisting **legs** based on **signals**.

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
