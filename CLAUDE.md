# AnySignal — Claude Guide
Rust async service that backfills and streams market data into QuestDB.

## Deployment
Docker via Coolify.

Configuration
All config comes from:

Config::from_env()

Never read environment variables directly in business logic.


## Project Structure
src/main.rs Service entrypoint. Spawns runners from RUNNERS env variable.
src/config.rs Configuration struct.
src/adapter/ External data source integrations.
src/backfill/ Historic ingestion jobs.
src/database/ QuestDB ingestion layer.
src/engine/ Realtime market state engine.
src/api/rest/ Poem REST API.


## Data Flow
Adapter → Backfill → Database → QuestDB


## Adding a Backfill Source
1. Create adapter
src/adapter/<source>/<dataset>.rs

1. Add writer
src/database/mod.rs

insert_<dataset>(sender, rows)

3. Implement backfill
src/backfill/<dataset>.rs

Implement:
PartitionKey
PartitionedSource

4. Register module
src/backfill/mod.rs

5. Add API variant
src/api/rest/endpoint.rs


## QuestDB Notes
Timestamp: microseconds
Buffer flush threshold: 64 MiB
Schema: schema_questdb.json


## Rules
Do not use unwrap() in production.
Use ? for error handling.


## Tests
cargo test -- --ignored --nocapture

## Token Efficiency Rules
When working in this repository:

1. Search before opening files
2. Open only relevant files
3. Edit minimal regions
4. Do not rewrite entire files
5. Prefer patches over full file output
