# PMXT Relay Legacy Archive

This folder preserves the older full-stack PMXT relay design.

That design is no longer the active public deployment path for this repository.
The live `pmxt_relay/` service is mirror-only and does not do server-side
filtered processing.

Use this archive only if you want to run the older processed-data service on
infrastructure you control.

## What This Archive Was For

The old relay architecture did three things on one box:

1. mirror raw PMXT archive parquet hours
2. process those raw hours into filtered per-market data
3. serve the processed data quickly to repeated users

That pattern is still useful in one specific case: you have your own
storage-rich machine and want a fast private processed-data service for
yourself or your team.

## Good Fit For This Archive

- a Linux server, NAS-backed box, or storage-heavy workstation you control
- one team sharing the same processed PMXT data repeatedly
- willingness to own ClickHouse, indexing, storage layout, and operational
  debugging yourself

Example of the kind of storage-rich self-hosted environment this archive fits:

![NAS-style self-hosted infrastructure](https://cdn.mos.cms.futurecdn.net/aJimJfHgV2uck6KefQMFzf.jpg)

If you revive this design on your own infrastructure, the benefit is speed for
repeated team pulls. The expensive processing work happens once on your box,
then your users read the already-processed results quickly.

## What It Is Not

This archive is not the active `pmxt_relay/` deployment.

Do not read the archived architecture as the current public recommendation for
this repo. The supported public direction is:

- raw mirror on the server
- local processing on user machines
- local filtered cache for repeat runs

## Recommended Alternative If You Do Not Want To Host This

If you do not want to operate a storage-heavy processed-data service, keep the
workflow local-first instead:

- download the raw PMXT dumps to a local drive
- process them locally
- keep your own filtered cache warm

A large external drive is usually enough:

![External drive for local PMXT dumps](https://www.digitaltrends.com/tachyon/2017/03/Lacie-Rugged-4gb-HD-inhandscale.jpg?resize=1200%2C720)

That path is simpler, cheaper, and is the current recommended workflow for this
repository.

## Accuracy Notes

- this folder is a historical snapshot
- it documents an older ClickHouse-backed filtered-serving design
- commands and layouts here should be treated as self-hosted reference
- if you want a live mirror-only deployment, use `pmxt_relay/` instead

The active docs for the current mirror service live in:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
- [`docs/pmxt-relay.md`](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-relay/)
