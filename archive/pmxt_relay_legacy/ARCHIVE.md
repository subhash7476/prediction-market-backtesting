# Archived PMXT Relay

This folder is a snapshot of the older full-stack PMXT relay implementation.

It is retained for reference and for users who still want the previous
server-side processing and filtered-serving design on infrastructure they own.

The active relay path now lives in `pmxt_relay/` and is mirror-focused.

Treat everything in this folder as a self-hosted archive pattern:

- mirror raw PMXT hours
- process them on your own box
- serve the processed outputs quickly to yourself or your team

If you do not want to run that kind of storage-heavy service, the recommended
path in this repo is the local-first workflow:

- keep raw dumps on a local drive
- process them locally
- use the active `pmxt_relay/` only as a raw mirror and raw file server
