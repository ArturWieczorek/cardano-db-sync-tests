#! /usr/bin/env nix-shell
#! nix-shell -i bash --pure --keep environment --keep PGHOST --keep PGPORT --keep PGUSER -p glibcLocales postgresql lsof procps
# shellcheck shell=bash

cd cardano-db-sync

export PGPASSFILE=config/pgpass-${environment}
echo "/tmp/postgres:5432:${environment}:postgres:*" > $PGPASSFILE
chmod 600 $PGPASSFILE

PGPASSFILE=$PGPASSFILE scripts/postgresql-setup.sh --createdb

nix-build -A cardano-db-sync -o db-sync-node

PGPASSFILE=$PGPASSFILE db-sync-node/bin/cardano-db-sync --config config/shelley-qa-config.json --socket-path ../cardano-node/db/node.socket --schema-dir schema/ --state-dir ledger-state/shelley_qa >> logfile.log &
