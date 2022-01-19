#! /usr/bin/env nix-shell
#! nix-shell -i bash --pure --keep PGPASSFILE --keep PGHOST --keep PGPORT --keep PGUSER -p glibcLocales postgresql lsof procps
# shellcheck shell=bash

cd cardano-db-sync

which psql

#nix-build -A cardano-db-sync -o db-sync-node

#ls -l
#PGPASSFILE=config/pgpass-shelley_qa


export PATH="/nix/store/jdvs7vad2l2z3fvkc9gwypsqvp159hgg-postgresql-11.13/bin:$PATH"

PGPASSFILE=$PGPASSFILE db-sync-node/bin/cardano-db-sync --config config/shelley-qa-config.json --socket-path ../cardano-node/db/node.socket --schema-dir schema/ --state-dir ledger-state/shelley_qa >> logfile.log
