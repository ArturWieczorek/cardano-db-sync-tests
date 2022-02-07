#! /usr/bin/env nix-shell
#! nix-shell -i bash -p nix coreutils gnugrep gawk bc wget postgresql buildkite-agent

set -euo pipefail

ENTRY_DIR="$PWD"

node_logfile="logs/node_logfile.log"
db_sync_logfile="logs/db_sync_logfile.log"

function usage() {
    cat << HEREDOC
    arguments:
    -e          environment - possible options: mainnet, testnet, shelley_qa
    -p          pr number that contains desired version of node that is not available on latest master
    optional arguments:
    -h        show this help message and exit
Example:
./start_node.sh -e shelley_qa -p 3458
USE UNDERSCORES IN environment NAMES !!!
HEREDOC
}

while getopts ":h:e:p:" o; do
    case "${o}" in
        h)
            usage
            ;;
        e)
            environment=${OPTARG}
            ;;
        p)
            pr_no=${OPTARG}
            ;;
        *)
            echo "NO SUCH ARGUMENT: ${OPTARG}"
            usage
            ;;
    esac
done
if [ $? != 0 ] || [ $# == 0 ] ; then
    echo "ERROR: Error in command line arguments." >&2 ; usage; exit 1 ;
fi
shift $((OPTIND-1))

function get_network_param_value() {

	if [ "$environment" = "mainnet"  ]
	    then
	        echo "--mainnet"

	elif [ "$environment" = "testnet" ]
		then
	    	echo "--testnet-magic 1097911063"

	elif [ "$environment" = "staging" ]
		then
	    	echo "--testnet-magic 633343913"

	elif [ "$environment" = "shelley_qa" ]
		then
	    	echo "--testnet-magic 3"
	fi
}

function get_db_sync_progress() {
  local db_sync_progress=$(psql -P pager=off -qt -U postgres -d "${environment}" -c "select
     100 * (extract (epoch from (max (time) at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC')))
        / (extract (epoch from (now () at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC')))
    as sync_percent
    from block ;")
  echo ${db_sync_progress}
}

function get_db_sync_progress_integer() {
  local int_db_sync_progress=$(get_db_sync_progress)
  echo ${int_db_sync_progress%.*}
}

function get_db_sync_latest_epoch() {
  local db_sync_latest_epoch=$(psql -P pager=off -qt -U postgres -d "${environment}" -c "select no from epoch order by id desc limit 1;")
  echo ${db_sync_latest_epoch}
}

function get_db_sync_latest_block() {
  local db_sync_latest_block=$(psql -P pager=off -qt -U postgres -d "${environment}" -c "select block_no from block order by id desc limit 1;")
  echo ${db_sync_latest_block}
}

function get_node_tip_data() {
   local CWD="$PWD"
   cd $NODE_DIR
   export CARDANO_NODE_SOCKET_PATH="${environment}/node.socket"
	 ./cardano-cli query tip $(get_network_param_value)
   cd $CWD
}

function get_node_sync_progress() {
  local node_sync_progress=$(get_node_tip_data | jq .syncProgress | bc)
  echo ${node_sync_progress}
}

function get_node_sync_progress_integer() {
  local int_node_sync_progress=$(get_node_sync_progress)
  echo ${int_node_sync_progress%.*}
}

function get_node_era() {
  local era=$(get_node_tip_data | jq .era)
  echo $era
}

function get_node_hash() {
  local hash=$(get_node_tip_data | jq .hash)
  echo $hash
}

function get_node_epoch() {
  local epoch=$(get_node_tip_data | jq .epoch)
  echo $epoch
}

function get_node_slot() {
  local slot=$(get_node_tip_data | jq .slot)
  echo $slot
}

function get_node_block() {
  local block=$(get_node_tip_data | jq .block)
  echo $block
}

function get_latest_db_synced_epoch_from_logs() {

	local log_filepath=$1
	IN=$(tail -n 1 $log_filepath)
	preformated_string=$(echo "$IN" | sed  's/^.*[Ee]poch/epoch/') # this will return: "Starting epoch 1038"

	IFS=' ' read -ra ADDR <<< "$preformated_string"
	for i in "${!ADDR[@]}"; do
		if [[ "${ADDR[$i]}" == *"epoch"* ]]; then # This is index $i for epoch keyword - we know that epoch number has ${i+1) position
	    	epoch_number=$(echo "${ADDR[$((i+1))]}" | sed 's/,[[:blank:]]*$//g') # use sed to remove comma at the end of slot number
            echo "$epoch_number"
	    fi
	done
}

echo "We are here: ${PWD}, script name is $0"
echo ""
echo "Creating cardano-node directory and entering it ..."

mkdir cardano-node
cd cardano-node

mkdir logs

export NODE_DIR="$PWD"
NODE_PR=""

if [[ ! -z "$pr_no" ]]
then
      NODE_PR="-pr-${pr_no}"
fi

echo ""
echo "Downloading cardano-node & cli archive:"

wget -q --content-disposition "https://hydra.iohk.io/job/Cardano/cardano-node${NODE_PR}/cardano-node-linux/latest-finished/download/1/"
downloaded_archive=$(ls | grep tar)


echo ""
echo "Unpacking and removing archive ..."

tar -xf $downloaded_archive
rm $downloaded_archive

NODE_CONFIGS_URL=$(curl -Ls -o /dev/null -w %{url_effective} https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/index.html | sed 's|\(.*\)/.*|\1|')

echo ""
echo "Downloading node configuration files from $NODE_CONFIGS_URL for environments specified in script ..."
echo ""

# Get latest configs for environment(s) you need:

for _environment in ${environment}
do
	mkdir ${_environment}
	cd ${_environment}
	echo "Node configuration files located in ${PWD}:"
	wget -q  $NODE_CONFIGS_URL/${_environment}-config.json
	wget -q  $NODE_CONFIGS_URL/${_environment}-byron-genesis.json
	wget -q  $NODE_CONFIGS_URL/${_environment}-shelley-genesis.json
	wget -q  $NODE_CONFIGS_URL/${_environment}-alonzo-genesis.json
	wget -q  $NODE_CONFIGS_URL/${_environment}-topology.json
	wget -q  $NODE_CONFIGS_URL/${_environment}-db-sync-config.json
	echo ""
	cd ..
done

echo ""
ls -l $environment

echo ""
echo "Node version: "
echo ""
./cardano-node --version

echo ""
echo "CLI version: "
echo ""
./cardano-cli --version


echo ""
echo ""
echo "Starting node."

./cardano-node run --topology ${environment}/${environment}-topology.json --database-path ${environment}/db --socket-path ${environment}/node.socket --config ${environment}/${environment}-config.json >> $node_logfile &

CARDANO_NODE_PID=$!

sleep 1

cat $node_logfile

#sync_progress=$(get_node_sync_progress)
#while [ "$sync_progress" -lt "10" ]
#do
#	sleep 60
#  echo "Latest progress: $sync_progress"
#	sync_progress=$(get_node_sync_progress)
#done

cd $ENTRY_DIR

export PGHOST=localhost
export PGUSER=postgres
export PGPORT=5432

# start and setup postgres
./scripts/postgres-start.sh "/tmp/postgres" -k

# clone db-sync
git clone git@github.com:input-output-hk/cardano-db-sync.git

#DBSYNC_REV="release/12.0.x"

cd cardano-db-sync
mkdir logs
export DBSYNC_REPO="$PWD"

if [ -n "${DBSYNC_REV:-""}" ]; then
  git fetch
  git checkout "$DBSYNC_REV"
elif [ -n "${DBSYNC_BRANCH:-""}" ]; then
  git fetch
  git checkout "$DBSYNC_BRANCH"
else
  git pull origin master
fi
git rev-parse HEAD

# build db-sync
nix-build -A cardano-db-sync -o db-sync-node

export PGPASSFILE=config/pgpass-${environment}
echo "/tmp/postgres:5432:${environment}:postgres:*" > $PGPASSFILE

chmod 600 $PGPASSFILE
PGPASSFILE=$PGPASSFILE scripts/postgresql-setup.sh --createdb

db_sync_start_time=$(echo "$(date +'%d/%m/%Y %H:%M:%S')")  # format: 17/02/2021 23:42:12

if [ "$environment" = "shelley_qa" ];
then
    PGPASSFILE=$PGPASSFILE db-sync-node/bin/cardano-db-sync --config config/shelley-qa-config.json --socket-path ../cardano-node/shelley_qa/node.socket --schema-dir schema/ --state-dir ledger-state/shelley_qa >> $db_sync_logfile &
else
    PGPASSFILE=$PGPASSFILE db-sync-node/bin/cardano-db-sync --config config/${environment}-config.yaml --socket-path ../cardano-node/${environment}/node.socket --schema-dir schema/ --state-dir ledger-state/${environment} >> $db_sync_logfile &
fi

CARDANO_DB_SYNC_PID=$!

sleep 5
cat $db_sync_logfile

node_sync_progress=$(get_node_sync_progress_integer)
db_sync_progress=$(get_db_sync_progress_integer)

curl \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"query": "{ cardano { tip { number slotNo epoch { number } } } }"}' https://explorer.shelley-qa.dev.cardano.org/graphql

while [ "$node_sync_progress" -lt "100" ] || [ "$db_sync_progress" -lt "99" ] || [ "$db_sync_progress" -lt "99" ]
do
	sleep 60
  current_node_epoch=$(get_node_epoch)
  echo "Latest node epoch: $current_node_epoch"

  current_db_sync_epoch=$(get_db_sync_latest_epoch)
  echo "Latest db-sync epoch: $current_db_sync_epoch"

  node_sync_progress=$(get_node_sync_progress_integer)
  echo "Node sync progress: $(get_node_sync_progress) %"

  db_sync_progress=$(get_db_sync_progress_integer)
  echo "DB sync progress: $(get_db_sync_progress) %"
  echo ""
done

final_node_epoch=$(get_node_epoch)
echo "Latest node epoch: $final_node_epoch"
final_db_sync_epoch=$(get_db_sync_latest_epoch)
echo "Latest db-sync epoch: $final_db_sync_epoch"
final_node_sync_progress=$(get_node_sync_progress)
echo "Node sync progress: $final_node_sync_progress"
final_db_sync_progress=$(get_db_sync_progress)
echo "DB sync progress: $final_db_sync_progress"

kill -9 $CARDANO_NODE_PID
kill -9 $CARDANO_DB_SYNC_PID
