import argparse
import json
import os
import platform
import random
import re
import signal
import subprocess
import tarfile
import shutil
import requests
import time
import urllib.request
import zipfile
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from git import Repo

from psutil import process_iter
from utils import seconds_to_time, date_diff_in_seconds, get_no_of_cpu_cores, \
    get_current_date_time, get_os_type, get_directory_size, get_total_ram_in_GB


NODE = "./cardano-node"
CLI = "./cardano-cli"
ROOT_TEST_PATH = ""
NODE_LOG_FILE = "logfile.log"
DB_SYNC_LOG_FILE = "logfile.log"

MAINNET_EXPLORER_URL = "https://explorer.cardano.org/graphql"
STAGING_EXPLORER_URL = "https://explorer.staging.cardano.org/graphql"
TESTNET_EXPLORER_URL = "https://explorer.cardano-testnet.iohkdev.io/graphql"
SHELLEY_QA_EXPLORER_URL = "https://explorer.shelley-qa.dev.cardano.org/graphql"


def set_repo_paths():
    global ROOT_TEST_PATH
    ROOT_TEST_PATH = Path.cwd()
    print(f"ROOT_TEST_PATH: {ROOT_TEST_PATH}")


def create_dir(dir_name):
    Path(f"{ROOT_TEST_PATH}/{dir_name}").mkdir(parents=True, exist_ok=True)
    return f"{ROOT_TEST_PATH}/{dir_name}"


def clone_repo(repo_name, repo_branch):
    current_directory = os.getcwd()
    location = current_directory + f"/{repo_name}"
    repo = Repo.clone_from(f"git@github.com:input-output-hk/{repo_name}.git", location)
    repo.git.checkout(repo_branch)
    print(f"Repo: {repo_name} cloned to: {location}")
    return location


def get_node_archive_url(node_pr):
    cardano_node_pr=f"-pr-{node_pr}"
    return f"https://hydra.iohk.io/job/Cardano/cardano-node{cardano_node_pr}/cardano-node-linux/latest-finished/download/1/"


def get_db_sync_archive_url(db_pr):
    cardano_db_sync_pr=f"-pr-{db_pr}"
    return f"https://hydra.iohk.io/job/Cardano/cardano-db-sync{cardano_db_sync_pr}/cardano-db-sync-linux/latest-finished/download/1/"


def get_and_extract_archive_files(archive_url):
    current_directory = os.getcwd()
    request = requests.get(archive_url, allow_redirects=True)
    download_url = request.url
    archive_name = download_url.split("/")[-1].strip()

    print("Get and extract archive files:")
    print(f" - current_directory: {current_directory}")
    print(f" - download_url: {download_url}")
    print(f" - archive name: {archive_name}")

    urllib.request.urlretrieve(download_url, Path(current_directory) / archive_name)

    print(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    print(f" ------ listdir (after archive extraction): {os.listdir(current_directory)}")


def get_node_config_files(env):

    urllib.request.urlretrieve(
        "https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/"
        + env
        + "-config.json",
        env + "-config.json",
        )
    urllib.request.urlretrieve(
        "https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/"
        + env
        + "-byron-genesis.json",
        env + "-byron-genesis.json",
        )
    urllib.request.urlretrieve(
        "https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/"
        + env
        + "-shelley-genesis.json",
        env + "-shelley-genesis.json",
        )
    urllib.request.urlretrieve(
        "https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/"
        + env
        + "-alonzo-genesis.json",
        env + "-alonzo-genesis.json",
        )
    urllib.request.urlretrieve(
        "https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/"
        + env
        + "-topology.json",
        env + "-topology.json",
        )


def set_node_socket_path_env_var():
    socket_path = 'db/node.socket'
    os.environ["CARDANO_NODE_SOCKET_PATH"] = str(socket_path)


def get_testnet_value():
    env = vars(args)["environment"]
    if env == "mainnet":
        return "--mainnet"
    elif env == "testnet":
        return "--testnet-magic 1097911063"
    elif env == "staging":
        return "--testnet-magic 633343913"
    elif env == "shelley_qa":
        return "--testnet-magic 3"
    else:
        return None


def get_node_version():
    try:
        cmd = CLI + " --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
        )
        cardano_cli_version = output.split("git rev ")[0].strip()
        cardano_cli_git_rev = output.split("git rev ")[1].strip()
        return str(cardano_cli_version), str(cardano_cli_git_rev)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def get_current_tip(timeout_seconds=10):
    CWD = os.getcwd()
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    cmd = CLI + " query tip " + get_testnet_value()

    for i in range(timeout_seconds):
        try:
            output = (
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                    .decode("utf-8")
                    .strip()
            )
            output_json = json.loads(output)
            print(output_json)
            os.chdir(CWD )
            if output_json["epoch"] is not None:
                output_json["epoch"] = int(output_json["epoch"])
            if "syncProgress" not in output_json:
                output_json["syncProgress"] = None
            else:
                output_json["syncProgress"] = int(float(output_json["syncProgress"]))

            return output_json["epoch"], int(output_json["block"]), output_json["hash"], \
                   int(output_json["slot"]), output_json["era"].lower(), output_json["syncProgress"]
        except subprocess.CalledProcessError as e:
            print(f" === Waiting 60s before retrying to get the tip again - {i}")
            print(f"     !!!ERROR: command {e.cmd} return with error (code {e.returncode}): {' '.join(str(e.output).split())}")
            if "Invalid argument" in str(e.output):
                exit(1)
            pass
        time.sleep(60)
    exit(1)


def wait_for_node_to_start():
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take >10 mins for the cli to work (opening db and
    # replaying the ledger)
    start_counter = time.perf_counter()
    get_current_tip(18000)
    stop_counter = time.perf_counter()

    start_time_seconds = int(stop_counter - start_counter)
    print(f" === It took {start_time_seconds} seconds for the QUERY TIP command to be available")
    return start_time_seconds


def start_node(env):
    current_directory = Path.cwd()
    print(f"current_directory: {current_directory}")
    cmd = (
        f"{NODE} run --topology {env}-topology.json --database-path "
        f"{Path(ROOT_TEST_PATH) / 'cardano-node' / 'db'} "
        f"--host-addr 0.0.0.0 --port 3000 --config "
        f"{env}-config.json --socket-path ./db/node.socket"
    )

    logfile = open(NODE_LOG_FILE, "w+")
    print(f"start node cmd: {cmd}")

    try:
        p = subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
        print("waiting for db folder to be created")
        count = 0
        count_timeout = 299
        while not os.path.isdir(current_directory / "db"):
            time.sleep(1)
            count += 1
            if count > count_timeout:
                print(
                    f"ERROR: waited {count_timeout} seconds and the DB folder was not created yet")
                exit(1)

        print(f"DB folder was created after {count} seconds")
        secs_to_start = wait_for_node_to_start()
        print(f" - listdir current_directory: {os.listdir(current_directory)}")
        print(f" - listdir db: {os.listdir(current_directory / 'db')}")
        return secs_to_start
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def stop_node():
    for proc in process_iter():
        if "cardano-node" in proc.name():
            print(f" --- Killing the `cardano-node` process - {proc}")
            proc.send_signal(signal.SIGTERM)
            proc.terminate()
            proc.kill()
    time.sleep(20)
    for proc in process_iter():
        if "cardano-node" in proc.name():
            print(f" !!! ERROR: `cardano-node` process is still active - {proc}")


def setup_postgres():
    CWD = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    os.environ["PGHOST"] = 'localhost'
    os.environ["PGUSER"] = 'postgres'
    os.environ["PGPORT"] = '5432'

    try:
        cmd = "./scripts/postgres-start.sh '/tmp/postgres' -k"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        print(f"Setup postgres script output: {output}")
        os.chdir(CWD)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def start_db_sync():
    CWD = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    os.environ["environment"] = vars(args)["environment"]

    try:
        cmd = "./scripts/create_database.sh"
        p = subprocess.Popen(cmd)
        os.chdir(CWD)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )
    not_found = True
    counter = 0
    while (not_found):
        if counter > 10:
            print(f"ERROR: waited {counter} seconds and the db-sync was not started")
            exit(1)

        for proc in process_iter():
            if "cardano-db-sync" in proc.name():
                print(f"db-sync process present: {proc}")
                not_found = False
                return
        print("Waiting for db-sync to start")
        counter += 1
        time.sleep(3)


def get_db_sync_version():
    try:
        cmd = "db-sync-node/bin/cardano-db-sync --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
        )
        print("cardano-db-sync version :")
        print(output)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def get_db_sync_progress():
    env = vars(args)["environment"]
    os.environ["PATH"] = "/nix/store/jdvs7vad2l2z3fvkc9gwypsqvp159hgg-postgresql-11.13/bin:$PATH"
    p = subprocess.Popen(["psql", "-P", "pager=off", "-qt", "-U", "postgres", "-d", f"{env}",  "-c", "select 100 * (extract (epoch from (max (time) at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC'))) / (extract (epoch from (now () at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC'))) as sync_percent from block ;" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        outs, errs = p.communicate(timeout=5)
        return outs.decode("utf-8")
    except TimeoutExpired as e:
        p.kill()
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

def wait_for_db_to_sync():
    start_sync = time.perf_counter()
    db_sync_progress = float(get_db_sync_progress())

    count = 0
    while db_sync_progress < 99.99:
        if count % 60 == 0:
            get_current_tip()
            print(f"db sync progress : {db_sync_progress}")
            db_sync_progress = float(get_db_sync_progress())
        time.sleep(1)
        count += 1
        #actual_epoch, actual_block, actual_hash, actual_slot, actual_era,syncProgress = get_current_tip()


def main():

    start_test_time = get_current_date_time()
    print(f"Test start time: {start_test_time}")

    set_repo_paths()

    NODE_DIR=create_dir("cardano-node")
    os.chdir(NODE_DIR)

    env = vars(args)["environment"]
    print(f"env: {env}")

    set_node_socket_path_env_var()

    node_tag = str(vars(args)["node_tag"]).strip()
    print(f"Node PR number: {node_tag}")


    platform_system, platform_release, platform_version = get_os_type()
    print(f"Platform: {platform_system, platform_release, platform_version}")

    get_node_config_files(env)
    get_and_extract_archive_files(get_node_archive_url(node_tag))


    print(f"====================== Start node: ========================")

    cli_version, cli_git_rev = get_node_version()
    print(f"- cardano-cli version: {cli_version}")
    print(f"- cardano-cli git revision: {cli_git_rev}")

    node_start_time = get_current_date_time()
    start_delay_in_secs = start_node(env)

    print("Node is syncing ...")

    setup_postgres()
    os.chdir(ROOT_TEST_PATH)
    DB_SYNC_DIR = clone_repo("cardano-db-sync", "tags/12.0.1-pre1")
    os.chdir(DB_SYNC_DIR)
    start_db_sync()
    get_db_sync_version()

    file_o=open(DB_SYNC_LOG_FILE)
    content=file_o.read()
    print(content)
    file_o.close()



    db_sync_progress = float(get_db_sync_progress())
    print(f"Progress: {db_sync_progress}")
    wait_for_db_to_sync()


    print(f"   =============== Stop node: ======================")
    stop_node()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute basic sync test\n\n")

    parser.add_argument(
        "-nt", "--node_tag", help="node tag - used for sync from clean state"
    )
    parser.add_argument(
        "-dt", "--db_sync_pr", help="db-sync pr number"
    )
    parser.add_argument(
        "-e",
        "--environment",
        help="the environment on which to run the tests - shelley_qa, testnet, staging or mainnet.",
    )

    args = parser.parse_args()

    main()
