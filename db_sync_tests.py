import argparse
import json
import os
from os.path import normpath, basename
import platform
import random
import re
import signal
import subprocess
import tarfile
import shutil
import gzip
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
    get_current_date_time, get_os_type, get_directory_size, get_total_ram_in_GB, \
    upload_artifact, clone_repo, print_file, stop_process, export_env_var, create_dir, zip_file


ROOT_TEST_PATH = Path.cwd()

NODE_LOG_FILE_PATH = f"{ROOT_TEST_PATH}/cardano-node/node_logfile.log"
DB_SYNC_LOG_FILE_PATH = f"{ROOT_TEST_PATH}/cardano-db-sync/db_sync_logfile.log"
TEST_RESULTS_FILE_NAME = 'test_results.json'
EPOCH_SYNC_TIMES_FILE_NAME = 'epoch_sync_times_dump.json'
EPOCH_SYNC_TIMES_FILE_PATH = f"{ROOT_TEST_PATH}/cardano-db-sync/{EPOCH_SYNC_TIMES_FILE_NAME}"


def get_environment():
    return vars(args)["environment"]


def get_node_pr():
    return str(vars(args)["node_pr"]).strip()


def get_db_sync_branch():
    return str(vars(args)["db_sync_branch"]).strip()


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
    base_url = "https://hydra.iohk.io/job/Cardano/iohk-nix/cardano-deployment/latest-finished/download/1/"
    urllib.request.urlretrieve(base_url + env + "-config.json",env + "-config.json",)
    urllib.request.urlretrieve(base_url + env + "-byron-genesis.json", env + "-byron-genesis.json",)
    urllib.request.urlretrieve(base_url + env + "-shelley-genesis.json", env + "-shelley-genesis.json",)
    urllib.request.urlretrieve(base_url + env + "-alonzo-genesis.json", env + "-alonzo-genesis.json",)
    urllib.request.urlretrieve(base_url + env + "-topology.json", env + "-topology.json",)


def set_node_socket_path_env_var_in_cwd():
    current_directory = Path.cwd()
    if not 'cardano-node' == basename(normpath(current_directory)):
        raise Exception(f"You're not inside 'cardano-node' directory but in: {current_directory}")
    socket_path = 'db/node.socket'
    export_env_var("CARDANO_NODE_SOCKET_PATH", socket_path)


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
        cmd = "./cardano-cli --version"
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


def get_node_tip(timeout_seconds=10):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    cmd = "./cardano-cli query tip " + get_testnet_value()

    for i in range(timeout_seconds):
        try:
            output = (
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode("utf-8").strip()
            )
            output_json = json.loads(output)
            print(output_json)
            os.chdir(current_directory)
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
    get_node_tip(18000)
    stop_counter = time.perf_counter()

    start_time_seconds = int(stop_counter - start_counter)
    print(f" === It took {start_time_seconds} seconds for the QUERY TIP command to be available")
    return start_time_seconds


def start_node_in_cwd(env):
    current_directory = Path.cwd()
    if not 'cardano-node' == basename(normpath(current_directory)):
        raise Exception(f"You're not inside 'cardano-node' directory but in: {current_directory}")

    print(f"current_directory: {current_directory}")
    cmd = (
        f"./cardano-node run --topology {env}-topology.json --database-path "
        f"{Path(ROOT_TEST_PATH) / 'cardano-node' / 'db'} "
        f"--host-addr 0.0.0.0 --port 3000 --config "
        f"{env}-config.json --socket-path ./db/node.socket"
    )

    logfile = open(NODE_LOG_FILE_PATH, "w+")
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


def setup_postgres():
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    export_env_var("PGHOST", 'localhost')
    export_env_var("PGUSER", 'postgres')
    export_env_var("PGPORT", '5432')

    try:
        cmd = "./scripts/postgres-start.sh '/tmp/postgres' -k"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        print(f"Setup postgres script output: {output}")
        os.chdir(current_directory)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def start_db_sync():
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    export_env_var("ENVIRONMENT", get_environment())
    export_env_var("LOG_FILEPATH", DB_SYNC_LOG_FILE_PATH)

    try:
        cmd = "./scripts/start_database.sh"
        p = subprocess.Popen(cmd)
        os.chdir(current_directory)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

    not_found = True
    counter = 0

    while (not_found):
        if counter > 600:
            print(f"ERROR: waited {counter} seconds and the db-sync was not started")
            exit(1)

        for proc in process_iter():
            if "cardano-db-sync" in proc.name():
                print(f"db-sync process present: {proc}")
                not_found = False
                return
        print("Waiting for db-sync to start")
        counter += 3
        time.sleep(3)


def get_db_sync_version():
    try:
        cmd = "db-sync-node/bin/cardano-db-sync --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
        )
        cardano_db_sync_version = output.split("git revision ")[0].strip()
        cardano_db_sync_git_revision = output.split("git revision ")[1].strip()
        return str(cardano_db_sync_version), str(cardano_db_sync_git_revision)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def get_db_sync_progress():
    p = subprocess.Popen(["psql", "-P", "pager=off", "-qt", "-U", "postgres", "-d", f"{get_environment()}",  "-c", "select 100 * (extract (epoch from (max (time) at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC'))) / (extract (epoch from (now () at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC'))) as sync_percent from block ;" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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


def get_db_sync_tip():
    isPostgresOn = True
    count = 0
    p = subprocess.Popen(["psql", "-P", "pager=off", "-qt", "-U", "postgres", "-d", f"{get_environment()}",  "-c", "select epoch_no, block_no from block order by id desc limit 1;" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    while isPostgresOn:
        try:
            outs, errs = p.communicate(timeout=5)
            epoch_no, block_no = [e.strip() for e in outs.decode("utf-8").split("|")]
            return epoch_no, block_no
        except ValueError as e:
            if count > 10:
                isPostgresOn = False
                raise
            time.sleep(60)
            count += 1
        except TimeoutExpired as e:
            p.kill()
            raise RuntimeError(
                "command '{}' return with error (code {}): {}".format(
                    e.cmd, e.returncode, " ".join(str(e.output).split())
                )
            )


def export_epoch_sync_times_from_db(file):
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    p = subprocess.Popen(["psql", f"{get_environment()}", "-t", "-c", f"\o {file}", "-c", "SELECT array_to_json(array_agg(epoch_sync_time), FALSE) FROM epoch_sync_time;" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        outs, errs = p.communicate(timeout=5)
        print(outs.decode("utf-8"))
        return outs.decode("utf-8")
    except (TimeoutExpired, subprocess.CalledProcessError) as e:
        p.kill()
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def wait_for_db_to_sync():
    start_sync = time.perf_counter()
    isFloat = False

    while not isFloat:
        try:
            db_sync_progress = float(get_db_sync_progress())
            isFloat = True
        except ValueError:
            print("Progress count has not started, output missing or not a float number")
            time.sleep(5)

    count = 0
    while db_sync_progress < 1:
        if count % 60 == 0:
            get_node_tip()
            epoch_no, block_no = get_db_sync_tip()
            db_sync_progress = float(get_db_sync_progress())
            print(f"db sync progress : {db_sync_progress}, epoch: {epoch_no}, block: {block_no}")
        time.sleep(1)
        count += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    return sync_time_seconds


def main():

    platform_system, platform_release, platform_version = get_os_type()
    print(f"Platform: {platform_system, platform_release, platform_version}")

    start_test_time = get_current_date_time()
    print(f"Test start time: {start_test_time}")

    env = get_environment()
    print(f"Environment: {env}")

    node_pr = get_node_pr()
    print(f"Node PR number: {node_pr}")

    db_branch = get_db_sync_branch()
    print(f"DB sync branch: {db_branch}")

    # cardano-node setup
    NODE_DIR=create_dir('cardano-node')
    os.chdir(NODE_DIR)
    set_node_socket_path_env_var_in_cwd()
    get_node_config_files(env)
    get_and_extract_archive_files(get_node_archive_url(node_pr))
    cli_version, cli_git_rev = get_node_version()
    start_node_in_cwd(env)
    print_file(NODE_LOG_FILE_PATH)

    # cardano-db sync setup
    os.chdir(ROOT_TEST_PATH)
    setup_postgres()
    DB_SYNC_DIR = clone_repo('cardano-db-sync', db_branch)
    os.chdir(DB_SYNC_DIR)
    sync_test_start_time = get_current_date_time()
    start_db_sync()
    db_sync_version, db_sync_git_rev = get_db_sync_version()
    print(f"- cardano-db-sync version: {db_sync_version}")
    print(f"- cardano-db-sync git revision: {db_sync_git_rev}")
    print_file(DB_SYNC_LOG_FILE_PATH)
    db_full_sync_time_in_secs = wait_for_db_to_sync()
    epoch_no, block_no = get_db_sync_tip()
    end_test_time = get_current_date_time()
    print(f"FINAL db-sync progress: {get_db_sync_progress()}, epoch: {epoch_no}, block: {block_no}")
    print(f"TOTAL sync time [sec]: {db_full_sync_time_in_secs}")


    # shut down services
    stop_process('cardano-db-sync')
    stop_process('cardano-node')

    # export test data as a json file
    test_data = OrderedDict()
    test_data["platform_system"] = platform_system
    test_data["platform_release"] = platform_release
    test_data["platform_version"] = platform_version
    test_data["no_of_cpu_cores"] = get_no_of_cpu_cores()
    test_data["total_ram_in_GB"] = get_total_ram_in_GB()
    test_data["env"] = env
    test_data["node_pr"] = node_pr
    test_data["db_sync_branch"] = db_branch
    test_data["node_cli_version"] = cli_version
    test_data["node_git_revision"] = cli_git_rev
    test_data["db_sync_version"] = db_sync_version
    test_data["db_sync_git_rev"] = db_sync_git_rev
    test_data["start_test_time"] = start_test_time
    test_data["end_test_time"] = end_test_time
    test_data["total_sync_time_in_sec"] = db_full_sync_time_in_secs
    test_data["total_sync_time_in_h_m_s"] = seconds_to_time(int(db_full_sync_time_in_secs))
    test_data["last_synced_epoch_no"] = epoch_no
    test_data["last_synced_block_no"] = block_no
    with open(TEST_RESULTS_FILE_NAME, 'w') as test_results_file:
        json.dump(test_data, test_results_file, indent=2)

    export_epoch_sync_times_from_db(EPOCH_SYNC_TIMES_FILE_NAME)

    print_file(TEST_RESULTS_FILE_NAME)

    # compress artifacts
    NODE_ARCHIVE = 'cardano_node.zip'
    DB_SYNC_ARCHIVE = 'cardano_db_sync.zip'
    SYNC_DATA_ARCHIVE = 'epoch_sync_times_dump.zip'

    #zip_file(NODE_ARCHIVE, NODE_LOG_FILE_PATH)
    #zip_file(DB_SYNC_ARCHIVE, DB_SYNC_LOG_FILE_PATH)
    #zip_file(SYNC_DATA_ARCHIVE, EPOCH_SYNC_TIMES_FILE_PATH)

    # upload artifacts
    #upload_artifact(NODE_ARCHIVE)
    #upload_artifact(DB_SYNC_ARCHIVE)
    #upload_artifact(SYNC_DATA_ARCHIVE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute basic sync test\n\n")

    parser.add_argument(
        "-npr", "--node_pr", help="node pr number"
    )
    parser.add_argument(
        "-dbr", "--db_sync_branch", help="db-sync branch"
    )
    parser.add_argument(
        "-e",
        "--environment",
        help="the environment on which to run the tests - shelley_qa, testnet, staging or mainnet.",
    )

    args = parser.parse_args()

    main()
