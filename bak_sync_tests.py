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

from psutil import process_iter
from utils import seconds_to_time, date_diff_in_seconds, get_no_of_cpu_cores, \
    get_current_date_time, get_os_type, get_directory_size, get_total_ram_in_GB


current_directory = os.getcwd()
print(f" - current_directory: {current_directory}")

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

def clone_repo(repo_name):
    current_directory = os.getcwd()
    try:
        cmd = f"git clone git@github.com:input-output-hk/{repo_name}.git"
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        print(f"Repo: {repo_name} cloned to: {current_directory}")
        return f"{current_directory}/{repo_name}"
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

def get_and_extract_node_files(node_pr):
    db_sync_pr=f"-pr-{node_pr}"
    initial_download_url = f"https://hydra.iohk.io/job/Cardano/cardano-node{db_sync_pr}/cardano-node-linux/latest-finished/download/1/"
    current_directory = os.getcwd()

    request = requests.get(initial_download_url, allow_redirects=True)
    download_url = request.url
    archive_name = download_url.split("/")[-1].strip()

    print(f" - current_directory: {current_directory}")
    print(f"download_url: {download_url}")
    print(f"archive name: {archive_name}")

    urllib.request.urlretrieve(download_url, Path(current_directory) / archive_name)

    print(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    print(f" - listdir (after archive extraction): {os.listdir(current_directory)}")


def git_get_commit_sha_for_tag_no(tag_no):
    global jData
    url = "https://api.github.com/repos/input-output-hk/cardano-node/tags"
    response = requests.get(url)

    # there is a rate limit for the provided url that we want to overpass with the below loop
    count = 0
    while not response.ok:
        time.sleep(random.randint(30, 350))
        count += 1
        response = requests.get(url)
        if count > 15:
            print(f"!!!! ERROR: Could not get the commit sha for tag {tag_no} after {count} retries")
            response.raise_for_status()
    jData = json.loads(response.content)

    for tag in jData:
        if tag.get('name') == tag_no:
            return tag.get('commit').get('sha')

    print(f" ===== !!! ERROR: The specified tag_no - {tag_no} - was not found ===== ")
    print(json.dumps(jData, indent=4, sort_keys=True))
    return None


def git_get_hydra_eval_link_for_commit_sha(commit_sha):
    global jData
    url = f"https://api.github.com/repos/input-output-hk/cardano-node/commits/{commit_sha}/status"
    response = requests.get(url)

    # there is a rate limit for the provided url that we want to overpass with the below loop
    count = 0
    while not response.ok:
        time.sleep(random.randint(30, 240))
        count += 1
        response = requests.get(url)
        if count > 10:
            print(f"!!!! ERROR: Could not get the hydra eval link for tag {commit_sha} after {count} retries")
            response.raise_for_status()
    jData = json.loads(response.content)

    for status in jData.get('statuses'):
        if "hydra.iohk.io/eval" in status.get("target_url"):
            return status.get("target_url")

    print(f" ===== !!! ERROR: There is not eval link for the provided commit_sha - {commit_sha} =====")
    print(json.dumps(jData, indent=2, sort_keys=True))
    return None


def get_hydra_build_download_url(eval_url):
    global eval_jData, build_jData

    headers = {'Content-type': 'application/json'}
    eval_response = requests.get(eval_url, headers=headers)

    eval_jData = json.loads(eval_response.content)

    if eval_response.ok:
        eval_jData = json.loads(eval_response.content)
    else:
        eval_response.raise_for_status()

    for build_no in eval_jData.get("builds"):
        build_url = f"https://hydra.iohk.io/build/{build_no}"
        build_response = requests.get(build_url, headers=headers)

        count = 0
        while not build_response.ok:
            time.sleep(2)
            count += 1
            build_response = requests.get(build_url, headers=headers)
            if count > 9:
                build_response.raise_for_status()

        build_jData = json.loads(build_response.content)

        if build_jData.get("job") == "cardano-node-linux":
            name = build_jData.get("nixname")
            return f"https://hydra.iohk.io/build/{build_no}/download/1/{name}-linux.tar.gz"

    print(f" ===== !!! ERROR: No build has found for the required os_type - {os_type} - {eval_url}")
    return None


def check_string_format(input_string):
    if len(input_string) == 40:
        return "commit_sha_format"
    elif len(input_string) == 7:
        return "eval_url"
    else:
        return "tag_format"


def get_and_extract_node_files_2(tag_no):
    print(" - get and extract the pre-built node files")
    current_directory = os.getcwd()
    print(f" - current_directory for extracting node files: {current_directory}")
    platform_system, platform_release, platform_version = get_os_type()

    if check_string_format(tag_no) == "tag_format":
        commit_sha = git_get_commit_sha_for_tag_no(tag_no)
    elif check_string_format(tag_no) == "commit_sha_format":
        commit_sha = tag_no
    elif check_string_format(tag_no) == "eval_url":
        commit_sha = None
    else:
        print(f" !!! ERROR: invalid format for tag_no - {tag_no}; Expected tag_no or commit_sha.")
        commit_sha = None

    if check_string_format(tag_no) == "eval_url":
        eval_no = tag_no
        eval_url = "https://hydra.iohk.io/eval/" + eval_no
    else:
        eval_url = git_get_hydra_eval_link_for_commit_sha(commit_sha)

    print(f"commit_sha  : {commit_sha}")
    print(f"eval_url    : {eval_url}")

    download_url = get_hydra_build_download_url(eval_url)
    get_and_extract_linux_files(download_url)


def get_and_extract_linux_files(download_url):
    current_directory = os.getcwd()
    print(f" - current_directory: {current_directory}")
    archive_name = download_url.split("/")[-1].strip()

    print(f"archive_name: {archive_name}")
    print(f"download_url: {download_url}")

    urllib.request.urlretrieve(download_url, Path(current_directory) / archive_name)

    print(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    print(f" - listdir (after archive extraction): {os.listdir(current_directory)}")


def get_and_extract_db_sync_files(db_pr):
    db_sync_pr=f"-pr-{db_pr}"
    initial_download_url = f"https://hydra.iohk.io/job/Cardano/cardano-db-sync{db_sync_pr}/cardano-db-sync-linux/latest-finished/download/1/"
    current_directory = os.getcwd()

    request = requests.get(initial_download_url, allow_redirects=True)
    download_url = request.url
    archive_name = download_url.split("/")[-1].strip()

    print(f" - current_directory: {current_directory}")
    print(f"download_url: {download_url}")
    print(f"archive name: {archive_name}")

    urllib.request.urlretrieve(download_url, Path(current_directory) / archive_name)

    print(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    print(f" - listdir (after archive extraction): {os.listdir(current_directory)}")


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


def wait_for_node_to_start(tag_no):
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take >10 mins for the cli to work (opening db and
    # replaying the ledger)
    start_counter = time.perf_counter()
    get_current_tip(tag_no, 18000)
    stop_counter = time.perf_counter()

    start_time_seconds = int(stop_counter - start_counter)
    print(f" === It took {start_time_seconds} seconds for the QUERY TIP command to be available")
    return start_time_seconds


def get_current_tip(tag_no=None, timeout_seconds=10):
    # tag_no should have this format: 1.23.0, 1.24.1, etc
    cmd = CLI + " query tip " + get_testnet_value()

    for i in range(timeout_seconds):
        try:
            output = (
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                    .decode("utf-8")
                    .strip()
            )
            output_json = json.loads(output)

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


def start_node_unix(env, tag_no):
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
        secs_to_start = wait_for_node_to_start(tag_no)
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


def wait_for_node_to_sync(env, tag_no):
    era_details_dict = OrderedDict()
    epoch_details_dict = OrderedDict()

    actual_epoch, actual_block, actual_hash, actual_slot, actual_era, syncProgress = get_current_tip(tag_no)
    start_sync = time.perf_counter()

    count = 0
    if syncProgress is not None:
        while syncProgress < 100:
            if count % 60 == 0:
                print(f"actual_era  : {actual_era} "
                      f" - actual_epoch: {actual_epoch} "
                      f" - actual_block: {actual_block} "
                      f" - actual_slot : {actual_slot} "
                      f" - syncProgress: {syncProgress}")
            time.sleep(1)
            count += 1
            actual_epoch, actual_block, actual_hash, actual_slot, actual_era, syncProgress = get_current_tip(tag_no)

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    print(f"sync_time_seconds: {sync_time_seconds}")

    return sync_time_seconds


def setup_postgres():
    os.environ["PGHOST"] = 'localhost'
    os.environ["PGUSER"] = 'postgres'
    os.environ["PGPORT"] = '5433'

    try:
        cmd = "./scripts/postgres-start.sh '/tmp/postgres' -k"
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        print(f"Running postgres script: {output}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

def create_database():
    os.chdir(ROOT_TEST_PATH)
    env = vars(args)["environment"]

    try:
        cmd = f"environment={env} ./scripts/create_database.sh"
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        print(f"Running create_database script: {output}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def start_db_sync():
    os.chdir(ROOT_TEST_PATH)
    #cmd2 = "ls -l config"
    #output2 = subprocess.check_output(cmd2, shell=True, stderr=subprocess.STDOUT)
    #print(f"Running create_database script: {output2}")
    current_directory = Path.cwd()
    print(f"current_directory start db: {current_directory}")
    env = vars(args)["environment"]
    os.environ["PGPASSFILE"] = str(f"config/pgpass-{env}")
    config = ""
    if env == "shelley_qa":
        config = "config/shelley-qa-config.json"
        env = "shelley_qa"
    else:
        config = f"config/{env}-config.yaml"
    #PGPASSFILE={os.environ['PGPASSFILE']}
    #cmd = (
    #    f"PGPASSFILE=config/pgpass-shelley_qa ./cardano-db-sync-extended --config {config} "
    #    f"--socket-path ../cardano-node/db/node.socket "
    #    f"--schema-dir schema/ "
    #    f"--state-dir ledger-state/{env}"
    #)
    cmd = "./scripts/start_database.sh"
    logfile = open(DB_SYNC_LOG_FILE, "w+")
    print(cmd)

    try:
        p = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        print(f"Running create_database script: {output}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

def main():

    start_test_time = get_current_date_time()
    print(f"Start test time: {start_test_time}")

    set_repo_paths()

    NODE_DIR=create_dir("cardano-node")
    os.chdir(NODE_DIR)

    env = vars(args)["environment"]
    print(f"env: {env}")

    set_node_socket_path_env_var()

    node_tag = str(vars(args)["node_tag"]).strip()
    print(f"node_node_tag: {node_tag}")


    platform_system, platform_release, platform_version = get_os_type()
    print(f"platform: {platform_system, platform_release, platform_version}")

    get_node_config_files_time = get_current_date_time()
    print(f"Get node config files time: {get_node_config_files_time}")
    print("get the node config files")
    get_node_config_files(env)

    get_node_build_files_time = get_current_date_time()
    print(f"Get node build files time:  {get_node_build_files_time}")
    print("get the pre-built node files")
    get_and_extract_node_files(node_tag)

    print("===================================================================================")
    print(f"====================== Start node sync test for tag: {node_tag} =============")
    print("===================================================================================")

    print(" --- node version ---")
    cli_version1, cli_git_rev1 = get_node_version()
    print(f"  - cardano_cli_version1: {cli_version1}")
    print(f"  - cardano_cli_git_rev1: {cli_git_rev1}")

    print(f"   ======================= Start node using node_tag: {node_tag} ====================")
    start_sync_time1 = get_current_date_time()

    secs_to_start1 = start_node_unix(env, node_tag)

    print(" - waiting for the node to sync")
    #sync_time_seconds1 = wait_for_node_to_sync(env, node_tag)

    #end_sync_time1 = get_current_date_time()
    #print(f"secs_to_start1            : {secs_to_start1}")
    #print(f"start_sync_time1          : {start_sync_time1}")
    #print(f"end_sync_time1            : {end_sync_time1}")
    time.sleep(10)

    db_sync_pr = str(vars(args)["db_sync_pr"]).strip()
    print(f"db_sync_tag: {db_sync_pr}")

    os.chdir(ROOT_TEST_PATH)
    setup_postgres()

    DB_SYNC_DIR = clone_repo("cardano-db-sync")
    os.chdir(DB_SYNC_DIR)

    current_directory = os.getcwd()

    for item in os.listdir(current_directory):
        if os.path.isfile(item):
            os.remove(item)
        elif os.path.isdir(item) and item != "config" and item != "scripts" and item != "schema":
            shutil.rmtree(item)

    get_and_extract_db_sync_files(db_sync_pr)
    create_database()
    os.chdir(DB_SYNC_DIR)
    start_db_sync()

    time.sleep(10)

    file_o=open(DB_SYNC_LOG_FILE)
    content=file_o.read()
    print(content)
    file_o.close()

    print(f"   =============== Stop node: {node_tag} ======================")
    #stop_node()


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
