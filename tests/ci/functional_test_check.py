#!/usr/bin/env python3

import argparse
import atexit
import csv
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

# isort: off
from github import Github

# isort: on

from build_download_helper import download_all_deb_packages
from clickhouse_helper import (
    CiLogsCredentials,
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    get_commit,
    override_status,
    post_commit_status,
    post_commit_status_to_file,
    update_mergeable_check,
)
from docker_images_helper import DockerImage, get_docker_image, pull_image
from download_release_packages import download_last_release
from env_helper import REPO_COPY, REPORT_PATH, TEMP_PATH
from get_robot_token import get_best_robot_token
from pr_info import FORCE_TESTS_LABEL, PRInfo
from report import ERROR, SUCCESS, StatusType, TestResults, read_test_results
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results

NO_CHANGES_MSG = "Nothing to run"


def get_additional_envs(
    check_name: str, run_by_hash_num: int, run_by_hash_total: int
) -> List[str]:
    result = []
    if "DatabaseReplicated" in check_name:
        result.append("USE_DATABASE_REPLICATED=1")
    if "DatabaseOrdinary" in check_name:
        result.append("USE_DATABASE_ORDINARY=1")
    if "wide parts enabled" in check_name:
        result.append("USE_POLYMORPHIC_PARTS=1")
    if "ParallelReplicas" in check_name:
        result.append("USE_PARALLEL_REPLICAS=1")
    if "s3 storage" in check_name:
        result.append("USE_S3_STORAGE_FOR_MERGE_TREE=1")
        result.append("RANDOMIZE_OBJECT_KEY_TYPE=1")
    if "analyzer" in check_name:
        result.append("USE_NEW_ANALYZER=1")

    if run_by_hash_total != 0:
        result.append(f"RUN_BY_HASH_NUM={run_by_hash_num}")
        result.append(f"RUN_BY_HASH_TOTAL={run_by_hash_total}")

    return result


def get_image_name(check_name: str) -> str:
    if "stateless" in check_name.lower():
        return "clickhouse/stateless-test"
    if "stateful" in check_name.lower():
        return "clickhouse/stateful-test"
    else:
        raise Exception(f"Cannot deduce image name based on check name {check_name}")


def get_run_command(
    check_name: str,
    builds_path: Path,
    repo_path: Path,
    result_path: Path,
    server_log_path: Path,
    kill_timeout: int,
    additional_envs: List[str],
    ci_logs_args: str,
    image: DockerImage,
    flaky_check: bool,
    tests_to_run: List[str],
) -> str:
    additional_options = ["--hung-check"]
    additional_options.append("--print-time")

    if tests_to_run:
        additional_options += tests_to_run

    additional_options_str = (
        '-e ADDITIONAL_OPTIONS="' + " ".join(additional_options) + '"'
    )

    envs = [
        f"-e MAX_RUN_TIME={int(0.9 * kill_timeout)}",
        # a static link, don't use S3_URL or S3_DOWNLOAD
        '-e S3_URL="https://s3.amazonaws.com/clickhouse-datasets"',
    ]

    if flaky_check:
        envs.append("-e NUM_TRIES=100")
        envs.append("-e MAX_RUN_TIME=1800")

    envs += [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)
    volume_with_broken_test = (
        f"--volume={repo_path}/tests/analyzer_tech_debt.txt:/analyzer_tech_debt.txt "
        if "analyzer" in check_name
        else ""
    )

    return (
        f"docker run --volume={builds_path}:/package_folder "
        f"{ci_logs_args}"
        f"--volume={repo_path}/tests:/usr/share/clickhouse-test "
        f"{volume_with_broken_test}"
        f"--volume={result_path}:/test_output "
        f"--volume={server_log_path}:/var/log/clickhouse-server "
        f"--cap-add=SYS_PTRACE {env_str} {additional_options_str} {image}"
    )


def get_tests_to_run(pr_info: PRInfo) -> List[str]:
    result = set()

    if pr_info.changed_files is None:
        return []

    for fpath in pr_info.changed_files:
        if re.match(r"tests/queries/0_stateless/[0-9]{5}", fpath):
            logging.info("File '%s' is changed and seems like a test", fpath)
            fname = fpath.split("/")[3]
            fname_without_ext = os.path.splitext(fname)[0]
            # add '.' to the end of the test name not to run all tests with the same prefix
            # e.g. we changed '00001_some_name.reference'
            # and we have ['00001_some_name.sh', '00001_some_name_2.sql']
            # so we want to run only '00001_some_name.sh'
            result.add(fname_without_ext + ".")
        elif "tests/queries/" in fpath:
            # log suspicious changes from tests/ for debugging in case of any problems
            logging.info("File '%s' is changed, but it doesn't look like a test", fpath)
    return list(result)


def process_results(
    result_directory: Path,
    server_log_path: Path,
) -> Tuple[StatusType, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible for content of result_directory.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    if server_log_path.exists():
        additional_files = additional_files + [
            p for p in server_log_path.iterdir() if p.is_file()
        ]

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found %s", status_path.name)
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"

        if results_path.exists():
            logging.info("Found test_results.tsv")
        else:
            logging.info("Files in result folder %s", os.listdir(result_directory))
            return ERROR, "Not found test_results.tsv", test_results, additional_files

        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            return ERROR, "Empty test_results.tsv", test_results, additional_files
    except Exception as e:
        return (
            ERROR,
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files  # type: ignore


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    parser.add_argument("kill_timeout", type=int)
    parser.add_argument(
        "--validate-bugfix",
        action="store_true",
        help="Check that added tests failed on latest stable",
    )
    parser.add_argument(
        "--post-commit-status",
        default="commit_status",
        choices=["commit_status", "file"],
        help="Where to public post commit status",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    repo_path = Path(REPO_COPY)
    post_commit_path = temp_path / "functional_commit_status.tsv"

    args = parse_args()
    check_name = args.check_name or os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"
    kill_timeout = args.kill_timeout or int(os.getenv("KILL_TIMEOUT", "0"))
    assert (
        kill_timeout > 0
    ), "kill timeout must be provided as an input arg or in KILL_TIMEOUT env"
    validate_bugfix_check = args.validate_bugfix
    print(f"Runnin check [{check_name}] with timeout [{kill_timeout}]")

    flaky_check = "flaky" in check_name.lower()

    run_changed_tests = flaky_check or validate_bugfix_check

    # For validate_bugfix_check we need up to date information about labels, so pr_event_from_api is used
    pr_info = PRInfo(
        need_changed_files=run_changed_tests, pr_event_from_api=validate_bugfix_check
    )

    # FIXME: move to job report and remove
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    atexit.register(update_mergeable_check, commit, pr_info, check_name)

    if validate_bugfix_check and "pr-bugfix" not in pr_info.labels:
        if args.post_commit_status == "file":
            post_commit_status_to_file(
                post_commit_path,
                f"Skipped (no pr-bugfix in {pr_info.labels})",
                SUCCESS,
                "null",
            )
        logging.info("Skipping '%s' (no pr-bugfix in %s)", check_name, pr_info.labels)
        sys.exit(0)

    if "RUN_BY_HASH_NUM" in os.environ:
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        run_by_hash_num = 0
        run_by_hash_total = 0
        check_name_with_group = check_name

    tests_to_run = []
    if run_changed_tests:
        tests_to_run = get_tests_to_run(pr_info)
        if not tests_to_run:
            state = override_status(SUCCESS, check_name, validate_bugfix_check)
            if args.post_commit_status == "commit_status":
                post_commit_status(
                    commit,
                    state,
                    "",
                    NO_CHANGES_MSG,
                    check_name_with_group,
                    pr_info,
                    dump_to_file=True,
                )
            elif args.post_commit_status == "file":
                post_commit_status_to_file(
                    post_commit_path,
                    description=NO_CHANGES_MSG,
                    state=state,
                    report_url="null",
                )
            sys.exit(0)

    image_name = get_image_name(check_name)

    docker_image = pull_image(get_docker_image(image_name))

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    if validate_bugfix_check:
        download_last_release(packages_path)
    else:
        download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = result_path / "run.log"

    additional_envs = get_additional_envs(
        check_name, run_by_hash_num, run_by_hash_total
    )
    if validate_bugfix_check:
        additional_envs.append("GLOBAL_TAGS=no-random-settings")
        additional_envs.append("BUGFIX_VALIDATE_CHECK=1")

    ci_logs_credentials = CiLogsCredentials(temp_path / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    run_command = get_run_command(
        check_name,
        packages_path,
        repo_path,
        result_path,
        server_log_path,
        kill_timeout,
        additional_envs,
        ci_logs_args,
        docker_image,
        flaky_check,
        tests_to_run,
    )
    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    try:
        subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    except subprocess.CalledProcessError:
        logging.warning("Failed to change files owner in %s, ignoring it", temp_path)

    ci_logs_credentials.clean_ci_logs_from_credentials(run_log_path)
    s3_helper = S3Helper()

    state, description, test_results, additional_logs = process_results(
        result_path, server_log_path
    )
    state = override_status(state, check_name, invert=validate_bugfix_check)

    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [run_log_path] + additional_logs,
        check_name_with_group,
    )

    print(f"::notice:: {check_name} Report url: {report_url}")
    if args.post_commit_status == "commit_status":
        post_commit_status(
            commit,
            state,
            report_url,
            description,
            check_name_with_group,
            pr_info,
            dump_to_file=True,
        )
    elif args.post_commit_status == "file":
        post_commit_status_to_file(
            post_commit_path,
            description,
            state,
            report_url,
        )
    else:
        raise Exception(
            f'Unknown post_commit_status option "{args.post_commit_status}"'
        )

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name_with_group,
    )
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state != SUCCESS:
        if FORCE_TESTS_LABEL in pr_info.labels:
            print(f"'{FORCE_TESTS_LABEL}' enabled, will report success")
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
