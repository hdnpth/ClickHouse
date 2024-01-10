import argparse
import concurrent.futures
from dataclasses import asdict, dataclass
from enum import Enum
import json
import os
import re
import subprocess
import sys
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import docker_images_helper
from ci_config import CI_CONFIG, JobNames
from commit_status_helper import (
    CommitStatusData,
    RerunHelper,
    format_description,
    get_commit,
    post_commit_status,
    set_status_comment,
    update_mergeable_check,
)
from digest_helper import DockerDigester, JobDigester
from env_helper import (
    CI,
    GITHUB_JOB_API_URL,
    GITHUB_RUN_URL,
    REPO_COPY,
    REPORT_PATH,
    S3_BUILDS_BUCKET,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, Git
from git_helper import Runner as GitRunner
from github import Github
from pr_info import PRInfo
from report import BuildResult, JobReport
from s3_helper import S3Helper
from clickhouse_helper import (
    CiLogsCredentials,
    ClickHouseHelper,
    get_instance_id,
    get_instance_type,
    prepare_tests_results_for_clickhouse,
)
from build_check import get_release_or_pr
import upload_result_helper
from version_helper import get_version_from_repo


@dataclass
class PendingState:
    class InternalState(Enum):
        PLANNED = "planned"
        STARTED = "started"

    updated_at: float
    run_url: str
    state: InternalState


class CiCache:
    def __init__(
        self,
        s3: S3Helper,
        job_digests: Dict[str, str],
    ):
        self.s3 = s3
        self.build_digest = job_digests[JobNames.PACKAGE_RELEASE.value]
        self.docs_digest = job_digests[JobNames.DOCS_CHECK.value]
        self.build_path = self.get_s3_path(self.build_digest)
        self.docs_path = f"CI_data/DOCS-{self.docs_digest}/"
        self.successful_jobs = []  # type: List[str]
        self.successful_on_master_jobs = []  # type: List[str]
        self.pending_jobs = []  # type: List[str]
        self.cache_updated = False
        self.job_digests = job_digests
        self.cache_path = Path(TEMP_PATH) / "ci_cache"

    def _get_job_status_file_name_and_s3path(
        self, job_name: str, batch: int, num_batches: int, is_master: bool = False
    ) -> Tuple[str, str]:
        file_name = (
            f"job_{job_name}_{self.job_digests[job_name]}_{batch}_{num_batches}.ci"
        )
        if is_master:
            file_name = file_name[-3] + ".master.ci"
        if not is_docs_job(job_name):
            path = self.docs_path + file_name
        else:
            path = self.build_path + file_name
        return file_name, path

    def _get_job_pending_file_name_and_s3path(
        self, job_name: str, batch: int, num_batches: int
    ) -> Tuple[str, str]:
        file_name = f"pending_job_{job_name}_{self.job_digests[job_name]}_{batch}_{num_batches}.cip"
        if not is_docs_job(job_name):
            path = self.docs_path + file_name
        else:
            path = self.build_path + file_name
        return file_name, path

    @staticmethod
    def get_s3_path(build_digest: str) -> str:
        return f"CI_data/BUILD-{build_digest}/"

    def update(self):
        """
        pulls cache state from s3, only cached job names without downloading CommitStatusData files
        """
        # # deep copy
        # self.successful_jobs_previous = list(self.successful_jobs)
        done_files = self.s3.list_prefix(self.build_path) + self.s3.list_prefix(
            self.docs_path
        )
        done_files = [file.split("/")[-1] for file in done_files]
        self.successful_jobs = [
            file.replace(".master.ci", ".ci")
            for file in done_files
            if file.endswith(".ci")
        ]
        self.successful_on_master_jobs = [
            file.replace(".master.ci", ".ci")
            for file in done_files
            if file.endswith(".master.ci")
        ]
        self.pending_jobs = [file for file in done_files if file.endswith(".cip")]
        self.cache_updated = True
        return self

    def fetch_job_statuses(self):
        """
        pulls CommitStatusData for all cached jobs from s3
        """
        if not self.cache_path.exists():
            self.cache_path.mkdir(parents=True, exist_ok=True)

        # clean up before start
        for file in self.cache_path.glob("*.ci"):
            file.unlink()

        # download all metadata files
        _ = self.s3.download_files(  # type: ignore
            bucket=S3_BUILDS_BUCKET,
            s3_path=self.build_path,
            file_suffix=".ci",
            local_directory=self.cache_path,
        )
        # print(f"CI metadata files [{files}]")
        _ = self.s3.download_files(  # type: ignore
            bucket=S3_BUILDS_BUCKET,
            s3_path=self.docs_path,
            file_suffix=".ci",
            local_directory=self.cache_path,
        )
        # print(f"CI docs metadata files [{files_docs}]")
        return self

    def is_successful(
        self, job: str, batch: int, num_batches: int, on_master: bool = False
    ) -> bool:
        """
        checks if a given job have already been done successfuly
        """
        if not self.cache_updated:
            self.update()
        return (
            self._get_job_status_file_name_and_s3path(job, batch, num_batches)[0]
            in self.successful_jobs
            if not on_master
            else self.successful_on_master_jobs
        )

    def push(
        self,
        job: str,
        batch: int,
        num_batches: int,
        job_status: CommitStatusData,
        is_master: bool = False,
    ) -> None:
        """
        pushes a job's status (CommitStatusData) to s3 cache
        @is_master adds master attribute for created cache item
        """
        success_flag_name, path = self._get_job_status_file_name_and_s3path(
            job, batch, num_batches, is_master
        )
        job_status.dump_to_file(success_flag_name)
        _ = self.s3.upload_file(
            bucket=S3_BUILDS_BUCKET, file_path=success_flag_name, s3_path=path
        )
        os.remove(success_flag_name)

    def pop(self, job: str, batch: int, num_batches: int) -> Optional[CommitStatusData]:
        """
        retrives job's status (CommitStatusData) from cache, or None if a cache miss
        """
        if not self.cache_path.exists():
            print(
                f"ERROR: {self.cache_path} does not exist. fetch job statuses from s3 cache"
            )
            self.fetch_job_statuses()
        success_flag_name = self._get_job_status_file_name_and_s3path(
            job, batch, num_batches
        )[0]
        if success_flag_name in self.successful_jobs:
            res = CommitStatusData.load_from_file(
                self.cache_path / success_flag_name
            )  # type: CommitStatusData
            assert res.status == "success", "BUG!"
            return res
        else:
            return None

    def is_pending(self, job: str, batch: int, num_batches: int) -> bool:
        """
        checks if a given job is being executed
        """
        if not self.cache_updated:
            self.update()
        return (
            self._get_job_pending_file_name_and_s3path(job, batch, num_batches)[0]
            in self.pending_jobs
        )

    def push_pending(
        self, job: str, batch: int, num_batches: int, state: PendingState.InternalState
    ) -> None:
        """
        pushes pending status for a job to the cache
        """
        print(
            f"Push pending state for the [{job}_[{batch}/{num_batches}]] to the cache"
        )
        file_name, s3_path = self._get_job_pending_file_name_and_s3path(
            job, batch, num_batches
        )
        PendingState(updated_at=time.time(), run_url=GITHUB_RUN_URL, state=state)
        with open(file_name, "w") as json_file:
            json.dump(asdict(self), json_file)  # type: ignore
        _ = self.s3.upload_file(
            bucket=S3_BUILDS_BUCKET, file_path=file_name, s3_path=s3_path
        )
        Path(file_name).unlink()

    def delete_pending(self, job: str, batch: int, num_batches: int) -> None:
        """
        deletes pending status for a job in the cache
        """
        _, s3_path = self._get_job_pending_file_name_and_s3path(job, batch, num_batches)
        self.s3.delete_file_from_s3(S3_BUILDS_BUCKET, s3_path)

    def await_jobs(self, jobs_with_params: Dict[str, Dict[str, Any]]) -> List[str]:
        if not jobs_with_params:
            return []
        print(f"Start awaiting jobs [{list(jobs_with_params)}]")
        poll_interval_sec = 180
        start_at = int(time.time())
        TIMEOUT = 3000
        expired_sec = 0
        done_jobs = []  # type: List[str]
        while expired_sec < TIMEOUT and jobs_with_params:
            time.sleep(poll_interval_sec)
            self.update()
            pending_finished: List[str] = []
            for job_name in jobs_with_params:
                num_batches = jobs_with_params[job_name]["num_batches"]
                for batch in jobs_with_params[job_name]["batches"]:
                    if self.is_pending(job_name, batch, num_batches):
                        continue
                    print(
                        f"Job [{job_name}_[{batch}/{num_batches}]] is not pending anymore"
                    )
                    pending_finished.append(job_name)
            if pending_finished:
                # restart timer
                start_at = int(time.time())
                expired_sec = 0
                # remove finished jobs from awaiting list
                for job in pending_finished:
                    del jobs_with_params[job]
                    done_jobs.append(job)
            else:
                expired_sec = int(time.time()) - start_at
            print(f"  ...awaiting continues... time left [{TIMEOUT - expired_sec}]")
        if done_jobs:
            print(
                f"Awaiting OK. Left jobs: [{list(jobs_with_params)}], finished jobs: [{done_jobs}]"
            )
        else:
            print("Awaiting FAILED. No job has finished.")
        return done_jobs


def get_check_name(check_name: str, batch: int, num_batches: int) -> str:
    res = check_name
    if num_batches > 1:
        res = f"{check_name} [{batch+1}/{num_batches}]"
    return res


def is_build_job(job: str) -> bool:
    if "package_" in job or "binary_" in job or job == "fuzzers":
        return True
    return False


def is_test_job(job: str) -> bool:
    return not is_build_job(job) and not "Style" in job and not "Docs check" in job


def is_docs_job(job: str) -> bool:
    return "Docs check" in job


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    # FIXME: consider switching to sub_parser for configure, pre, run, post actions
    parser.add_argument(
        "--configure",
        action="store_true",
        help="Action that configures ci run. Calculates digests, checks job to be executed, generates json output",
    )
    parser.add_argument(
        "--update-gh-statuses",
        action="store_true",
        help="Action that recreate success GH statuses for jobs that finished successfully in past and will be skipped this time",
    )
    parser.add_argument(
        "--pre",
        action="store_true",
        help="Action that executes prerequesetes for the job provided in --job-name",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Action that executes run action for specified --job-name. run_command must be configured for a given job name.",
    )
    parser.add_argument(
        "--post",
        action="store_true",
        help="Action that executes post actions for the job provided in --job-name",
    )
    parser.add_argument(
        "--mark-success",
        action="store_true",
        help="Action that marks job provided in --job-name (with batch provided in --batch) as successful",
    )
    parser.add_argument(
        "--job-name",
        default="",
        type=str,
        help="Job name as in config",
    )
    parser.add_argument(
        "--run-command",
        default="",
        type=str,
        help="A run command to run in --run action. Will override run_command from a job config if any",
    )
    parser.add_argument(
        "--batch",
        default=-1,
        type=int,
        help="Current batch number (required for --mark-success), -1 or omit for single-batch job",
    )
    parser.add_argument(
        "--infile",
        default="",
        type=str,
        help="Input json file or json string with ci run config",
    )
    parser.add_argument(
        "--outfile",
        default="",
        type=str,
        required=False,
        help="output file to write json result to, if not set - stdout",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="makes json output pretty formatted",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        default=False,
        help="skip fetching docker data from dockerhub, used in --configure action (for debugging)",
    )
    parser.add_argument(
        "--docker-digest-or-latest",
        action="store_true",
        default=False,
        help="temporary hack to fallback to latest if image with digest as a tag is not on docker hub",
    )
    parser.add_argument(
        "--skip-jobs",
        action="store_true",
        default=False,
        help="skip fetching data about job runs, used in --configure action (for debugging and nigthly ci)",
    )
    parser.add_argument(
        "--rebuild-all-docker",
        action="store_true",
        default=False,
        help="will create run config for rebuilding all dockers, used in --configure action (for nightly docker job)",
    )
    parser.add_argument(
        "--rebuild-all-binaries",
        action="store_true",
        default=False,
        help="will create run config without skipping build jobs in any case, used in --configure action (for release branches)",
    )
    parser.add_argument(
        "--commit-message",
        default="",
        help="debug option to test commit message processing",
    )
    return parser.parse_args()


def check_missing_images_on_dockerhub(
    image_name_tag: Dict[str, str], arch: Optional[str] = None
) -> Dict[str, str]:
    """
    Checks missing images on dockerhub.
    Works concurrently for all given images.
    Docker must be logged in.
    """

    def run_docker_command(
        image: str, image_digest: str, arch: Optional[str] = None
    ) -> Dict:
        """
        aux command for fetching single docker manifest
        """
        command = [
            "docker",
            "manifest",
            "inspect",
            f"{image}:{image_digest}" if not arch else f"{image}:{image_digest}-{arch}",
        ]

        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        return {
            "image": image,
            "image_digest": image_digest,
            "arch": arch,
            "stdout": process.stdout,
            "stderr": process.stderr,
            "return_code": process.returncode,
        }

    result: Dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_docker_command, image, tag, arch)
            for image, tag in image_name_tag.items()
        ]

        responses = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
        for resp in responses:
            name, stdout, stderr, digest, arch = (
                resp["image"],
                resp["stdout"],
                resp["stderr"],
                resp["image_digest"],
                resp["arch"],
            )
            if stderr:
                if stderr.startswith("no such manifest"):
                    result[name] = digest
                else:
                    print(f"Error: Unknown error: {stderr}, {name}, {arch}")
            elif stdout:
                if "mediaType" in stdout:
                    pass
                else:
                    print(f"Error: Unknown response: {stdout}")
                    assert False, "FIXME"
            else:
                print(f"Error: No response for {name}, {digest}, {arch}")
                assert False, "FIXME"
    return result


def _check_and_update_for_early_style_check(jobs_data: dict, docker_data: dict) -> None:
    """
    This is temporary hack to start style check before docker build if possible
    FIXME: need better solution to do style check as soon as possible and as fast as possible w/o dependency on docker job
    """
    jobs_to_do = jobs_data.get("jobs_to_do", [])
    docker_to_build = docker_data.get("missing_multi", [])
    if (
        "Style check" in jobs_to_do
        and docker_to_build
        and "clickhouse/style-test" not in docker_to_build
    ):
        index = jobs_to_do.index("Style check")
        jobs_to_do[index] = "Style check early"


def _update_config_for_docs_only(jobs_data: dict) -> None:
    DOCS_CHECK_JOBS = ["Docs check", "Style check"]
    print(f"NOTE: Will keep only docs related jobs: [{DOCS_CHECK_JOBS}]")
    jobs_to_do = jobs_data.get("jobs_to_do", [])
    jobs_data["jobs_to_do"] = [job for job in jobs_to_do if job in DOCS_CHECK_JOBS]
    jobs_data["jobs_to_wait"] = {
        job: params
        for job, params in jobs_data["jobs_to_wait"].items()
        if job in DOCS_CHECK_JOBS
    }


def _configure_docker_jobs(
    rebuild_all_dockers: bool, docker_digest_or_latest: bool = False
) -> Dict:
    # generate docker jobs data
    docker_digester = DockerDigester()
    imagename_digest_dict = (
        docker_digester.get_all_digests()
    )  # 'image name - digest' mapping
    images_info = docker_images_helper.get_images_info()

    # a. check missing images
    if not rebuild_all_dockers:
        # FIXME: we need login as docker manifest inspect goes directly to one of the *.docker.com hosts instead of "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"]
        #         find if it's possible to use the setting of /etc/docker/daemon.json
        docker_images_helper.docker_login()
        print("Start checking missing images in dockerhub")
        missing_multi_dict = check_missing_images_on_dockerhub(imagename_digest_dict)
        missing_multi = list(missing_multi_dict)
        missing_amd64 = []
        missing_aarch64 = []
        if not docker_digest_or_latest:
            # look for missing arm and amd images only among missing multiarch manifests @missing_multi_dict
            # to avoid extra dockerhub api calls
            missing_amd64 = list(
                check_missing_images_on_dockerhub(missing_multi_dict, "amd64")
            )
            # FIXME: WA until full arm support: skip not supported arm images
            missing_aarch64 = list(
                check_missing_images_on_dockerhub(
                    {
                        im: digest
                        for im, digest in missing_multi_dict.items()
                        if not images_info[im]["only_amd64"]
                    },
                    "aarch64",
                )
            )
        # FIXME: temporary hack, remove after transition to docker digest as tag
        else:
            if missing_multi:
                print(
                    f"WARNING: Missing images {list(missing_multi)} - fallback to latest tag"
                )
                for image in missing_multi:
                    imagename_digest_dict[image] = "latest"
        print("...checking missing images in dockerhub - done")
    else:
        # add all images to missing
        missing_multi = list(imagename_digest_dict)
        missing_amd64 = missing_multi
        # FIXME: WA until full arm support: skip not supported arm images
        missing_aarch64 = [
            name
            for name in imagename_digest_dict
            if not images_info[name]["only_amd64"]
        ]

    return {
        "images": imagename_digest_dict,
        "missing_aarch64": missing_aarch64,
        "missing_amd64": missing_amd64,
        "missing_multi": missing_multi,
    }


def _configure_jobs(
    job_digester: JobDigester,
    s3: S3Helper,
    rebuild_all_binaries: bool,
    pr_labels: Iterable[str],
    commit_tokens: List[str],
    is_master: bool,
) -> Dict:
    ## a. digest each item from the config
    job_digester = JobDigester()
    jobs_params: Dict[str, Dict] = {}
    jobs_to_do: List[str] = []
    jobs_to_skip: List[str] = []
    digests: Dict[str, str] = {}

    print("Calculating job digests - start")
    print("::group::digests")
    for job in CI_CONFIG.job_generator():
        digest = job_digester.get_job_digest(CI_CONFIG.get_digest_config(job))
        digests[job] = digest
        print(f"    job [{job.rjust(50)}] has digest [{digest}]")
    print("::endgroup::")
    print("Calculating job digests - done")

    ## b. check what we need to run
    ci_cache = None
    if "#no_ci_cache" not in commit_tokens:
        ci_cache = CiCache(s3, digests)

    for job in digests:
        digest = digests[job]
        job_config = CI_CONFIG.get_job_config(job)
        num_batches: int = job_config.num_batches
        batches_to_do: List[int] = []
        jobs_to_wait: Dict[str, Dict[str, Any]] = {}

        for batch in range(num_batches):  # type: ignore
            if job_config.disabled_on_master and is_master:
                continue
            if job_config.run_by_label and job_config.run_by_label in pr_labels:
                # this job controlled by label, add to todo if it's labe is set in pr
                batches_to_do.append(batch)
            elif job_config.run_always:
                # always add to todo
                batches_to_do.append(batch)
            elif rebuild_all_binaries and is_build_job(job):
                batches_to_do.append(batch)
            elif not ci_cache:
                batches_to_do.append(batch)
            elif ci_cache.is_successful(
                job, batch, num_batches, job_config.required_on_master
            ):
                batches_to_do.append(batch)
                if ci_cache and ci_cache.is_pending(job, batch, num_batches):
                    if job in jobs_to_wait:
                        jobs_to_wait[job]["batches"].append(batch)
                    else:
                        jobs_to_wait[job] = {
                            "btaches": [batch],
                            "num_batches": num_batches,
                        }

        if batches_to_do:
            jobs_to_do.append(job)
            jobs_params[job] = {
                "batches": batches_to_do,
                "num_batches": num_batches,
            }
        else:
            jobs_to_skip.append(job)

    ## c. check CI controlling labels and commit messages
    if pr_labels:
        jobs_requested_by_label = []  # type: List[str]
        ci_controlling_labels = []  # type: List[str]
        for label in pr_labels:
            label_config = CI_CONFIG.get_label_config(label)
            if label_config:
                jobs_requested_by_label += label_config.run_jobs
                ci_controlling_labels += [label]
        if ci_controlling_labels:
            print(f"NOTE: CI controlling labels are set: [{ci_controlling_labels}]")
            print(
                f"    :   following jobs will be executed: [{jobs_requested_by_label}]"
            )
            jobs_to_do = [job for job in jobs_requested_by_label if job in jobs_to_do]

    if commit_tokens:
        jobs_to_do_requested = []  # type: List[str]

        # handle ci set tokens
        ci_controlling_tokens = [
            token for token in commit_tokens if token.startswith("#ci_set_")
        ]
        for token_ in ci_controlling_tokens:
            label_config = CI_CONFIG.get_label_config(token_)
            assert label_config, f"Unknonwn token [{token_}]"
            print(
                f"NOTE: CI controlling token: [{ci_controlling_tokens}], add jobs: [{label_config.run_jobs}]"
            )
            jobs_to_do_requested += label_config.run_jobs

        # handle specific job requests
        requested_jobs = [
            token[len("#job_") :]
            for token in commit_tokens
            if token.startswith("#job_")
        ]
        if requested_jobs:
            assert any(
                len(x) > 1 for x in requested_jobs
            ), f"Invalid job names requested [{requested_jobs}]"
            for job in requested_jobs:
                job_with_parents = CI_CONFIG.get_job_with_parents(job)
                print(
                    f"NOTE: CI controlling token: [#job_{job}], add jobs: [{job_with_parents}]"
                )
                # always add requested job itself, even if it could be skipped
                jobs_to_do_requested.append(job_with_parents[0])
                for parent in job_with_parents[1:]:
                    if parent in jobs_to_do and parent not in jobs_to_do_requested:
                        jobs_to_do_requested.append(parent)

        if jobs_to_do_requested:
            print(
                f"NOTE: Only specific job(s) were requested by commit message tokens: [{jobs_to_do_requested}]"
            )
            jobs_to_do = list(
                set(job for job in jobs_to_do_requested if job in jobs_to_do)
            )

    return {
        "digests": digests,
        "jobs_to_do": jobs_to_do,
        "jobs_to_skip": jobs_to_skip,
        "jobs_params": {
            job: params for job, params in jobs_params.items() if job in jobs_to_do
        },
    }


def _update_gh_statuses(indata: Dict, s3: S3Helper) -> None:
    job_digests = indata["jobs_data"]["digests"]
    ci_cache = CiCache(s3, job_digests).fetch_job_statuses()

    # create GH status
    pr_info = PRInfo()
    commit = get_commit(Github(get_best_robot_token(), per_page=100), pr_info.sha)

    def _run_create_status(job: str, batch: int, num_batches: int) -> None:
        job_status = ci_cache.pop(job, batch, num_batches)
        if not job_status:
            return
        print(f"Going to re-create GH status for job [{job}] sha [{pr_info.sha}]")
        assert job_status.status == "success", "BUG!"
        commit.create_status(
            state=job_status.status,
            target_url=job_status.report_url,
            description=format_description(
                f"Reused from [{job_status.pr_num}-{job_status.sha[0:8]}]: "
                f"{job_status.description}"
            ),
            context=get_check_name(job, batch=batch, num_batches=num_batches),
        )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for job in job_digests:
            if is_build_job(job):
                # no GH status for build jobs
                continue
            num_batches = CI_CONFIG.get_job_config(job).num_batches
            for batch in range(num_batches):
                future = executor.submit(_run_create_status, job, batch, num_batches)
                futures.append(future)
        done, _ = concurrent.futures.wait(futures)
        for future in done:
            try:
                _ = future.result()
            except Exception as e:
                raise e
    print("Going to update overall CI report")
    set_status_comment(commit, pr_info)
    print("... CI report update - done")


def _fetch_commit_tokens(message: str) -> List[str]:
    pattern = r"#[\w-]+"
    matches = re.findall(pattern, message)
    res = [
        match
        for match in matches
        if match == "#no_merge_commit"
        or match == "#no_ci_cache"
        or match.startswith("#job_")
        or match.startswith("#ci_set_")
    ]
    return res


def _upload_build_artifacts(
    pr_info: PRInfo,
    build_name: str,
    build_digest: str,
    job_report: JobReport,
    s3: S3Helper,
    s3_destination: str,
) -> str:
    # There are ugly artifacts for the performance test. FIXME:
    s3_performance_path = "/".join(
        (
            get_release_or_pr(pr_info, get_version_from_repo())[1],
            pr_info.sha,
            CI_CONFIG.normalize_string(build_name),
            "performance.tar.zst",
        )
    )
    performance_urls = []
    assert job_report.build_dir_for_upload, "Must be set for build job"
    performance_path = Path(job_report.build_dir_for_upload) / "performance.tar.zst"
    if performance_path.exists():
        performance_urls.append(
            s3.upload_build_file_to_s3(performance_path, s3_performance_path)
        )
        print(
            "Uploaded performance.tar.zst to %s, now delete to avoid duplication",
            performance_urls[0],
        )
        performance_path.unlink()
    build_urls = (
        s3.upload_build_directory_to_s3(
            Path(job_report.build_dir_for_upload),
            s3_destination,
            keep_dirs_in_s3_path=False,
            upload_symlinks=False,
        )
        + performance_urls
    )
    print("::notice ::Build URLs: {}".format("\n".join(build_urls)))
    log_path = Path(job_report.additional_files[0])
    log_url = ""
    if log_path.exists():
        log_url = s3.upload_build_file_to_s3(
            log_path, s3_destination + "/" + log_path.name
        )
    print(f"::notice ::Log URL: {log_url}")

    # generate and upload build report
    build_result = BuildResult(
        build_name,
        log_url,
        build_urls,
        job_report.version,
        job_report.status,
        int(job_report.duration),
        GITHUB_JOB_API_URL(),
    )
    result_json_path = build_result.write_json(Path(TEMP_PATH))
    s3_path = CiCache.get_s3_path(build_digest) + result_json_path.name
    build_report_url = s3.upload_file(
        bucket=S3_BUILDS_BUCKET, file_path=result_json_path, s3_path=s3_path
    )
    print(f"Report file [{result_json_path}] has been uploaded to [{build_report_url}]")

    # Upload head master binaries
    static_bin_name = CI_CONFIG.build_config[build_name].static_binary_name
    if pr_info.is_master() and static_bin_name:
        # Full binary with debug info:
        s3_path_full = "/".join((pr_info.base_ref, static_bin_name, "clickhouse-full"))
        binary_full = Path(job_report.build_dir_for_upload) / "clickhouse"
        url_full = s3.upload_build_file_to_s3(binary_full, s3_path_full)
        print(f"::notice ::Binary static URL (with debug info): {url_full}")

        # Stripped binary without debug info:
        s3_path_compact = "/".join((pr_info.base_ref, static_bin_name, "clickhouse"))
        binary_compact = Path(job_report.build_dir_for_upload) / "clickhouse-stripped"
        url_compact = s3.upload_build_file_to_s3(binary_compact, s3_path_compact)
        print(f"::notice ::Binary static URL (compact): {url_compact}")

    return log_url


def _upload_build_profile_data(
    pr_info: PRInfo,
    build_name: str,
    job_report: JobReport,
    git_runner: GitRunner,
    ch_helper: ClickHouseHelper,
) -> None:
    ci_logs_credentials = CiLogsCredentials(Path("/dev/null"))
    if ci_logs_credentials.host:
        instance_type = get_instance_type()
        instance_id = get_instance_id()
        query = f"""INSERT INTO build_time_trace
            (
                pull_request_number,
                commit_sha,
                check_start_time,
                check_name,
                instance_type,
                instance_id,
                file,
                library,
                time,
                pid,
                tid,
                ph,
                ts,
                dur,
                cat,
                name,
                detail,
                count,
                avgMs,
                args_name
            )
            SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}', *
            FROM input('
                file String,
                library String,
                time DateTime64(6),
                pid UInt32,
                tid UInt32,
                ph String,
                ts UInt64,
                dur UInt64,
                cat String,
                name String,
                detail String,
                count UInt64,
                avgMs UInt64,
                args_name String')
            FORMAT JSONCompactEachRow"""

        auth = {
            "X-ClickHouse-User": "ci",
            "X-ClickHouse-Key": ci_logs_credentials.password,
        }
        url = f"https://{ci_logs_credentials.host}/"
        profiles_dir = Path(TEMP_PATH) / "profiles_source"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        print(
            "Processing profile JSON files from %s",
            Path(REPO_COPY) / "build_docker",
        )
        git_runner(
            "./utils/prepare-time-trace/prepare-time-trace.sh "
            f"build_docker {profiles_dir.absolute()}"
        )
        profile_data_file = Path(TEMP_PATH) / "profile.json"
        with open(profile_data_file, "wb") as profile_fd:
            for profile_source in profiles_dir.iterdir():
                if profile_source.name != "binary_sizes.txt":
                    with open(profiles_dir / profile_source, "rb") as ps_fd:
                        profile_fd.write(ps_fd.read())

        print(
            "::notice ::Log Uploading profile data, path: %s, size: %s, query: %s",
            profile_data_file,
            profile_data_file.stat().st_size,
            query,
        )
        ch_helper.insert_file(url, auth, query, profile_data_file)

        query = f"""INSERT INTO binary_sizes
            (
                pull_request_number,
                commit_sha,
                check_start_time,
                check_name,
                instance_type,
                instance_id,
                file,
                size
            )
            SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}', file, size
            FROM input('size UInt64, file String')
            SETTINGS format_regexp = '^\\s*(\\d+) (.+)$'
            FORMAT Regexp"""

        binary_sizes_file = profiles_dir / "binary_sizes.txt"

        print(
            "::notice ::Log Uploading binary sizes data, path: %s, size: %s, query: %s",
            binary_sizes_file,
            binary_sizes_file.stat().st_size,
            query,
        )
        ch_helper.insert_file(url, auth, query, binary_sizes_file)


def _run_test(job_name: str, run_command: str) -> int:
    assert (
        run_command or CI_CONFIG.get_job_config(job_name).run_command
    ), "Run command must be provided as input argument or be configured in job config"

    if not run_command:
        if CI_CONFIG.get_job_config(job_name).timeout:
            os.environ["KILL_TIMEOUT"] = str(CI_CONFIG.get_job_config(job_name).timeout)
        run_command = "/".join(
            (os.path.dirname(__file__), CI_CONFIG.get_job_config(job_name).run_command)
        )
        if ".py" in run_command and not run_command.startswith("python"):
            run_command = "python3 " + run_command
        print("Use run command from a job config")
    else:
        print("Use run command from the workflow")
    os.environ["CHECK_NAME"] = job_name
    print(f"Going to start run command [{run_command}]")
    process = subprocess.run(
        run_command,
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        check=False,
        shell=True,
    )

    if process.returncode == 0:
        print(f"Run action done for: [{job_name}]")
        exit_code = 0
    else:
        print(
            f"Run action failed for: [{job_name}] with exit code [{process.returncode}]"
        )
        exit_code = process.returncode
    return exit_code


def _get_ext_check_name(check_name: str) -> str:
    run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
    run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
    if run_by_hash_total > 1:
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        check_name_with_group = check_name
    return check_name_with_group


def main() -> int:
    exit_code = 0
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    args = parse_args(parser)

    if args.mark_success or args.pre or args.post or args.run:
        assert args.infile, "Run config must be provided via --infile"
        assert args.job_name, "Job name must be provided via --job-name"

    indata: Optional[Dict[str, Any]] = None
    if args.infile:
        indata = (
            json.loads(args.infile)
            if not os.path.isfile(args.infile)
            else json.load(open(args.infile))
        )
        assert indata and isinstance(indata, dict), "Invalid --infile json"

    result: Dict[str, Any] = {}
    s3 = S3Helper()
    pr_info = PRInfo()
    git_runner = GitRunner(set_cwd_to_git_root=True)

    ### CONFIGURE action: start
    if args.configure:
        # if '#no_merge_commit' is set in commit message - set git ref to PR branch head to avoid merge-commit
        tokens = []
        if (pr_info.number != 0 and not args.skip_jobs) or args.commit_message:
            message = args.commit_message or git_runner.run(
                f"{GIT_PREFIX} log {pr_info.sha} --format=%B -n 1"
            )
            tokens = _fetch_commit_tokens(message)
            print(f"Found commit message tokens: [{tokens}]")
            if "#no_merge_commit" in tokens and CI:
                git_runner.run(f"{GIT_PREFIX} checkout {pr_info.sha}")
                git_ref = git_runner.run(f"{GIT_PREFIX} rev-parse HEAD")
                print(
                    "#no_merge_commit is set in commit message - Setting git ref to PR branch HEAD to not use merge commit"
                )

        docker_data = {}
        git_ref = git_runner.run(f"{GIT_PREFIX} rev-parse HEAD")

        # let's get CH version
        version = get_version_from_repo(git=Git(True)).string
        print(f"Got CH version for this commit: [{version}]")

        docker_data = (
            _configure_docker_jobs(
                args.rebuild_all_docker, args.docker_digest_or_latest
            )
            if not args.skip_docker
            else {}
        )

        job_digester = JobDigester()
        build_digest = job_digester.get_job_digest(
            CI_CONFIG.get_digest_config("package_release")
        )
        docs_digest = job_digester.get_job_digest(
            CI_CONFIG.get_digest_config("Docs check")
        )
        jobs_data = (
            _configure_jobs(
                job_digester,
                s3,
                args.rebuild_all_binaries,
                pr_info.labels,
                tokens,
                pr_info.is_master(),
            )
            if not args.skip_jobs
            else {}
        )

        # FIXME: Early style check manipulates with job names might be not robust with await feature
        # if pr_info.number != 0 and not args.docker_digest_or_latest:
        #     # FIXME: it runs style check before docker build if possible (style-check images is not changed)
        #     #    find a way to do style check always before docker build and others
        #     _check_and_update_for_early_style_check(jobs_data, docker_data)
        if pr_info.has_changes_in_documentation_only():
            _update_config_for_docs_only(jobs_data)

        # wait for pending jobs to be finished, await_jobs is a long blocking call if any job has to be awaited
        ci_cache = CiCache(s3, jobs_data["digests"])
        awaited_jobs = ci_cache.await_jobs(jobs_data.get("jobs_to_wait", {}))
        for job in awaited_jobs:
            jobs_to_do = jobs_data["jobs_to_do"]
            if job in jobs_to_do:
                jobs_to_do.remove(job)
            else:
                assert False, "BUG"

        # set planned jobs as pending in the CI cache if on the master
        if pr_info.is_master():
            for job in jobs_data["jobs_to_do"]:
                config = CI_CONFIG.get_job_config(job)
                if config.run_always or config.run_by_label:
                    continue
                job_params = jobs_data["jobs_to_do"]["jobs_params"][job]
                ci_cache.push_pending(
                    job,
                    job_params["batch"],
                    job_params["num_batches"],
                    state=PendingState.InternalState.PLANNED,
                )

        # conclude results
        result["git_ref"] = git_ref
        result["version"] = version
        result["build"] = build_digest
        result["docs"] = docs_digest
        result["jobs_data"] = jobs_data
        result["docker_data"] = docker_data
    ### CONFIGURE action: end

    ### PRE action: start
    elif args.pre:
        CommitStatusData.cleanup()
        JobReport.cleanup()

        if is_test_job(args.job_name):
            assert indata, "Run config must be provided via --infile"
            report_path = Path(REPORT_PATH)
            report_path.mkdir(exist_ok=True, parents=True)
            path = CiCache.get_s3_path(indata["build"])
            files = s3.download_files(  # type: ignore
                bucket=S3_BUILDS_BUCKET,
                s3_path=path,
                file_suffix=".json",
                local_directory=report_path,
            )
            print(
                f"Pre action done. Report files [{files}] have been downloaded from [{path}] to [{report_path}]"
            )
        else:
            print(f"Pre action done. Nothing to do for [{args.job_name}]")
    ### PRE action: end

    ### RUN action: start
    elif args.run:
        check_name = args.job_name
        check_name_with_group = _get_ext_check_name(check_name)
        print(
            f"Check if rerun for name: [{check_name}], extended name [{check_name_with_group}]"
        )
        commit = get_commit(Github(get_best_robot_token(), per_page=100), pr_info.sha)
        rerun_helper = RerunHelper(commit, check_name_with_group)
        if rerun_helper.is_already_finished_by_status():
            status = rerun_helper.get_finished_status()
            print(f"Check is already finished with status [{status}]")
            if status is not None and status.state == "success":
                exit_code = 0
            else:
                exit_code = 1
        else:
            exit_code = _run_test(check_name, args.run_command)
    ### RUN action: end

    ### POST action: start
    elif args.post:
        assert indata
        ch_helper = ClickHouseHelper()
        job_report = JobReport.load() if JobReport.exist() else None

        check_url = ""
        if is_build_job(args.job_name):
            assert job_report, "Job Report must be present for a build job"
            build_name = args.job_name
            s3_path_prefix = "/".join(
                (
                    get_release_or_pr(pr_info, get_version_from_repo())[0],
                    pr_info.sha,
                    build_name,
                )
            )
            log_url = _upload_build_artifacts(
                pr_info,
                build_name,
                build_digest=indata["build"],
                job_report=job_report,
                s3=s3,
                s3_destination=s3_path_prefix,
            )
            _upload_build_profile_data(
                pr_info, build_name, job_report, git_runner, ch_helper
            )
            check_url = log_url
        else:
            # FIXME: switch all jobs to job report and do the following actions uncoditionally
            if job_report:
                additional_urls = []
                s3_path_prefix = "/".join(
                    (
                        get_release_or_pr(pr_info, get_version_from_repo())[0],
                        pr_info.sha,
                        CI_CONFIG.normalize_string(
                            job_report.check_name or _get_ext_check_name(args.job_name)
                        ),
                    )
                )
                if job_report.build_dir_for_upload:
                    additional_urls = s3.upload_build_directory_to_s3(
                        Path(job_report.build_dir_for_upload),
                        s3_path_prefix,
                        keep_dirs_in_s3_path=False,
                        upload_symlinks=False,
                    )
                if job_report.test_results or job_report.additional_files:
                    check_url = upload_result_helper.upload_results(
                        s3,
                        pr_info.number,
                        pr_info.sha,
                        job_report.test_results,
                        job_report.additional_files,
                        job_report.check_name or args.job_name,
                        additional_urls=additional_urls or None,
                    )
                commit = get_commit(
                    Github(get_best_robot_token(), per_page=100), pr_info.sha
                )
                post_commit_status(
                    commit,
                    job_report.status,
                    check_url,
                    format_description(job_report.description),
                    job_report.check_name or args.job_name,
                    pr_info,
                    dump_to_file=True,
                )
                update_mergeable_check(
                    commit,
                    pr_info,
                    job_report.check_name or _get_ext_check_name(args.job_name),
                )

        # FIXME: switch all jobs to job report and do the following actions uncoditionally
        if job_report:
            print(f"Job report url: [{check_url}]")
            prepared_events = prepare_tests_results_for_clickhouse(
                pr_info,
                job_report.test_results,
                job_report.status,
                job_report.duration,
                job_report.start_time,
                check_url or "",
                job_report.check_name or args.job_name,
            )
            ch_helper.insert_events_into(
                db="default", table="checks", events=prepared_events
            )
    ### POST action: end

    ### MARK SUCCESS action: start
    elif args.mark_success:
        assert indata, "Run config must be provided via --infile"
        job = args.job_name
        job_config = CI_CONFIG.get_job_config(job)
        num_batches = job_config.num_batches
        ci_cache = CiCache(s3, indata["jobs_data"]["digests"])
        assert (
            num_batches <= 1 or 0 <= args.batch < num_batches
        ), f"--batch must be provided and in range [0, {num_batches}) for {job}"

        # FIXME: find generic design for propagating and handling job status (e.g. stop using statuses in GH api)
        #   now job ca be build job w/o status data, any other job that exit with 0 with or w/o status data
        if is_build_job(job):
            # there is no status for build jobs
            # create dummy success to mark it as done
            # FIXME: consider creating commit status for build jobs too, to treat everything the same way
            CommitStatusData("success", "dummy description", "dummy_url").dump_status()

        job_status = None
        if CommitStatusData.exist():
            # normal scenario
            job_status = CommitStatusData.load_status()
        else:
            # apparently exit after rerun-helper check
            # do nothing, exit without failure
            print(f"ERROR: no status file for job [{job}]")

        if job_config.run_always or job_config.run_by_label:
            print(f"Job [{job}] runs always or by label in CI - do not cache")
        else:
            # delete pending status in the cache if any
            if pr_info.is_master():
                ci_cache.delete_pending(job, args.batch, num_batches)
            if job_status and job_status.is_ok():
                # push success job status to the cache
                ci_cache.push(
                    job, args.batch, num_batches, job_status, pr_info.is_master()
                )
                print(f"Job [{job}] is ok")
            elif job_status:
                print(f"Job [{job}] is not ok, status [{job_status.status}]")
    ### MARK SUCCESS action: end

    elif args.update_gh_statuses:
        assert indata, "Run config must be provided via --infile"
        _update_gh_statuses(indata=indata, s3=s3)

    ### print results
    if args.outfile:
        with open(args.outfile, "w") as f:
            if isinstance(result, str):
                print(result, file=f)
            elif isinstance(result, dict):
                print(json.dumps(result, indent=2 if args.pretty else None), file=f)
            else:
                raise AssertionError(f"Unexpected type for 'res': {type(result)}")
    else:
        if isinstance(result, str):
            print(result)
        elif isinstance(result, dict):
            print(json.dumps(result, indent=2 if args.pretty else None))
        else:
            raise AssertionError(f"Unexpected type for 'res': {type(result)}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
