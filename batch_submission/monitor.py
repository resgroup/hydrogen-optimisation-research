from typing import List
from time import sleep
from datetime import datetime
import numpy as np
import azure.batch.models as batchmodels

from batch_submission.config import POOL_ID, MONITOR_SLEEP_TIME_S
from batch_submission.batch_submission import BatchSubmission
from examples.azure_batch.batch_downloader import BatchDownloader

BAD_STATES = [
    batchmodels.ComputeNodeState.unusable,
    batchmodels.ComputeNodeState.preempted,
    batchmodels.ComputeNodeState.unknown,
    batchmodels.ComputeNodeState.start_task_failed,
]


class Monitor:
    def __init__(self, batch_job: BatchSubmission):
        self.batch_job = batch_job
        self.blob_service_client = self.batch_job.blob_service_client
        self.batch_service_client = self.batch_job.batch_service_client

        self.pool_id: list = POOL_ID
        self.job_ids: list = self.batch_job.job_ids

    def _list_tasks(self, job_id: str) -> List[batchmodels.CloudTask]:
        tasks = self.batch_service_client.task.list(job_id=job_id)
        return list(tasks)

    def _get_all_tasks(self) -> List[batchmodels.CloudTask]:
        task_list = []
        for job_id in self.job_ids:
            task_list += self._list_tasks(job_id=job_id)
        return task_list

    def _list_compute_nodes(self) -> List[batchmodels.ComputeNode]:
        nodes = self.batch_service_client.compute_node.list(
            pool_id=self.pool_id,
        )
        return list(nodes)

    def _find_failed_nodes(self) -> List[batchmodels.ComputeNode]:
        nodes = self._list_compute_nodes()
        bad_nodes = [node for node in nodes if node.state in BAD_STATES]
        return bad_nodes

    def _resize_pool(
        self,
        target_low_priority_nodes: int,
        target_dedicated_nodes: int = 0,
    ) -> None:
        self.batch_service_client.pool.resize(
            pool_id=self.pool_id,
            pool_resize_parameter=batchmodels.PoolResizeParameter(
                target_dedicated_nodes=target_dedicated_nodes,
                target_low_priority_nodes=target_low_priority_nodes,
            ),
        )

    def _launch_nodes(self) -> None:
        pool = self.batch_service_client.pool.get(pool_id=self.pool_id)
        original_low_priority_node_count = pool.target_low_priority_nodes
        original_dedicated_node_count = pool.target_dedicated_nodes
        self._resize_pool(
            target_low_priority_nodes=0,
            target_dedicated_nodes=0,
        )
        # Wait for nodes to be de-allocated
        while True:
            pool = self.batch_service_client.pool.get(pool_id=self.pool_id)
            if pool.current_dedicated_nodes == 0 and pool.current_low_priority_nodes == 0:
                self._resize_pool(
                    target_low_priority_nodes=original_low_priority_node_count,
                    target_dedicated_nodes=original_dedicated_node_count,
                )
                break
            sleep(15)

    def _relaunch_nodes(self, n_failed_nodes) -> None:
        pool = self.batch_service_client.pool.get(pool_id=self.pool_id)
        original_low_priority_node_count = pool.target_low_priority_nodes
        original_dedicated_node_count = pool.target_dedicated_nodes
        self._resize_pool(
            target_low_priority_nodes=0,
            target_dedicated_nodes=0,
        )
        # Wait for nodes to be de-allocated
        while True:
            pool = self.batch_service_client.pool.get(pool_id=self.pool_id)
            if pool.current_dedicated_nodes == 0 and pool.current_low_priority_nodes == 0:
                self._resize_pool(
                    target_low_priority_nodes=n_failed_nodes,
                    target_dedicated_nodes=0,
                )
                break
            sleep(15)

    @staticmethod
    def _build_timing_dict(task_time_s: float) -> dict:
        return {
            "total": {
                "seconds": sum(task_time_s),
                "minutes": sum(task_time_s) / 60,
                "hours": sum(task_time_s) / 3600,
                "days": sum(task_time_s) / 3600 / 24,
            },
            "mean": {
                "seconds": np.mean(task_time_s),
                "minutes": np.mean(task_time_s) / 60,
            },
            "minimum": {
                "seconds": np.min(task_time_s),
                "minutes": np.min(task_time_s) / 60,
            },
            "maximum": {
                "seconds": np.max(task_time_s),
                "minutes": np.max(task_time_s) / 60,
            },
            "stdev": {
                "seconds": np.std(task_time_s),
                "minutes": np.std(task_time_s) / 60,
            },
        }

    def calc_stats(self, task_list: List[batchmodels.CloudTask]) -> dict:
        active_tasks = [
            t for t in task_list if t.state == batchmodels.TaskState.active]
        preparing_tasks = [
            t for t in task_list if t.state == batchmodels.TaskState.preparing]
        running_tasks = [
            t for t in task_list if t.state == batchmodels.TaskState.running]
        completed_tasks = [
            t for t in task_list if t.state == batchmodels.TaskState.completed]
        succeeded_tasks = [
            t for t in completed_tasks if t.execution_info.exit_code == 0]
        task_time_s = [(t.execution_info.end_time -
                        t.execution_info.start_time).total_seconds() for t in completed_tasks]
        return {
            "counts": {
                "total_tasks": len(task_list),
                "active_tasks": len(active_tasks),
                "preparing_tasks": len(preparing_tasks),
                "running_tasks": len(running_tasks),
                "completed_tasks": len(completed_tasks),
                "successful_tasks": len(succeeded_tasks),
            },
            "percentages": {
                "active_tasks": len(active_tasks) / len(task_list),
                "preparing_tasks": len(preparing_tasks) / len(task_list),
                "running_tasks": len(running_tasks) / len(task_list),
                "completed_tasks": len(completed_tasks) / len(task_list),
                "successful_tasks": (
                    len(succeeded_tasks) / len(completed_tasks) if
                    len(completed_tasks) > 0 else 0,
                ),
            },
            "timing": (
                self._build_timing_dict(task_time_s) if
                len(succeeded_tasks) > 0 else None,
            ),
        }

    def run(
        self,
        print_output: bool = True,
        sleep_time_s: int = MONITOR_SLEEP_TIME_S,
        analysis_name=None,
        combinations=None,
        n_best_results_download=0,
    ) -> None:
        while True:
            failed_nodes = self._find_failed_nodes()
            tasks = self._get_all_tasks()
            stats = self.calc_stats(
                task_list=tasks,
            )
            if print_output:
                print(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - '
                    f'{len(failed_nodes)} failed nodes - '
                    f'{stats.get("percentages", {}).get("completed_tasks", 0):.2%}% Completed'
                )

            if failed_nodes:
                print(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Relaunching Nodes!'
                )
                self._relaunch_nodes(len(failed_nodes))

            if stats.get("percentages", {}).get("completed_tasks") == 1.0:

                print('Downloading batch results...')

                batch_downloader = BatchDownloader(analysis_name=analysis_name)
                results = batch_downloader.download_results(combinations=combinations)
                results.to_csv('batch_results/batch_results_temp.csv')

                # extract rows from results that have the lowest five lcoh2 values:
                best_results = results.nsmallest(n_best_results_download, 'lcoh2').reset_index()
                for i in range(n_best_results_download):
                    combination = best_results.loc[i, 'combination']
                    annual_results = batch_downloader.download_annual_results(combination=combination)
                    annual_results.to_csv(f'batch_results/annual_results_{combination}.csv')
                    success = True
                    run_number = 0
                    while success:
                        try:
                            full_timeseries = batch_downloader.download_full_timeseries(combination=combination,
                                                                                        run_number=run_number)
                            full_timeseries.to_csv(f'batch_results/full_timeseries_{combination}_{run_number}.csv')
                        except:
                            success = False

                        run_number += 1

                self._resize_pool(
                    target_low_priority_nodes=0,
                    target_dedicated_nodes=0,
                )

                break
            sleep(sleep_time_s)
