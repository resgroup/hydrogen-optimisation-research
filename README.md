# hydrogen-optimisation-research

### Getting Started

- Create or update a **conda** environment based on `environment.yml`:

  ```
  conda env create -f environment.yml
  conda activate hoptimiser
  ``````
- From the command line in the root of the repo, run:

  `python -m examples.standalone_electrolyzer_with_storage.simulation_engine`

### Running with Azure Batch

- Copy the `batch_submission` package from [Azure Batch](https://github.com/resgroup/AzureBatch) into the project root, using `v0.7.2` or higher.
- Configure the following `ENVIRONMENT` variables within the `.env` file in the 
project root:
```python
BATCH_ACCOUNT_NAME=
BATCH_ACCOUNT_URL=
BATCH_ACCOUNT_KEY=

STORAGE_ACCOUNT_NAME=
STORAGE_ACCOUNT_KEY=

POOL_ID=HoptimiserPool
JOB_ID=HoptimiserJob
MAX_WALL_CLOCK_TIME_MINUTES=10
MAX_TASK_RETRY_COUNT=1
LOW_PRIORITY_POOL_NODE_COUNT=4
```
- Modify the `ez_storage_kwh_inputs` and `ez_power_kw_inputs` input parameters 
within the `__main__` method of `batch_runner.py` as desired to specify the 
desired solution space to explore.
- Utilize the `batch_downloader.py` script to download a summary of results available on blob storage or a particular run.
- Execute with: `python -m examples.azure_batch.batch_runner`