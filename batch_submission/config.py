import os
import datetime

from batch_submission.pool import VmSize

from dotenv import load_dotenv
load_dotenv()

BATCH_ACCOUNT_NAME = os.environ.get("BATCH_ACCOUNT_NAME")
BATCH_ACCOUNT_KEY = os.environ.get("BATCH_ACCOUNT_KEY")
BATCH_ACCOUNT_URL = os.environ.get("BATCH_ACCOUNT_URL")

STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.environ.get("STORAGE_ACCOUNT_KEY")
STORAGE_ACCOUNT_DOMAIN = os.environ.get(
    "STORAGE_ACCOUNT_DOMAIN", "blob.core.windows.net")
STORAGE_ACCOUNT_SAS_TOKEN_TIMEOUT_HOURS = int(
    os.environ.get("STORAGE_ACCOUNT_SAS_TOKEN_TIMEOUT_HOURS", 24 * 7))

DEDICATED_POOL_NODE_COUNT = os.environ.get("DEDICATED_POOL_NODE_COUNT", 0)
LOW_PRIORITY_POOL_NODE_COUNT = os.environ.get(
    "LOW_PRIORITY_POOL_NODE_COUNT", 1)
POOL_VM_SIZE = os.environ.get("POOL_VM_SIZE", VmSize.STANDARD_DS1_v2)

POOL_ID = os.environ.get("POOL_ID")
JOB_ID = os.environ.get("JOB_ID")
MAX_WALL_CLOCK_TIME = datetime.timedelta(minutes=int(
    os.environ.get("MAX_WALL_CLOCK_TIME_MINUTES", 60 * 24)))
MAX_TASK_RETRY_COUNT = os.environ.get("MAX_TASK_RETRY_COUNT")
STANDARD_OUT_FILE_NAME = os.environ.get("STANDARD_OUT_FILE_NAME", 'stdout.txt')
MONITOR_SLEEP_TIME_S = os.environ.get("MONITOR_SLEEP_TIME_S", 600)
