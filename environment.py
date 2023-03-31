# pylint: disable=missing-module-docstring
import os
from tempfile import gettempdir

from dotenv import load_dotenv

if os.getenv("ENV", "").upper() == "LOCAL":
    load_dotenv()

ENV = os.getenv("ENV")

DEBUG = True if os.getenv("DEBUG") else None
TEMP_DIR = os.getenv("DEBUG_TMP_DIR", gettempdir())

DEFAULT_REQUEST_TIMEOUT = int(os.getenv("DEFAULT_REQUEST_TIMEOUT", "180"))
BATCH_CALL_LIMIT = int(os.getenv("BATCH_CALL_LIMIT", "16500"))


SERVICE_NAME = os.getenv("POWERTOOLS_SERVICE_NAME")


# NODE
QUICKNODE_API_KEY=os.getenv("QUICKNODE_API_KEY", "53fa4e1be29c13e2cf63de4bbf2366cfc752ffe9/")
QUICKNODE_SERVICE=os.getenv("QUICKNODE_SERVICE", f"https://skilled-thrilling-sheet.quiknode.pro/{QUICKNODE_API_KEY}")
PROVIDER= os.getenv("PROVIDER", "")

PRODUCER_LIMIT_SIZE = int(os.getenv("PRODUCER_LIMIT_SIZE", "1650000"))
PRODUCER_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "165000"))
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "16500"))
BATCH_CHUNKS_SIZE = int(os.getenv("BATCH_CHUNKS_SIZE", "100"))


# this should be set to the max rate allowed by the source
ASYNC_CONCURRENT_REQUESTS = int(os.getenv("ASYNC_CONCURRENT_REQUESTS", "1"))

