# pylint: disable=missing-module-docstring

import asyncio
import gzip
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict
from itertools import starmap
from pathlib import Path
from typing import Any, Iterable, Set

import aiofiles
import backoff
import simplejson as json
from aiohttp import ClientError, ClientResponseError, ClientSession
from autologging import logged
from aws_lambda_powertools import Logger
from funcy import chunks, lcat

from environment import (
    ASYNC_CONCURRENT_REQUESTS,
    BATCH_CALL_LIMIT,
    DEFAULT_REQUEST_TIMEOUT,
    PROVIDER,
    QUICKNODE_API_KEY,
    SERVICE_NAME,
    TEMP_DIR,
)
from utils import (
    AsyncTimedContextManger,
    BackoffHandlers,
    JsonRpcMetadata,
    Metadata,
    Node,
    Timed,
    UtilsMixIn,
)

logger = Logger(service=SERVICE_NAME, child=True)

for (
    name
) in (
    logging.Logger.manager.loggerDict.keys()  # pylint: disable=consider-iterating-dictionary
):
    if "snowflake" in name:
        logging.getLogger(name).propagate = False

node_provider = Node(
    host=PROVIDER,
    auth=None,
    headers={
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
        "Content-Encoding": "gzip",
    },
)

ASYNC_WAIT_TIMEOUT = 600
BACKOFF_MAX_TIME = DEFAULT_REQUEST_TIMEOUT


@logged(logger)
class BaseClient(UtilsMixIn, ABC):
    # pylint: disable=no-member
    """Base Client that contains generic functions used by more than one child class"""

    CONCURRENT_REQUESTS: int = ASYNC_CONCURRENT_REQUESTS
    # file_map: dict = {}

    def __init__(
        self,
        env: str,
        meta: Metadata,
        result_batches: str,
    ) -> None:
        self.temp_dir = Path(TEMP_DIR)
        self.files: Set[Path] = set()
        self.metadata = meta
        self.pending = set()
        self.env = env
        self.result_batches = result_batches

        self.semaphore: asyncio.BoundedSemaphore = None

    @Timed(log_fn=logger.info)
    def load_work(self):
        """Load Work"""
        self.execute_work()

    @Timed(log_fn=logger.info)
    def execute_work(self):
        """Asynchronously start the work"""
        asyncio.run(self._do_work())

    @abstractmethod
    async def _do_work(self) -> Set[asyncio.Task]:
        """
        Asynchronously fetch and do the work
        """

    def _set_query_results(self) -> int:

        row_count = sum([len(batch) for batch in self.result_batches])
        self.__log.info(
            f"Processed {row_count:,} rows",
            extra={
                "row_count": row_count,
                "batches": self.result_batches,
            },
        )
        return row_count

    @Timed(log_fn=logger.debug, start_msg="")
    def update_external_table_metadata(self, files: Iterable[Path]) -> None:
        """Update the external table metadata"""
        sql = self.sql_refresh.format(files="','".join([str(f) for f in files]))
        self.__log.info(f"Query: [{sql}]")
        results = self.cursor.execute(sql).fetchall()
        for row in results:
            details = {
                column.name: row[i] for i, column in enumerate(self.cursor.description)
            }
            if details["status"] == "REGISTERED_NEW":
                self.__log.info(
                    details["status"],
                    extra=self.rename_dict_keys(prefix_suffix="_", obj=details),
                )
            elif details["status"] == "REGISTER_SKIPPED":
                self.__log.info(
                    details["status"],
                    extra=self.rename_dict_keys(prefix_suffix="_", obj=details),
                )
            else:
                self.__log.error(
                    details["status"],
                    extra=self.rename_dict_keys(prefix_suffix="_", obj=details),
                )

    async def _cancel_pending_tasks(self, tasks, pending) -> None:
        for task in pending:
            task.cancel()
        if pending:
            self.__log.warning(
                "%d of %d tasks timed out and were cancelled.",
                len(pending),
                len(tasks),
            )

    async def _dump(self, stem: str, data: object) -> None:
        """Dumps data to a temp file"""

        path = self.temp_dir / Path(f"{stem}.json.gz")
        self.files |= {path}
        await self._do_dump(path, data)

    async def _do_dump(self, file: Path, data: object):
        async with aiofiles.open(file.absolute(), mode="ab") as fs_:
            await fs_.write(
                gzip.compress(
                    (
                        json.dumps(
                            obj=data, bigint_as_string=True, separators=(",", ":")
                        )
                        + "\n"
                    ).encode("utf-8")
                )
            )

    def _get_partition(self, block) -> int:
        """Partition by block_number into groups of 10^5"""
        block_number = int(block)
        return round(block_number, -5)

    def log_file_sizes(self):
        """Log the sizes of the files passed in"""
        for file in self.files:
            if file.exists():
                self.__log.debug(
                    f"File size for file {file} is {int(os.stat(file).st_size/1000000)} MBs"
                )
            else:
                self.__log.warning(
                    f"File does not exist: {file}", extra={"file": file.as_posix()}
                )


@logged(logger)
class BatchJsonRpcClient(BaseClient):
    """Hits Json RPC Node to read smart contract data"""

    node: Node = node_provider

    def __init__(self, env: str, meta: Metadata, result_batches: str) -> None:
        super().__init__(env, meta, result_batches)
        self.results = []

    async def _do_work(self) -> Set[asyncio.Task]:
        tasks = []

        self.semaphore = (
            # Semaphore must be initialized inside the event loop
            asyncio.BoundedSemaphore(value=self.CONCURRENT_REQUESTS)
        )
        self._set_query_results()

        for file in os.scandir(TEMP_DIR):
            if file.path.endswith(".json.gz"):
                os.remove(file.path)

        async with ClientSession(
            headers=self.node.headers,
            raise_for_status=True,
            base_url=self.node.host,
        ) as session:

            for batch in chunks(BATCH_CALL_LIMIT, self.result_batches):

                tasks.append(
                    asyncio.create_task(
                        self._execute_batch_rpc_call(
                            session=session,
                            request_data=list(starmap(self.create_rpc_request, batch)),
                        )
                    )
                )

            results, pending = await asyncio.wait(tasks, timeout=ASYNC_WAIT_TIMEOUT)
            await self._cancel_pending_tasks(tasks, pending)
            self.results = lcat([x.result() for x in results])
            self.log_file_sizes()

    @backoff.on_exception(
        backoff.expo,
        (ClientError, asyncio.TimeoutError, ClientResponseError),
        giveup=BackoffHandlers.give_up,
        logger=logger.name,
        max_time=BACKOFF_MAX_TIME,
        on_backoff=BackoffHandlers.log_backoff_event,
        on_giveup=BackoffHandlers.log_giveup_event,
        raise_on_giveup=False,
        backoff_log_level=logging.DEBUG,
        giveup_log_level=logging.DEBUG,
    )
    async def _send_batch_request(
        self, session: ClientSession, requests: Iterable[dict]
    ) -> Iterable[dict]:
        if self.node.headers.get("Content-Encoding", {}) in ("gzip", "GZIP"):
            payload = {"data": gzip.compress(bytes(json.dumps(requests), "utf-8"))}
        else:
            payload = {"json": requests}

        async with AsyncTimedContextManger(
            log_fn=self.__log.info  # pylint: disable=no-member
        ), session.request("POST", url=QUICKNODE_API_KEY, **payload) as response:
            responses = await response.json()
        return responses

    def _get_request_metadata(self, session, request_) -> dict:
        return asdict(
            JsonRpcMetadata(
                host=session._base_url.host,  # pylint: disable=protected-access
                request=request_,
                **asdict(self.metadata),
            )
        )

    async def _execute_batch_rpc_call(
        self, session: ClientSession, request_data: Iterable
    ) -> list:
        async with self.semaphore:
            responses = await self._send_batch_request(
                session=session, requests=request_data
            )
            request_lookup = {request["id"]: request for request in request_data}

            transformed = self._transform_responses(session, responses, request_lookup)

            results = []
            for result in transformed:
                await self._dump(self.get_temp_file_stem(result), result)
                results.append(result)

            return results


    def get_temp_file_stem(self, result):
        """Override to change file stem of temp files"""
        return int(result["data"]["id"].split("-")[-1])

    @staticmethod
    def pad_string(
        prefix: str = "", suffix: str = "", max_length: int = 74, char: str = "0"
    ) -> str:
        """
        Pad a string (prefix, suffix, both) with a given character to a given length

        Example:
        ```python
        pad_sting("0x", "1234", 64, "0")
        >>> "0x0000000000000000000000000000000000000000000000000000000000001234"
        ```
        """
        padding = char * max_length
        start = len(prefix)
        end = max_length - len(suffix)
        return f"{prefix}{padding[start:end]}{suffix}"

    @abstractmethod
    def create_rpc_request(self) -> dict:
        """
        Create the RPC payload for the call to the contract
        args are passed in by position, so the first argument is the first argument
        returns a dictionary following JSON RPC 2.0 spec
        """

    @abstractmethod
    def _transform_responses(self, session, responses, requests: dict) -> Any:
        """
        Transform the responses from the RPC call into the proper format and add metadata.
        """
