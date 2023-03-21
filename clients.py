# pylint: disable=missing-module-docstring

import asyncio
import gzip
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict
from itertools import starmap
from pathlib import Path
from typing import Any, Iterable, Set, Union

import aiofiles
import backoff
import simplejson as json
from aiohttp import ClientError, ClientResponseError, ClientSession
from autologging import logged
from aws_lambda_powertools import Logger
from funcy import cat, chunks

from environment import (
    ASYNC_CONCURRENT_REQUESTS,
    BATCH_CALL_LIMIT,
    DEFAULT_REQUEST_TIMEOUT,
    QUICKNODE_SERVICE,
    SERVICE_NAME,
    TEMP_DIR,
)
from utils import (
    AsyncTimedContextManger,
    BackoffHandlers,
    JsonRpcMetadata,
    Metadata,
    Node,
    RestAPIMetadata,
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

quicknode_ethereum = Node(
    host=QUICKNODE_SERVICE,
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
        self, env: str, meta: Metadata, work_items: str
    ) -> None:
        self.temp_dir = Path(TEMP_DIR)
        self.files: Set[Path] = set()
        self.metadata = meta
        self.pending = set()
        self.env = env
        self.work_items = self.deserialize(work_items)
        self.semaphore: asyncio.BoundedSemaphore = None

    @Timed(log_fn=logger.info)
    def fetch(self):
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
        query_id = self.query_id
        self.__log.info(
            f"Retrieving results for query id: {query_id}", extra={"query_id": query_id}
        )
        if hasattr(self.work_items[0], "rowcount"):
            row_count = sum([batch.rowcount for batch in self.work_items])
        else:
            row_count = sum([len(batch) for batch in self.work_items])
        self.__log.info(
            f"Query id: {query_id} returned {row_count:,} rows",
            extra={
                "query_id": query_id,
                "row_count": row_count,
                "batches": self.work_items,
            },
        )
        return row_count

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
class RestClient(BaseClient):
    """Client to help fetch from a Rest API"""

    node: Node = quicknode_ethereum

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
            raise_for_status=True, base_url=self.base_url  # pylint: disable=no-member
        ) as session:

            for current_calls in chunks(
                self.CONCURRENT_REQUESTS, cat(self.work_items)
            ):

                for cur_request_data in current_calls:
                    tasks.append(
                        asyncio.create_task(
                            self._execute_api_call(
                                session=session,
                                request_data=self.create_rest_call(cur_request_data),
                            )
                        )
                    )

            _, pending = await asyncio.wait(tasks, timeout=ASYNC_WAIT_TIMEOUT)
            await self._cancel_pending_tasks(tasks, pending)
            self.log_file_sizes()

    def _get_request_metadata(self, session, request_) -> dict:
        return asdict(
            RestAPIMetadata(
                base_url=str(session._base_url),  # pylint: disable=protected-access
                request=request_,
                **asdict(self.metadata),
            )
        )

    async def _execute_api_call(self, session: ClientSession, request_data: dict):
        async with self.semaphore:
            request, response = await self._send_request(
                session=session, requests=request_data
            )

            transformed = self._transform_responses(
                session=session, response=response, request_data=request
            )

            await self._dump(
                self._get_partition(transformed["block_number"]),
                transformed,
            )

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
    async def _send_request(
        self, session: ClientSession, requests: Iterable[dict]
    ) -> Iterable[dict]:
        if self.node.headers.get("Content-Encoding", {}) in ("gzip", "GZIP"):
            payload = {"data": gzip.compress(bytes(json.dumps(requests), "utf-8"))}
        else:
            payload = {"json": requests}

        async with AsyncTimedContextManger(
            log_fn=self.__log.info  # pylint: disable=no-member
        ), session.request("POST", url=self.url, **payload) as response:
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

    @abstractmethod
    def create_rest_call(self, request_data: list):
        """
        Create the payload for the rest call
        """
