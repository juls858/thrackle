# pylint: disable=missing-module-docstring
import asyncio
import base64
import copy
import datetime
import gzip
import inspect
import logging
import pickle
import sys
import time
from dataclasses import dataclass
from functools import wraps
from io import BytesIO
from typing import Any, Dict, Generator, Iterable, Mapping, Sequence

from aiohttp import BasicAuth, ClientError, ClientResponseError
from aws_lambda_powertools import Logger
from funcy import cat, chunks

from environment import SERVICE_NAME

logger = Logger(service=SERVICE_NAME, child=True)

ISO_DATE = "%Y-%m-%dT%H:%M:%SZ"


class UtilsMixIn:
    """Mixin class for common utility functions."""

    @staticmethod
    def rename_dict_keys(prefix_suffix: Iterable, obj: Dict) -> Dict:
        """Rename keys in a dictionary."""
        new_obj = copy.deepcopy(obj)
        return {
            f"{prefix_suffix[0]}{k}{prefix_suffix[-1]}": v for k, v in new_obj.items()
        }

    @staticmethod
    def serializes(obj: object) -> str:
        """Serialize an object (Pickle -> GZIP -> base85)"""
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="w") as gzip_file:
            gzip_file.write(pickle.dumps(obj))
        encoded = base64.b85encode(buffer.getvalue()).decode("utf-8")
        return encoded

    @staticmethod
    def deserialize(obj: str) -> Any:
        """Deserialize an object (base85 -> un-gzip -> unp-pickle)"""
        return pickle.loads(gzip.decompress(base64.b85decode(obj.encode("utf-8"))))

    @staticmethod
    def chunks(size: int, sequence: Sequence) -> Generator[list, None, None]:
        """Lazily partition nested sequences"""
        return chunks(size, cat(sequence))


class BackoffHandlers(UtilsMixIn):  # pylint: disable=missing-class-docstring
    @classmethod
    async def give_up(cls, error: ClientError) -> bool:
        """
        Give up (don't retry) on truthy (true condition)
        """
        if isinstance(error, ClientResponseError):
            return (
                error.status in (400, 403, 404)  # BAD REQUEST #UNAUTHORIZED # NOT FOUND
                or error.status >= 500
            )

    @classmethod
    async def log_backoff_event(cls, details) -> None:
        """Log backoff event."""
        error = sys.exc_info()[1]
        if isinstance(error, ClientResponseError):
            # pylint: disable=consider-using-f-string
            extra = {
                "headers": dict(error.headers),
                "status": error.status,
                "message": error.message,
            }
            logger.debug(
                (
                    "Backoff details - Backing off for {wait:0.1f}s after {tries} tries "
                    "calling function {target}"
                ).format(**details),
                extra=cls.rename_dict_keys("_", extra),
            )

    @classmethod
    async def log_giveup_event(cls, details) -> None:
        """Log giveup event."""
        error = sys.exc_info()[1]
        if isinstance(error, ClientResponseError):
            # pylint: disable=consider-using-f-string
            extra = {
                "headers": dict(error.headers),
                "status": error.status,
                "message": error.message,
            }
            logger.error(
                (
                    "Giveup details - Giving up after {elapsed:0.1f}s and {tries} tries "
                    "calling function {target}"
                ).format(**details),
                extra=cls.rename_dict_keys("_", extra),
            )

    @classmethod
    async def log_giveup_event_info(cls, details) -> None:
        """Log giveup event."""
        error = sys.exc_info()[1]
        if isinstance(error, ClientResponseError):
            # pylint: disable=consider-using-f-string
            extra = {
                "headers": dict(error.headers),
                "status": error.status,
                "message": error.message,
            }
            logger.info(
                (
                    "Giveup details - Giving up after {elapsed:0.1f}s and {tries} tries "
                    "calling function {target}"
                ).format(**details),
                extra=cls.rename_dict_keys("_", extra),
            )

    @classmethod
    async def give_up_on_404(cls, error: ClientError) -> bool:
        """
        Give up (don't retry) on truthy (true condition)
        """
        if isinstance(error, ClientResponseError):
            return (
                error.status
                in (404,)  # BAD REQUEST  # NOT FOUND  # NOT IMPLEMENTED
                # or error.status >= 500
            )


@dataclass
class Node:  # pylint: disable=missing-class-docstring
    host: str
    auth: BasicAuth
    headers: dict


class Route(Node):  # pylint: disable=missing-class-docstring
    url: str
    stage: str = ""

    def __init__(
        self, host: str, auth: BasicAuth, headers: dict, url: str, stage: str = ""
    ):
        super().__init__(host=host, auth=auth, headers=headers)
        self._url = url
        self.stage = stage

    @property
    def url(self) -> str:
        """Return URL."""
        return self._url.format(host=self.host, stage=self.stage)


class Timed(object):
    """
    Decorator class for logging function start, completion, and elapsed time.
    This works with normal functions as well as asyncio functions.

    Examples:
    ```python
        @Timed()
        def my_func_a():
            pass
        # 2019-08-18 - INFO - Beginning call to my_func_a()...
        # 2019-08-18 - INFO - Completed call to my_func_a()  (00:00:00 elapsed)

        @Timed(log_fn=logging.debug)
        def my_func_b():
            pass
        # 2019-08-18 - DEBUG - Beginning call to my_func_b()...
        # 2019-08-18 - DEBUG - Completed call to my_func_b()  (00:00:00 elapsed)

        @Timed("doing a thing")
        def my_func_c():
            pass
        # 2019-08-18 - INFO - Beginning doing a thing...
        # 2019-08-18 - INFO - Completed doing a thing  (00:00:00 elapsed)

        @Timed("doing a thing with {foo_obj.name}")
        def my_func_d(foo_obj):
            pass
        # 2019-08-18 - INFO - Beginning doing a thing with Foo...
        # 2019-08-18 - INFO - Completed doing a thing with Foo  (00:00:00 elapsed)

        @Timed("doing a thing with '{custom_kwarg}'", custom_kwarg="foo")
        def my_func_e(foo_obj):
            pass
        # 2019-08-18 - INFO - Beginning doing a thing with 'foo'...
        # 2019-08-18 - INFO - Completed doing a thing with 'foo'  (00:00:00 elapsed)
        ```
    """

    def __init__(
        # pylint: disable=unused-argument
        self,
        desc_text="'{desc_detail}' call to {fn.__module__}.{fn.__qualname__}()",
        desc_detail="",
        start_msg="Beginning {desc_text}...",
        success_msg="Completed {desc_text}  {elapsed}",
        log_fn=logging.info,
        **extra_kwargs,
    ):
        """All arguments optional"""
        self.context = extra_kwargs.copy()  # start with extra args
        self.context.update(locals())  # merge all constructor args
        self.context["elapsed"] = None

    def re_eval(self, context_key: str):
        """Evaluate the f-string in self.context[context_key], store back the result"""
        self.context[context_key] = f_str(
            self.context[context_key], locals_=self.context
        )

    def elapsed_str(self, elapsed: datetime.timedelta) -> str:
        """Return a formatted string, e.g. '(HH:MM:SS elapsed)'"""
        return f"({elapsed} elapsed)"

    def elapsed(self) -> datetime.timedelta:
        """Calculate elapsed time"""
        seconds = time.time() - self.context["start"]
        return datetime.timedelta(seconds=seconds)

    def __call__(self, func):
        """Call the decorated function"""

        @wraps(func)
        def wrapped_fn(*args, **kwargs):
            """
            The decorated function definition. Note that the log needs access to
            all passed arguments to the decorator, as well as all of the function's
            native args in a dictionary, even if args are not provided by keyword.
            If start_msg is None or success_msg is None, those log entries are skipped.
            """
            self.context["start"] = time.time()

            self.context["fn"] = func
            fn_arg_names = inspect.getfullargspec(func).args
            for arg_name, arg_value in enumerate(args, 0):
                self.context[fn_arg_names[arg_name]] = arg_value
            self.context.update(kwargs)
            desc_detail_fn = None
            log_fn = self.context["log_fn"]
            # If desc_detail is callable, evaluate dynamically (both before and after)
            if callable(self.context["desc_detail"]):
                desc_detail_fn = self.context["desc_detail"]
                self.context["desc_detail"] = desc_detail_fn()

            # Re-evaluate any decorator args which are f-strings
            self.re_eval("desc_detail")
            self.re_eval("desc_text")
            # Remove 'desc_detail' if blank or unused
            self.context["desc_text"] = self.context["desc_text"].replace("'' ", "")
            self.re_eval("start_msg")
            if self.context["start_msg"]:
                # log the start of execution
                log_fn(self.context["start_msg"])
            if asyncio.iscoroutinefunction(func):
                print("this is a coroutine")
                # If the function is a coroutine, await it
                ret_val = self._async_helper(func, args, kwargs)
            else:
                ret_val = func(*args, **kwargs)
            if desc_detail_fn:
                # If desc_detail callable, then reevaluate
                self.context["desc_detail"] = desc_detail_fn()
            elapsed = self.elapsed()
            self.context["elapsed"] = self.elapsed_str(elapsed)
            # log the end of execution
            log_fn(
                f_str(self.context["success_msg"], locals_=self.context),
                extra={"seconds": elapsed.total_seconds()},
            )
            return ret_val

        return wrapped_fn

    @staticmethod
    async def _async_helper(func, args, kwargs):
        return await func(*args, **kwargs)


class AsyncTimedContextManger:
    """Async context manager for timing a block of code"""

    def __init__(self, log_fn=logging.info):
        self.log_fn = log_fn
        self.start: float = None

    async def __aenter__(self):
        self.start = time.time()

    async def __aexit__(self, *args):
        end = time.time()
        elapsed = datetime.timedelta(seconds=end - self.start)
        self.log_fn(
            f"Completed in {elapsed}",
            extra={
                "duration_seconds": elapsed.total_seconds(),
                "start": time.strftime(ISO_DATE, time.gmtime(self.start)),
                "end": time.strftime(ISO_DATE, time.gmtime(end)),
            },
        )


def f_str(f_string_text, locals_, globals_=None):
    """
    Dynamically evaluate the provided f_string_text

    Sample usage:
        format_str = "{i}*{i}={i*i}"
        i = 2
        f_str(format_str, locals()) # "2*2=4"
        i = 4
        f_str(format_str, locals()) # "4*4=16"
        f_str(format_str, {"i": 12}) # "10*10=100"
    """
    locals_ = locals_ or {}
    globals_ = globals_ or {}
    ret_val = eval(  # pylint: disable=eval-used
        f'f"""{f_string_text}"""', locals_, globals_
    )
    return ret_val


@dataclass(frozen=True)
class Metadata:
    """
    Dataclass for operational metadata
    """

    app_name: str
    batch_id: str
    request_id: str


@dataclass(frozen=True)
class JsonRpcMetadata(Metadata):
    """Metadata for a JSON-RPC request"""

    host: str
    request: Mapping


@dataclass(frozen=True)
class PackageMetadata(Metadata):
    """Metadata for local python package"""

    package: str
    version: str


@dataclass(frozen=True)
class RestAPIMetadata(Metadata):
    """Metadata for a Rest-API request"""

    base_url: str
    request: Mapping
