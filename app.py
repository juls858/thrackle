from autologging import logged
from aws_lambda_powertools import Logger

from clients import RestClient
from environment import (
    DEFAULT_REQUEST_TIMEOUT,
    ENV,
    QUICKNODE_API_KEY,
    QUICKNODE_SERVICE,
    SERVICE_NAME,
)
from utils import Metadata, UtilsMixIn

ASYNC_WAIT_TIMEOUT = 800
BACKOFF_MAX_TIME = DEFAULT_REQUEST_TIMEOUT

LOCATION_FORMAT = (
    "%(pathname)s:%(lineno)d in %(name)s:%(funcName)s"  # pylint: disable=invalid-name
)
logger = Logger(location=LOCATION_FORMAT)


@logged(logger)
class ContractReader(RestClient, UtilsMixIn):
    """Hits Etherscan API to fetch Ethereum smart contract ABIs"""

    def _transform_responses(self, session, responses, requests):
        return [
            {
                "contract_address": response["id"].split("-")[0],
                "function_signature": response["id"].split("-")[1],
                "call_name": response["id"].split("-")[2],
                "function_input": None
                if (function_input := response["id"].split("-")[3]) == "None"
                else function_input,
                "block_number": response["id"].split("-")[4],
                "metadata": self._get_request_metadata(
                    session, requests[response["id"]]
                ),
                "data": response,
            }
            for response in responses
        ]

    # pylint: disable=arguments-differ
    def create_rpc_request(
        self,
        contract_address: str,
        function_signature: str,
        call_name: str,
        function_input: str,
        block_number: int,
    ) -> dict:
        """
        JSONRPC call to get function from a smart contract with the given inputs

        Text Signature:     eth_call(inputs)
        """
        if not function_input:
            input_data = function_signature
        elif function_input.startswith("0x"):
            input_data = self.pad_string(function_signature, function_input[2:])
        else:
            input_data = self.pad_string(function_signature, function_input)
        return {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": contract_address,
                    "data": input_data,
                },
                block_number if isinstance(block_number, str) else hex(block_number),
            ],
            "id": f"{contract_address}-{function_signature}-{call_name}-{function_input}-{block_number}",
        }

    def pad_string(
        self, prefix: str = "", suffix: str = "", max_length: int = 74, char: str = "0"
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


if __name__ == "__main__":

    meta = Metadata(
        request_id=None,
        app_name="Uniswap v3 Pipeline",
        batch_id=None,
    )
    pools = [
        "0x309d54007b48a76139152b211b3a6c847943a617",

    ]
    reader = ContractReader(
        meta=meta,
        env=ENV.lower(),
        work_items=pools,
    )
    reader.fetch()
