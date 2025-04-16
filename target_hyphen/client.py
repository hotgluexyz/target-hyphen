from target_hotglue.client import HotglueSink
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

import requests
from requests_auth_aws_sigv4 import AWSSigV4
import backoff
import json
from target_hotglue.common import HGJSONEncoder

class HyphenSink(HotglueSink):

    @property
    def base_url(self) -> str:
        return self.config.get("url", "https://api-wallet-uat.hyphensolutions.com")

    @property
    def authenticator(self):
        return AWSSigV4(
            service="execute-api",
            aws_access_key_id=self.config.get("access_key"),
            aws_secret_access_key=self.config.get("secret_key"),
            region=self.config.get("region", "us-west-2"),
        )

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}, verify=True
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)
        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data
            else None
        )

        response = requests.request(
            method=http_method,
            auth=self.authenticator,
            url=url,
            params=params,
            headers=headers,
            data=data,
            verify=verify
        )
        self.validate_response(response)
        return response

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 500 and response.json().get("error", "") == "Duplicate batch number":
            self.logger.info("Batch already exists!")
            return
        elif response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(f"{msg} with response: {response.json()}")
        elif 400 <= response.status_code < 500:
            try:
                msg = response.json()
            except:
                msg = self.response_error_message()
            raise FatalAPIError(msg)
