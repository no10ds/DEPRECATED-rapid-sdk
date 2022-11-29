import json
import os

import requests
from requests.auth import HTTPBasicAuth

from rapid.utils.constants import TIMEOUT_PERIOD
from rapid.exceptions import AuthenticationErrorException, CannotFindCredentialException

RAPID_CLIENT_ID = "RAPID_CLIENT_ID"
RAPID_CLIENT_SECRET = "RAPID_CLIENT_SECRET"  # pragma: allowlist secret # nosec
RAPID_URL = "RAPID_URL"


class RapidAuth:
    def __init__(
        self, client_id: str = None, client_secret: str = None, url: str = None
    ) -> None:
        self.client_id = self.evaluate_inputs(client_id, RAPID_CLIENT_ID)
        self.client_secret = self.evaluate_inputs(client_secret, RAPID_CLIENT_SECRET)
        self.url = self.evaluate_inputs(url, RAPID_URL)
        self.validate_credentials()

    def evaluate_inputs(self, value: str, environment_variable: str):
        if not value:
            value = os.environ.get(environment_variable)
            if not value:
                raise CannotFindCredentialException(
                    f"No value passed for {environment_variable}, could not authenticate to rAPId"
                )
        return value

    @property
    def headers(self):
        return {"Content-Type": "application/x-www-form-urlencoded"}

    @property
    def payload(self):
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
        }

    def credentials_secret(self):
        return HTTPBasicAuth(self.client_id, self.client_secret)

    def request_token(self):
        response = requests.post(
            self.url + "/oauth2/token",
            auth=self.credentials_secret(),
            headers=self.headers,
            json=self.payload,
            timeout=TIMEOUT_PERIOD,
        )
        return response

    def validate_credentials(self):
        if self.request_token().status_code != 200:
            raise AuthenticationErrorException(
                "Auth not configured, could not connect to instance of rAPId"
            )

    def fetch_token(self):
        return json.loads(self.request_token().content.decode("utf-8"))["access_token"]
