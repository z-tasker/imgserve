from __future__ import annotations
import json
import requests

from .errors import APIError, MissingCredentialsError, UnexpectedStatusCodeError
from .logger import simple_logger

class ImgServe:
    def __init__(self, remote_url: str, username: str = "", password: str = "") -> None:
        local = remote_url.startswith("http://localhost")
        if not local:
            if username == "" or password == "":
                raise MissingCredentialsError("must set remote username and password when using {remote_url}")
            
        self.remote_url = remote_url
        self.auth = requests.auth.HTTPBasicAuth(username, password) if not local else None
        self.log = simple_logger("ImgServe" + " local" if local else " remote")

    def get_experiment(self, name: str) -> Dict[str, Any]:
        try:
            response = requests.get(f"{self.remote_url}/experiments/{name}", auth=self.auth)
            if response.status_code != 200:
                raise UnexpectedStatusCodeError(f"{response.status_code} from {self.remote_url}")
            resp = json.loads(response.text)
        except requests.exceptions.ConnectionError as e:
            raise APIError(f"connection to {self.remote_url} failed, is it running?") from e

        if "error" in resp:
            raise APIError(json.dumps(resp))

        return resp

