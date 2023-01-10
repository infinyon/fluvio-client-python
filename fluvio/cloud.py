from ._fluvio_python import Cloud
import logging
import sys

DEFAULT_REMOTE = "https://infinyon.cloud"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)

logger.addHandler(consoleHandler)


def login(
    useOauth2=True,
    remote=DEFAULT_REMOTE,
    profile=None,
    email=None,
    password=None,
):
    if email is not None and password is not None:
        Cloud.login_with_username(remote, profile, email, password)
    else:
        logger.debug("fluvio-client-python - getting auth-url")
        cloudClient = Cloud.new(remote)
        auth0_url, user_code = cloudClient.get_auth0_url()
        logger.info(
            f"Please visit the following URL: {auth0_url} and verify the following code: {user_code} matches.\n Then, proceed with authentication."
        )

        cloudClient.authenticate_with_auth0()
        logger.debug("fluvio-client-python - success")
