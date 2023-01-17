from ._fluvio_python import Cloud
import logging

DEFAULT_REMOTE = "https://infinyon.cloud"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.CRITICAL)
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
        cloudClient = Cloud.new(remote)
        auth0_url, user_code = cloudClient.get_auth0_url()
        logger.critical(
            f"Please visit the following URL: {auth0_url} and verify the following code: {user_code} matches.\n Then, proceed with authentication."  # noqa: E501
        )
        consoleHandler.flush()
        cloudClient.authenticate_with_auth0()
