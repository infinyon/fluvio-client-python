from ._fluvio_python import Cloud
import logging
import sys

DEFAULT_REMOTE = "https://infinyon.cloud"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
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
        Cloud.login(False, remote, profile, email, password)
    else:
        logger.info('fluvio-client-python - getting auth-url')
        auth0_url, user_code = Cloud.auth0_url(remote)
        logger.info(f'Please visit the following URL: {auth0_url} and verify the following code: {user_code} matches.\n Then, proceed with authentication.')
        logger.info('fluvio-client-python - got auth-url')
        Cloud.login(useOauth2, remote, profile, email, password)

    Cloud.login(useOauth2, remote, profile, email, password)
    logger.debug('after login')
