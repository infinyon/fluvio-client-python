from ._fluvio_python import Cloud

DEFAULT_PROFILE_NAME = "cloud"
DEFAULT_REMOTE = "https://infinyon.cloud"


def login(
    useOauth2=True,
    profile=DEFAULT_PROFILE_NAME,
    remote=DEFAULT_REMOTE,
    email=None,
    password=None,
):
    Cloud.login(useOauth2, profile, remote, email, password)
