from ._fluvio_python import Cloud

DEFAULT_REMOTE = "https://infinyon.cloud"


def login(
    useOauth2=True,
    remote=DEFAULT_REMOTE,
    profile=None,
    email=None,
    password=None,
):
    Cloud.login(useOauth2, remote, profile, email, password)
