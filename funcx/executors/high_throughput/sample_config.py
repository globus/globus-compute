import getpass

global_options = {
    'username' : getpass.getuser(),
    'email' : 'USER@USERDOMAIN.COM',
}

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
#-----------------------------------------------------
default_profile = HighThroughputExecutor(
    provider=LocalProvider(
        channel=LocalChannel
    )
)
#-----------------------------------------------------

# List endpoint profiles
endpoint_profiles = [
    default_profile,
]
