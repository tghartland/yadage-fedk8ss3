from yadage.state_providers import providersetup
from .localfs_publicobjects import LocalFSGlobalObjectsProvider
@providersetup('private_fs_public_obj')
def setup(dataarg,dataopts):
    return LocalFSGlobalObjectsProvider(
        global_share   = dataarg.split(':')[1],
        local_workdir  = dataopts.get('local','local_workdir'),
        stateopts      = dataopts
    )

