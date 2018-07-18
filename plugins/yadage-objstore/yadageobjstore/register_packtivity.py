from packtivity.statecontexts import stateloader
from .localfs_publicobjects import LocalFSGlobalObjectsState

@stateloader('LocalFSGlobalObjectsState')
def localfspublicobj_stateloader(jsondata, **opts):
    return LocalFSGlobalObjectsState.fromJSON(jsondata)
