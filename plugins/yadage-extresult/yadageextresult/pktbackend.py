import logging

from packtivity.asyncbackends import ExternalAsyncMixin, RemoteResultMixin
from packtivity.backendutils import backend

from .jobspec import make_external_job
from .kubejobbackend import KubernetesBackend
from .fedjobbackend import FederatedKubernetesBackend
from .S3ResultStore import S3ResultStore

log = logging.getLogger(__name__)

class RemoteResultExternalBackend(ExternalAsyncMixin,RemoteResultMixin):
    def __init__(self, **kwargs):
        kwargs['job_backend']   = KubernetesBackend(kwargs['resultstore'])
        kwargs['resultbackend'] = S3ResultStore(kwargs['resultstore'])
        ExternalAsyncMixin.__init__(self,**kwargs)
        RemoteResultMixin.__init__(self,**kwargs)

    def make_external_job(self,spec,parameters,state,metadata):
        return make_external_job(spec,parameters,state)

class FedRemoteResultExternalBackend(ExternalAsyncMixin,RemoteResultMixin):
    def __init__(self, **kwargs):
        kwargs['job_backend']   = FederatedKubernetesBackend(kwargs['resultstore'])
        kwargs['resultbackend'] = S3ResultStore(kwargs['resultstore'])
        ExternalAsyncMixin.__init__(self,**kwargs)
        RemoteResultMixin.__init__(self,**kwargs)

    def make_external_job(self,spec,parameters,state,metadata):
        return make_external_job(spec,parameters,state)

@backend('kubernetes')
def k8s_backend(backendstring, backendopts):
    backend = RemoteResultExternalBackend(**backendopts)
    return False, backend

@backend('fed-kubernetes')
def fed_k8s_backend(backendstring, backendopts):
    backend = FedRemoteResultExternalBackend(**backendopts)
    return False, backend
