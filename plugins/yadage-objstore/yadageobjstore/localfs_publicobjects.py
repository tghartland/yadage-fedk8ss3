import os
import uuid
import shutil
import logging
import copy
import six

import yadageobjstore.known_types
log = logging.getLogger(__name__)

class ObjectStore(object):
    def __init__(self, options):
        from minio import Minio
        self.client = Minio(
            options['host'],
            access_key = options['access_key'],
            secret_key = options['secret_key'],
            secure = True
        )
        self.bucket = options['bucket']

    def put(self, identifier, local_path):
        identifier = identifier.lstrip('/')
        self.client.fput_object(self.bucket,identifier,local_path)

    def get(self, identifier, local_path):
        identifier = identifier.lstrip('/')
        self.client.fget_object(self.bucket, identifier, local_path)

class FileStore(object):
    def __init__(self, options):
        self.store_path = options['store_path']

    def put(self, identifier, local_path):
        shutil.copy(local_path, os.path.join(self.store_path,identifier))

    def get(self, identifier, local_path):
        shutil.copy(os.path.join(self.store_path,identifier), local_path)

class LocalFSGlobalObjectsState(object):
    def __init__(self, global_share, local_workdir, datamodel, options = None):
        self.global_share = global_share
        self.local_workdir = local_workdir
        self.options = options or {}
        self._datamodel = datamodel

        storetype = options.get('type','filebased')
        if storetype == 'filebased':
            storespec = {'store_path': global_share}
            storespec.update(**self.options)
            self.store = FileStore(storespec)
        elif storetype == 's3':
            storespec = {'bucket': global_share}
            storespec.update(**self.options)
            self.store = ObjectStore(storespec)
        else:
            raise RuntimeError('unknown storetype {}'.format(storetype))

    @property
    def metadir(self):
        return os.path.join(self.local_workdir, '_packtivity')

    @property
    def readwrite(self):
        return [self.local_workdir]

    @property
    def readonly(self):
        return []

    def ensure(self):
        pass

    def put_file(self, local_path, global_path):
        log.info('putting file from {} to {}'.format(local_path, global_path))
        self.store.put(global_path, local_path)

    def get_file(self, local_path, global_path):
        log.info('getting file from {} to {}'.format(global_path,local_path))
        self.store.get(global_path, local_path)

    def model(self, p):
        return p

    @property
    def datamodel(self):
        return  self._datamodel

    def acquisition_spec(self, fileobj,localnames):
        if localnames == 'uuid':
            targetname = 'local-'+str(uuid.uuid4())
            local_path = os.path.join(self.local_workdir, targetname)
            fileobj.local_path = local_path
            remotepath = fileobj.path
        else:
            raise RuntimeError('not sure how to name the local files')
        return {
            'source': remotepath, 'target': fileobj.local_path
        }

    def make_local_pars(self, parameters, localnames = 'uuid'):
        localpars = copy.deepcopy(parameters)

        setup_spec = {
            'acquisitions': []
        }

        for p, v in localpars.leafs():
            log.info('processing leaf at {} value: {} (type: {})'.format(p.path, v, type(v)))
            if isinstance(v,six.string_types):
                what = v.format(workdir = self.local_workdir)
                localpars.replace(p,what)
            if isinstance(v,yadageobjstore.known_types.SimpleFile):
                if v.path: #if this is a global file we need to acquire it
                    setup_spec['acquisitions'].append(self.acquisition_spec(v,localnames))
                ### we override any pre-existing local path
                v.local_path = v.local_path.format(workdir = self.local_workdir)
                parameters.replace(p, v)
                localpars.replace(p,v.local_path)
        return setup_spec, localpars

    @classmethod
    def fromJSON(cls, data):
        return cls(**{k:v for k,v in data.items() if not k in ['state_type']})

    def json(self):
        return {
            'state_type': 'LocalFSGlobalObjectsState',
            'global_share':  self.global_share,
            'local_workdir': self.local_workdir,
            'options': self.options,
            'datamodel': self._datamodel
        }

class LocalFSGlobalObjectsProvider(object):
    def __init__(self, global_share, local_workdir, stateopts = None):
        self.global_share = global_share
        self.local_workdir = os.path.abspath(local_workdir)
        self.stateopts = stateopts or {}
        self.init_states = []

    @property
    def datamodel(self):
        return {
            'keyword': '$type',
            'types': {
                'File': 'yadageobjstore.known_types:SimpleFile',
            },
            'literals': {
               'magics': ['global://', 'local://'],
               'parser': 'yadageobjstore.known_types:parse_literal'
            }
        }

    def json(self):
        return {}

    def new_provider(self, name, init_states = None):
        return self

    def new_state(self,name, dependencies, readonly = False):
        workdir = str(uuid.uuid4())
        state =  LocalFSGlobalObjectsState(self.global_share,
            self.local_workdir,
            self.datamodel,
            self.stateopts
        )
        state.ensure()
        return state
