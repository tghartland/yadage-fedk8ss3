import os
import uuid
import copy
import json
import time
import random
import logging

from kubernetes import client, config

log = logging.getLogger(__name__)

fed_group = "core.federation.k8s.io"
fed_version = "v1alpha1"
fed_api_ver = fed_group + "/" + fed_version
body_del_opts = client.V1DeleteOptions()


class SubmitToKubeMixin(object):
    def __init__(self, **kwargs):
        self.namespace = kwargs['namespace']
        if kwargs.get('kubeconfig') == 'incluster':
            log.info('load incluster config')
            config.load_incluster_config()
        else:
            cfg = kwargs.get('kubeconfig', os.path.join(os.environ['HOME'],'.kube/config'))
            log.info('load config %s', cfg)
            config.load_kube_config(cfg)
            import urllib3
            urllib3.disable_warnings()

        self.federation_api = client.CustomObjectsApi(client.ApiClient(client.Configuration()))


    def placement_random_cluster(self):
        clusters = self.federation_api.list_cluster_custom_object(fed_group, fed_version, "federatedclusters")
        cluster_names = [cluster["metadata"]["name"] for cluster in clusters["items"]]
        return [random.choice(cluster_names),]


    def delete_created_resources(self, resources):
        for r in resources:
            if r['kind'] == 'FederatedJob':
                resource_name = r['metadata']['name']
                try:
                    # j = client.BatchV1Api().read_namespaced_job(resource_name,self.namespace)
                    # client.BatchV1Api().delete_namespaced_job(resource_name,self.namespace,j.spec)
                    self.federation_api.delete_namespaced_custom_object(fed_group, fed_version, self.namespace, "federatedjobs", resource_name, body_del_opts)
                except client.rest.ApiException:
                    pass

                try:
                    client.CoreV1Api().delete_collection_namespaced_pod(self.namespace, label_selector = 'job-name={}'.format(resource_name))
                except client.rest.ApiException:
                    pass

            elif r["kind"] == "FederatedJobPlacement":
                resource_name = r['metadata']['name']
                try:
                    self.federation_api.delete_namespaced_custom_object(fed_group, fed_version, self.namespace, "federatedjobplacements", resource_name, body_del_opts)
                except client.rest.ApiException:
                    pass


            elif r['kind'] == 'FederatedConfigMap':
                resource_name = r['metadata']['name']
                try:
                    # client.CoreV1Api().delete_namespaced_config_map(resource_name,self.namespace,client.V1DeleteOptions())
                    self.federation_client.delete_namespaced_custom_object(fed_group, fed_version, self.namespace, "federatedconfigmaps", resource_name, body_del_opts)
                except client.rest.ApiException:
                    pass

    def submit(self, jobspec):
        proxy_data, kube_resources = self.plan_kube_resources(jobspec)
        self.create_kube_resources(kube_resources)
        return proxy_data

    def ready(self, job_proxy):
        print("\njob_proxy:\n", job_proxy, "\n")
        ready = self.determine_readiness(job_proxy)
        if ready and not 'ready' in job_proxy:
            log.info('is first time ready %s', job_proxy['job_id'])
            job_proxy['ready'] = ready
            if job_proxy['last_success']:
                log.info('is first success %s delete resources', job_proxy['job_id'])
                self.delete_created_resources(job_proxy['resources'])
        return ready

class FederatedKubernetesBackend(SubmitToKubeMixin):
    def __init__(self,
                 resultstore,
                 kubeconfig = None,
                 stateopts = None,
                 resources_opts = None,
                 resource_labels = None,
                 svcaccount = 'default',
                 namespace = 'default',
                 ):
        self.svcaccount = svcaccount

        self.namespace  = namespace
        kwargs = {
            'namespace': namespace,
            'kubeconfig': kubeconfig,
        }

        SubmitToKubeMixin.__init__(self, **kwargs)

        self.stateopts  = stateopts or {'type': 'hostpath'}
        self.specopts   = {'type': 'single_ctr_job'}
        self.resource_labels = resource_labels or {'component': 'yadage'}
        self.resources_opts = resources_opts or {
            'requests': {
                'memory': "0.1Gi",
                'cpu': "100m"
            }
        }
        self.ydgconfig =  {
          "resultstorage": resultstore
        }
        self.cvmfs_repos = ['atlas.cern.ch','sft.cern.ch','atlas-condb.cern.ch']


    def create_kube_resources(self, resources):
        for r in resources:
            if r['kind'] == 'FederatedJob':
                api_response = self.federation_api.create_namespaced_custom_object(fed_group, fed_version, self.namespace, "federatedjobs", r)
                log.info('created federated job %s', r['metadata']['name'])
                # log.info("api_response: %s", api_response)
                # log.info("")
            elif r['kind'] == "FederatedJobPlacement":
                api_response = self.federation_api.create_namespaced_custom_object(fed_group, fed_version, self.namespace, "federatedjobplacements", r)
                log.info("created federated job placement %s", r["metadata"]["name"])
                # log.info("api_response: %s", api_response)
                # log.info("")

            elif r['kind'] == 'ConfigMap':
                cm = client.V1ConfigMap(
                    api_version = 'v1',
                    kind = r['kind'],
                    metadata = {'name': r['metadata']['name'], 'namespace': self.namespace, 'labels': self.resource_labels},
                    data = r['data']
                )
                client.CoreV1Api().create_namespaced_config_map(self.namespace,cm)
                log.info('created configmap %s', r['metadata']['name'])


    def determine_readiness(self, job_proxy):
        ready = job_proxy.get('ready',False)
        if ready:
            return True

        log.info('actually checking job %s', job_proxy['job_id'])

        job_id  = job_proxy['job_id']
        jobstatus = client.BatchV1Api().read_namespaced_job(job_id,self.namespace).status
        job_proxy['last_success'] = jobstatus.succeeded
        job_proxy['last_failed']  = jobstatus.failed
        ready =  job_proxy['last_success'] or job_proxy['last_failed']
        if ready:
            log.info('job %s is ready and successful. success: %s failed: %s', job_id,
                job_proxy['last_success'], job_proxy['last_failed']
            )
        return ready

    def successful(self,job_proxy):
        return job_proxy['last_success']

    def fail_info(self,resultproxy):
        pass

    def auth_binds(self,authmount):
        container_mounts = []
        volumes = []

        log.debug('binding auth')
        volumes.append({
            'name': 'hepauth',
            'secret': yaml.load(open('secret.yml'))
        })
        container_mounts.append({
            "name": 'hepauth',
            "mountPath": authmount
        })
        return container_mounts, volumes

    def cvmfs_binds(self, repos):
        container_mounts = []
        volumes = []
        log.debug('binding CVMFS')
        for repo in repos:
            reponame = repo.replace('.','').replace('-','')
            volumes.append({
                'name': reponame,
                'flexVolume': {
                'driver': "cern/cvmfs",
                    'options': {
                        'repository': repo
                    }
                }
            })
            container_mounts.append({
                "name": reponame,
                "mountPath": '/cvmfs/'+repo
            })
        return container_mounts, volumes

    def make_par_mount(self, job_uuid, parmounts):
        parmount_configmap_contmount = []
        configmapspec = {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {'name': 'parmount-{}'.format(job_uuid)},
            'data': {}
        }

        vols_by_dir_name = {}
        for i,x in enumerate(parmounts):
            configkey = 'parmount_{}'.format(i)
            configmapspec['data'][configkey] = x['mountcontent']

            dirname  = os.path.dirname(x['mountpath'])
            basename = os.path.basename(x['mountpath'])

            vols_by_dir_name.setdefault(dirname,{
                'name': 'vol-{}'.format(dirname.replace('/','-')),
                'configMap': {
                    'name': configmapspec['metadata']['name'],
                    'items': []
                }
            })['configMap']['items'].append({
                'key': configkey, 'path': basename
            })

        log.debug(vols_by_dir_name)

        for dirname,volspec in vols_by_dir_name.items():
            parmount_configmap_contmount.append({
                'name': volspec['name'],
                'mountPath':  dirname
            })
        return parmount_configmap_contmount, vols_by_dir_name.values(), configmapspec

    def plan_kube_resources(self, jobspec):
        job_uuid = str(uuid.uuid4())

        kube_resources = []

        env           = jobspec['spec']['environment']
        cvmfs         = 'CVMFS' in env['resources']
        parmounts     = env['par_mounts']
        auth          = 'GRIProxy' in env['resources']
        sequence_spec = jobspec['sequence_spec']

        container_mounts, volumes = [], []

        container_mounts_state, volumes_state = [
            { "name": "comms-volume",   "mountPath": sequence_spec['config_mounts']['comms'] },
            { "name": "workdir-volume", "mountPath": jobspec['local_workdir'] }
        ], [
            { "name": "workdir-volume", "emptyDir": {} },
            { "name": "comms-volume",   "emptyDir": {} }
        ]

        container_mounts += container_mounts_state
        volumes          += volumes_state

        if cvmfs:
            container_mounts_cvmfs, volumes_cvmfs = self.cvmfs_binds(self.cvmfs_repos)
            container_mounts += container_mounts_cvmfs
            volumes          += volumes_cvmfs

        if auth:
            container_mounts_auth, volumes_auth = self.auth_binds('/recast_auth')
            container_mounts += container_mounts_auth
            volumes          += volumes_auth

        if parmounts:
            container_mounts_pm, volumes_pm, pm_cm_spec = self.make_par_mount(job_uuid, parmounts)
            container_mounts += container_mounts_pm
            volumes += volumes_pm
            kube_resources.append(pm_cm_spec)


        jobconfigname = "wflow-job-config-{}".format(job_uuid)
        jobname = "wflow-job-{}".format(job_uuid)
        resultfilename = 'result-{}.json'.format(job_uuid)

        jobconfig = copy.deepcopy(jobspec)
        jobconfig['resultfile'] = resultfilename

        kube_resources.append({
          "apiVersion": "v1",
          "kind": "ConfigMap",
          "data": {
            "ydgconfig.json": json.dumps(self.ydgconfig),
            "jobconfig.json": json.dumps(jobconfig)
          },
          "metadata": {
            "name": jobconfigname
          }
        })

        configmounts = [{
            "name": "job-config",
            "mountPath": sequence_spec['config_mounts']['jobconfig']
        }]

        container_sequence = [{
          "name": seqname,
          "image": sequence_spec[seqname]['image'],
          "command": sequence_spec[seqname]['cmd'],
          "env": sequence_spec['config_env'] if sequence_spec[seqname]['iscfg'] else [],
          "volumeMounts":  container_mounts + (configmounts if sequence_spec[seqname]['iscfg'] else [])
        } for seqname in sequence_spec['sequence']]

        """kube_resources.append({
          "apiVersion": "batch/v1",
          "kind": "Job",
          "spec": {
            "backoffLimit": 0,
            "template": {
              "spec": {
                "restartPolicy": "Never",
                "securityContext" : {
                    "runAsUser": 0
                },
                "initContainers": container_sequence[:-1],
                "containers": container_sequence[-1:],
                "volumes": [{
                    "name": "job-config",
                    "configMap": { "name": jobconfigname },
                }] + volumes
              },
              "metadata": { "name": jobname }
            }
          },
          "metadata": { "name": jobname }
        })"""
        kube_resources.append({
            "apiVersion": fed_api_ver,
            "kind": "FederatedJob",
            "metadata": {
                "name": jobname
                },
            "spec": {
                "template": {
                    "spec": {
                        "backoffLimit": 0,
                        "template": {
                            "spec": {
                                "restartPolicy": "Never",
                                "securityContext" : {
                                    "runAsUser": 0
                                    },
                                "initContainers": container_sequence[:-1],
                                "containers": container_sequence[-1:],
                                "volumes": [{
                                    "name": "job-config",
                                    "configMap": { "name": jobconfigname },
                                    }] + volumes
                                },
                            "metadata": { "name": jobname }
                            }
                        },
                    }
                }
            })

        kube_resources.append({
            "apiVersion": fed_api_ver,
            "kind": "FederatedJobPlacement",
            "metadata": {
                "name": jobname
                },
            "spec": {
                # "clusterNames": self.placement_random_cluster()
                "clusterNames": ["cluster-01", "cluster-02", "cluster-03"]
                }
            })


        return {
            'resultjson': resultfilename,
            'job_id': jobname,
            'resources': kube_resources
        }, kube_resources
