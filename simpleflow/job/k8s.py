from base64 import b64encode
from jinja2 import Template
import json
import os
import yaml

import kubernetes.config
import kubernetes.client

from simpleflow.utils import json_dumps


class KubernetesJob(object):
    def __init__(self, job_name, domain, response):
        self.job_name = job_name
        self.response = response
        self.domain = domain

    def load_config(self):
        """
        Load config in the current Kubernetes cluster, either via in cluster config
        or via the local kube config if on a development machine.
        """
        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()

    def compute_job_definition(self):
        """
        Compute a job definition from the SWF response
        """
        # extract job template location
        input = self.response.get("input")
        if not input:
            raise ValueError("Cannot extract job template from empty input")
        meta = json.loads(input).get("meta")
        if not meta:
            raise ValueError("Cannot extract 'meta' key from task input")
        job_template = meta["k8s_job_template"]

        # get job template
        with open(job_template) as f:
            content = f.read()

        # setup variables that will be interpolated in the template
        variables = dict(os.environ)
        for key, value in meta.get("k8s_job_data", {}):
            variables[key] = value
        variables["JOB_NAME"] = self.job_name
        variables["PAYLOAD"] = b64encode(json_dumps(self.response))

        # fill in the blanks
        template = Template(content)
        rendered = template.render(**variables)

        return yaml.load(rendered)

    def schedule(self):
        """
        Schedule a job from the given job template. See example of it here:
        https://github.com/kubernetes-incubator/client-python/blob/master/examples/create_deployment.py
        """
        # build job definition
        job_definition = self.compute_job_definition()

        # load cluster config
        self.load_config()

        # schedule job
        api = kubernetes.client.BatchV1Api()
        namespace = os.getenv("K8S_NAMESPACE", "default")
        api.create_namespaced_job(body=job_definition, namespace=namespace)
