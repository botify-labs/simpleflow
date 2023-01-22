import json
import os
from base64 import b64encode

import kubernetes.client
import kubernetes.config

from simpleflow.utils import json_dumps


class KubernetesJob:
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
        import jinja2
        from yaml import safe_load

        # extract job template location
        input = self.response.get("input")
        if not input:
            raise ValueError("Cannot extract job template from empty input")
        meta = json.loads(input).get("meta")
        if not meta:
            raise ValueError("Cannot extract 'meta' key from task input")
        job_template = meta["k8s_job_template"]

        # setup variables that will be interpolated in the template
        variables = dict(os.environ)
        for key, value in meta.get("k8s_job_data", {}):
            variables[key] = value
        variables["JOB_NAME"] = self.job_name
        variables["PAYLOAD"] = b64encode(json_dumps(self.response).encode("utf-8"))

        # render the job template with those context variables
        path, filename = os.path.split(job_template)

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(path or "./"),
            undefined=jinja2.StrictUndefined,
            autoescape=True,  # Unsure we should do this: if someone uses k8s, let us know :)
        )
        rendered = env.get_template(filename).render(variables)

        return safe_load(rendered)

    def schedule(self):
        """
        Schedule a job from the given job template. See example of it here:
        https://github.com/kubernetes-client/python/blob/master/examples/deployment_create.py
        """
        # build job definition
        job_definition = self.compute_job_definition()

        # load cluster config
        self.load_config()

        # schedule job
        api = kubernetes.client.BatchV1Api()
        namespace = os.getenv("K8S_NAMESPACE", "default")
        api.create_namespaced_job(body=job_definition, namespace=namespace)
