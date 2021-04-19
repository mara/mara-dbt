import json

from mara_pipelines.pipelines import Pipeline, Task

from . import config
from .commands import DbtRun, DbtTest


def load_manifest():
    """Loads and returns the dbt manifest file content"""
    with open(config.manifest_file_path()) as f:
        return json.load(f)


def model_name_to_task_id(model_name: str):
    """Generates a task id from a dbt a model name"""
    return model_name.lower().replace('.','__')


def add_nodes_from_manifest(pipeline: Pipeline, manifest, add_model_tests: bool = False):
    """
    Adds mara tasks to a pipeline for a dbt manifest file

    Args:
        pipeline: The pipeline to which the tasks will be added
        manifest: The manifest file. See load_manifest()
        add_model_tests: If dbt test commands shall be added
    """
    nodes: {str: Node} = {}
    upstreams: {str: [str]} = {}

    for node in manifest["nodes"].keys():
        if node.split(".")[0] == "model":
            node_test = node.replace("model", "test")

            model = node.split('.')[-1]
            model_id = model_name_to_task_id(model)

            nodes[node] = Task(id=model_id,
                               description=f'DBT model {node}',
                               commands=[DbtRun([model])])
            if add_model_tests:
                nodes[node_test] = Task(id=model_id+'_test',
                                        description=f'DBT test model {node}',
                                        commands=[DbtTest([model])])

    for node in manifest["nodes"].keys():
        if node.split(".")[0] == "model":

            model = node.split('.')[-1]
            model_id = model.lower().replace('.','__')

            # Set dependency to run tests on a model after model runs finishes
            if add_model_tests:
                node_test = node.replace("model", "test")
                upstreams[node_test] = [model_id]
            else:
                upstreams[node_test] = []

            # Set all model -> model dependencies
            for upstream_node in manifest["nodes"][node]["depends_on"]["nodes"]:

                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    upstream_model = upstream_node.split('.')[-1]
                    upstream_model_id = model_name_to_task_id(upstream_model)
                    upstreams[node].append(upstream_model_id)

    for node in nodes:
        if node in upstreams:
            #print(f'node: {node}, upstreams: {json.dumps(upstreams[node])}')
            pipeline.add(nodes[node], upstreams=upstreams[node])
        else:
            #print(f'node: {node}')
            pipeline.add(nodes[node])
