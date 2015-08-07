from __future__ import absolute_import

import ast

import graphviz


def find_workflow(workflow, nodes):
    for node in (i for i in nodes if isinstance(i, ast.ClassDef)):
        if node.name == workflow.__class__.__name__:
            return node


def find_method(method, nodes):
    for node in (i for i in nodes if isinstance(i, ast.FunctionDef)):
        if node.name == 'run':
            return node


class Graph(object):
    def __init__(self, name, children=None):
        if children is None:
            children = []
        self._name = name
        self.children = children

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self.name

    def append(self, node):
        self.children.append(node)


class TaskNode(Graph):
    def __init__(self, name, args, lineno):
        super(TaskNode, self).__init__(name)
        self._name = name
        self.args = args
        self.lineno = lineno

    @property
    def name(self):
        return '{} #{}'.format(self._name, self.lineno)

    @property
    def id(self):
        return '{}({}) #{}'.format(
            self._name,
            ', '.join(self.args),
            self.lineno,
        )


class FutureNode(Graph):
    def __init__(self, name):
        self._name = name


class WaitNode(TaskNode):
    def __init__(self, args, lineno):
        super(WaitNode, self).__init__('wait', args, lineno)


def handle_assign(node):
    variables = [i.id for i in node.targets]
    val = node.value
    # We only care about ``self.submit()``method calls
    if not isinstance(val, ast.Call):
        return None
    if val.func.value.id != 'self':
        return None
    if val.func.attr != 'submit':
        return None

    name = val.args[0].id
    args = [i.id for i in val.args[1:]]
    return variables[0], TaskNode(name, args, node.lineno)


def render(output, graph):
    for node in graph.children:
        render(output, node)
        output.node(graph.id)
        output.edge(graph.id, node.id)


def generate(workflow):
    nodes = ast.parse(open(workflow.run.im_func.func_code.co_filename).read())
    workflow_node = find_workflow(workflow, nodes.body)
    run = find_method('run', workflow_node.body)

    graph = graphviz.Graph(format='png')
    symbols = {}
    root = Graph(workflow.__class__.__name__ + '.run')
    inputs = [i.id for i in run.args.args][1:]  # discard "self".
    for i in inputs:
        symbols[i] = root

    for node in run.body:
        if isinstance(node, ast.Assign):
            var, task = handle_assign(node)
            symbols[var] = task
            for arg in task.args:
                dep = symbols[arg]
                dep.append(task)
                graph.node(dep.name)
                graph.edge(dep.name, task.name)
        elif isinstance(node, ast.Expr):
            if isinstance(node.value, ast.Call):
                # FIXME(ggreg): handle named import ``import futures as fut``
                if node.value.func.value.id != 'futures':
                    continue
                # FIXME(ggreg): corner case, handle method alias i.e.
                # ``w = futures.wait``
                elif node.value.func.attr != 'wait':
                    continue
                args = [arg.id for arg in node.value.args]
                wait_node = WaitNode(args, node.lineno)
                for arg in args:
                    dep = symbols[arg]
                    dep.append(wait_node)
                    graph.node(dep.name)
                    graph.edge(dep.name, wait_node.name)

    graph.render('{}.deps'.format(workflow.name))

    graph2 = graphviz.Digraph(format='png')
    render(graph2, root)
    graph2.render('{}.deps2'.format(workflow.name))

    return symbols
