from simpleflow import Workflow, futures
from simpleflow.canvas import Chain


class MarkerWorkflow(Workflow):
    name = 'example'
    version = 'example'
    task_list = 'example'

    def run(self):
        m = self.submit(self.record_marker('marker 1'))
        m = self.submit(self.record_marker('marker 1', 'some details'))
        self.submit(self.record_marker('marker 2', "2nd marker's details"))
        futures.wait(m)
        markers = self.list_markers()
        assert 2 == len(markers)
        print('Markers: {}'.format(markers))
        markers = self.list_markers(all=True)
        assert 3 == len(markers)
        print('Markers, all: {}'.format(markers))


class MarkerInChainWorkflow(Workflow):
    name = 'example'
    version = 'example'
    task_list = 'example'

    def run(self):
        chain = Chain(
            self.record_marker('marker 1'),
            self.record_marker('marker 1', 'some details'),
            self.record_marker('marker 2', "2nd marker's details"),
        )
        futures.wait(self.submit(chain))
        markers = self.list_markers()
        assert 2 == len(markers)
        print('Markers: {}'.format(markers))
        markers = self.list_markers(all=True)
        assert 3 == len(markers)
        print('Markers, all: {}'.format(markers))
