from simpleflow import Workflow, futures


class MarkerWorkflow(Workflow):
    name = 'example'
    version = 'example'
    task_list = 'example'

    def run(self):
        m = self.submit(self.record_marker('marker 1'))
        m = self.submit(self.record_marker('marker 1', 'some details'))
        self.submit(self.record_marker('marker 2', "2nd marker's details"))
        futures.wait(m)
        print('Markers: {}'.format(self.list_markers()))
        print('Markers, all: {}'.format(self.list_markers(all=True)))
