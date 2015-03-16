import unittest
import mock

from cdf.workflows.analysis import FeatureTaskRegistry, TaskRegistry


ok = mock.MagicMock()
ok.exception = None
fail = mock.MagicMock()
fail.exception = Exception()


class TestFeatureTaskRegistry(unittest.TestCase):
    def test_register(self):
        r = FeatureTaskRegistry()
        r.register(ok, 'document')
        r.register(ok, 'task1')
        r.register(fail, 'document')
        r.register(ok, 'task2')
        r.register(ok, 'task1')

        self.assertEqual(r.registry['document'], [ok, fail])
        self.assertEqual(r.registry['task1'], [ok, ok])
        self.assertEqual(r.registry['task2'], [ok])

    def test_get_status_document(self):
        r = FeatureTaskRegistry()
        r.register(ok, 'document')
        r.register(fail, 'document')

        result = r.get_task_status()
        expected = {'document': False}
        self.assertEqual(result, expected)

    def test_get_status_task(self):
        r = FeatureTaskRegistry()
        r.register(ok, 'task1')
        r.register(ok, 'task1')

        result = r.get_task_status()
        expected = {'task1': True}
        self.assertEqual(result, expected)


class TestTaskRegistry(unittest.TestCase):
    def test_register(self):
        r = TaskRegistry()
        r.register(ok, 'document', 'feature1')
        r.register(ok, 'toto', 'feature2')
        r.register(fail, 'document', 'feature1')

        self.assertEqual(
            r.registry['feature1'].registry['document'], [ok, fail])
        self.assertEqual(
            r.registry['feature2'].registry['toto'], [ok])

    def test_get_status(self):
        r = TaskRegistry()
        r.register(ok, 'document', 'feature1')
        r.register(ok, 'task1', 'feature2')
        r.register(fail, 'document', 'feature1')

        result = r.get_task_status()
        expected = [
            {'task': 'document', 'feature': 'feature1', 'success': False},
            {'task': 'task1', 'feature': 'feature2', 'success': True},
        ]
        self.assertItemsEqual(result, expected)