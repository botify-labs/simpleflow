from __future__ import annotations

import unittest
from datetime import datetime

import pytz

import swf.constants
from swf.models.event import Event
from swf.models.history import History

from ..mocks.event import mock_get_workflow_execution_history


class TestEvent(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_instantiate_with_invalid_type(self):
        with self.assertRaises(TypeError):
            Event("WrongType")

    def test_repr_with_missing_attr(self):
        with self.assertRaises(AttributeError):
            ev = Event("WorkflowExecutionStarted", swf.contants.REGISTERED, 0, None)
            delattr(ev, "id")
            ev.__repr__()

    def test_iso_date(self):
        ev = Event("WorkflowExecutionStarted", "REGISTERED", 0, {None: {}})
        self.assertEqual(datetime(1970, 1, 1, 0, 0, tzinfo=pytz.UTC), ev.timestamp)


class TestHistory(unittest.TestCase):
    def setUp(self):
        self.event_list = mock_get_workflow_execution_history()
        self.history = History.from_event_list(self.event_list["events"])

    def tearDown(self):
        pass

    def test_get_by_valid_index(self):
        val = self.history[0]
        self.assertIsNotNone(val)
        self.assertIsInstance(val, Event)

    def test_get_by_invalid_index(self):
        with self.assertRaises(IndexError):
            _ = self.history[42]  # mocked event list doesn't have 43 indexes

    def test_get_by_valid_slice(self):
        val = self.history[0:1]
        self.assertIsNotNone(val)
        self.assertIsInstance(val, History)
        self.assertEqual(len(val), 1)

    def test_get_by_invalid_slice(self):
        h = self.history[45:99]
        self.assertIsNotNone(h)
        self.assertIsInstance(h, History)
        self.assertEqual(len(h), 0)

    def test_get_by_invalid_index_type(self):
        with self.assertRaises(TypeError):
            _ = self.history["invalid, bitch"]
