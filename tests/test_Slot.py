"""
Tests for Slot class.

"""
import pytest
from conftest import test_dir
from pyslurmtq.Slot import Slot
from pyslurmtq.Task import Task


class TestTask:
    def test_slot(self, test_dir):
        """Basic tests for the slot class"""
        slot = Slot('c101', 1)
        task = Task(0, 'echo test', test_dir)
        task2 = Task(1, 'echo test', test_dir)
        assert slot.is_free()
        slot.occupy(task)
        assert not slot.is_free()
        assert slot.tasks[-1] == task
        with pytest.raises(AttributeError):
            slot.occupy(task2)
        slot.release()
        assert slot.is_free()
        slot.occupy(task2)
        assert not slot.is_free()
        assert slot.tasks[-1] == task2
        assert slot.tasks[0] == task
