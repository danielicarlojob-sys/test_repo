import os
import io
import tempfile
import builtins
import inspect
import pytest

from src.utils import log_file


class TestLogMessage:
    def test_log_message_console_only(self, capsys, monkeypatch):
        """Message should be printed to console but not written to file if to_file=False."""
        monkeypatch.setattr(
            log_file, "LOG_FILE", os.path.join(
                tempfile.gettempdir(), "fake_log.txt"))

        log_file.log_message("Hello Test", to_file=False)

        captured = capsys.readouterr()
        assert "Hello Test" in captured.out
        # Ensure file not created
        assert not os.path.exists(log_file.LOG_FILE)

    def test_log_message_console_and_file(self, capsys, monkeypatch):
        """Message should be printed and appended to the log file when to_file=True."""
        tmpfile = os.path.join(tempfile.gettempdir(), "log_message_test.txt")
        monkeypatch.setattr(log_file, "LOG_FILE", tmpfile)

        # Remove old test file if exists
        if os.path.exists(tmpfile):
            os.remove(tmpfile)

        log_file.log_message("Write to file", to_file=True)

        # Console check
        captured = capsys.readouterr()
        assert "Write to file" in captured.out

        # File check
        with open(tmpfile, "r", encoding="utf-8") as f:
            contents = f.read()
        assert "Write to file" in contents

    def test_log_message_multiple_writes(self, capsys, monkeypatch):
        """Multiple messages should be appended to file (not overwritten)."""
        tmpfile = os.path.join(tempfile.gettempdir(), "log_message_append.txt")
        monkeypatch.setattr(log_file, "LOG_FILE", tmpfile)

        if os.path.exists(tmpfile):
            os.remove(tmpfile)

        log_file.log_message("First line", to_file=True)
        log_file.log_message("Second line", to_file=True)

        with open(tmpfile, "r", encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 2
        assert "First line" in lines[0]
        assert "Second line" in lines[1]


class TestDebugInfo:
    def test_debug_info_returns_expected_format(self):
        """debug_info should include filename, function, and line number."""
        def dummy_function():
            return log_file.debug_info()

        debug_str = dummy_function()
        assert "[DEBUG]" in debug_str
        assert "function: dummy_function" in debug_str
        assert "file:" in debug_str
        assert "line:" in debug_str

    def test_debug_info_caller_changes(self):
        """Ensure debug_info adapts to different caller functions."""
        def first():
            return log_file.debug_info()

        def second():
            return log_file.debug_info()

        first_info = first()
        second_info = second()

        # Function names should be different in the debug string
        assert "function: first" in first_info
        assert "function: second" in second_info
