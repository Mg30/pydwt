import pytest
from pydwt.context.connection import Connection


def test_connection():
    args = {"url": "sqlite:///:memory:", "echo": True}
    conn = Connection(args)
    assert conn

def test_connection_get_engine():
    args = {"url": "sqlite:///:memory:", "echo": True}
    conn = Connection(args)
    engine = conn.get_engine()
    assert engine.connect()
