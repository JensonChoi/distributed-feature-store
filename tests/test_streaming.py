from __future__ import annotations

from typing import Any

from feature_store.streaming import StreamConsumer


class FakeMessage:
    def __init__(self, topic: str, partition: int, offset: int):
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self) -> None:
        self.committed: list[FakeMessage] = []

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> None:
        assert not asynchronous
        self.committed.append(message)


def test_stream_commit_uses_latest_offset_per_partition() -> None:
    consumer = FakeConsumer()
    stream = StreamConsumer.__new__(StreamConsumer)
    stream.consumer = consumer  # type: ignore[assignment]
    messages: list[Any] = [
        FakeMessage("features", 0, 1),
        FakeMessage("features", 1, 4),
        FakeMessage("features", 0, 3),
    ]
    stream._commit_latest(messages)
    assert [(item.partition(), item.offset()) for item in consumer.committed] == [(0, 3), (1, 4)]
