from ._fluvio_python import (
    Record as _Record,
    RecordMetadata as _RecordMetadata,
)
import typing


class Record:
    """The individual record for a given stream."""

    _inner: _Record

    def __init__(self, inner: _Record):
        self._inner = inner

    def offset(self) -> int:
        """The offset from the initial offset for a given stream."""
        return self._inner.offset()

    def value(self) -> typing.List[int]:
        """Returns the contents of this Record's value"""
        return self._inner.value()

    def value_string(self) -> str:
        """The UTF-8 decoded value for this record."""
        return self._inner.value_string()

    def key(self) -> typing.List[int]:
        """Returns the contents of this Record's key, if it exists"""
        return self._inner.key()

    def key_string(self) -> str:
        """The UTF-8 decoded key for this record."""
        return self._inner.key_string()

    def timestamp(self) -> int:
        """Timestamp of this record."""
        return self._inner.timestamp()


class RecordMetadata:
    """Metadata of a record send to a topic."""

    _inner: _RecordMetadata

    def __init__(self, inner: _RecordMetadata):
        self._inner = inner

    def offset(self) -> int:
        """Return the offset of the sent record in the topic/partition."""
        return self._inner.offset()

    def partition_id(self) -> int:
        """Return the partition index the record was sent to."""
        return self._inner.partition_id()
