import typing

from ._fluvio_python import (
    CommonCreateRequest as _CommonCreateRequest,
    PartitionMap as _PartitionMap,
    MessageMetadataSmartModuleSpec as _MessageMetadataSmartModuleSpec,
    MessageMetadataTopicSpec as _MessageMetadataTopicSpec,
    MetadataPartitionSpec as _MetadataPartitionSpec,
    MetadataSmartModuleSpec as _MetadataSmartModuleSpec,
    MetadataTopicSpec as _MetadataTopicSpec,
    MetaUpdateTopicSpec as _MetaUpdateTopicSpec,
    MetaUpdateSmartModuleSpec as _MetaUpdateSmartModuleSpec,
    SmartModuleSpec as _SmartModuleSpec,
    TopicSpec as _TopicSpec,
)


class PartitionMap:
    _inner: _PartitionMap

    def __init__(self, inner: _PartitionMap):
        self._inner = inner

    @classmethod
    def new(cls, partition: int, replicas: typing.List[int]):
        return cls(_PartitionMap.new(partition, replicas))


class TopicSpec:
    _inner: _TopicSpec

    def __init__(self, inner: _TopicSpec):
        self._inner = inner

    @classmethod
    def new(cls):
        return cls(_TopicSpec.new_computed(1, 1, True))

    @classmethod
    def new_assigned(cls, partition_maps: typing.List[PartitionMap]):
        partition_maps = [x._inner for x in partition_maps]
        return cls(_TopicSpec.new_assigned(partition_maps))

    @classmethod
    def new_computed(cls, partitions: int, replications: int, ignore: bool):
        return cls(_TopicSpec.new_computed(partitions, replications, ignore))

    @classmethod
    def new_mirror(cls):
        return cls(_TopicSpec.new_mirror())

    def set_storage(
        self, partition_size: typing.Optional[int], segment: typing.Optional[int]
    ):
        self._inner.set_storage(partition_size, segment)

    def set_system(self, system: bool):
        self._inner.set_system(system)

    def set_retention_time(self, retention: int):
        self._inner.set_retation_time(retention)

    def set_compression_type(self, compression: str):
        self._inner.set_compression_type(compression)


class CommonCreateRequest:
    _inner: _CommonCreateRequest

    def __init__(self, inner: _CommonCreateRequest):
        self._inner = inner

    @classmethod
    def new(cls, name: str, dry_run: bool, timeout: int):
        return cls(_CommonCreateRequest.new(name, dry_run, timeout))


class MetadataTopicSpec:
    _inner: _MetadataTopicSpec

    def __init__(self, inner: _MetadataTopicSpec):
        self._inner = inner

    def name(self) -> str:
        return self._inner.name()


class MessageMetadataTopicSpec:
    _inner: _MessageMetadataTopicSpec

    def __init__(self, inner: _MessageMetadataTopicSpec):
        self._inner = inner

    def is_update(self) -> bool:
        return self._inner.is_update()

    def is_delete(self) -> bool:
        return self._inner.is_delete()

    def metadata_topic_spec(self) -> MetadataTopicSpec:
        return MetadataTopicSpec(self._inner.metadata_topic_spec())


class MetaUpdateTopicSpec:
    _inner: _MetaUpdateTopicSpec

    def __init__(self, inner: _MetaUpdateTopicSpec):
        self._inner = inner

    def all(self) -> typing.List[MetadataTopicSpec]:
        inners = self._inner.all()
        return [MetadataTopicSpec(i) for i in inners]

    def changes(self) -> typing.List[MessageMetadataTopicSpec]:
        inners = self._inner.changes()
        return [MessageMetadataTopicSpec(i) for i in inners]

    def epoch(self) -> int:
        return self._inner.epoch()


class SmartModuleSpec:
    _inner: _SmartModuleSpec

    def __init__(self, inner: _SmartModuleSpec):
        self._inner = inner

    @classmethod
    def new(cls, path: str):
        f = open(path, mode="rb")
        data = f.read()
        f.close()
        return cls(_SmartModuleSpec.with_binary(data))


class MetadataSmartModuleSpec:
    _inner: _MetadataSmartModuleSpec

    def __init__(self, inner: _MetadataSmartModuleSpec):
        self._inner = inner

    def name(self) -> str:
        return self._inner.name()


class MessageMetadataSmartModuleSpec:
    _inner: _MessageMetadataSmartModuleSpec

    def __init__(self, inner: _MessageMetadataSmartModuleSpec):
        self._inner = inner

    def is_update(self) -> bool:
        return self._inner.is_update()

    def is_delete(self) -> bool:
        return self._inner.is_delete()

    def metadata_smart_module_spec(self) -> MetadataSmartModuleSpec:
        return MetadataSmartModuleSpec(self._inner.metadata_smart_module_spec())


class MetaUpdateSmartModuleSpec:
    _inner: _MetaUpdateSmartModuleSpec

    def __init__(self, inner: _MetaUpdateSmartModuleSpec):
        self._inner = inner

    def all(self) -> typing.List[MetadataSmartModuleSpec]:
        inners = self._inner.all()
        return [MetadataSmartModuleSpec(i) for i in inners]

    def changes(self) -> typing.List[MessageMetadataSmartModuleSpec]:
        inners = self._inner.changes()
        return [MessageMetadataSmartModuleSpec(i) for i in inners]

    def epoch(self) -> int:
        return self._inner.epoch()


class MetadataPartitionSpec:
    _inner: _MetadataPartitionSpec

    def __init__(self, inner: _MetadataPartitionSpec):
        self._inner = inner

    def name(self) -> str:
        return self._inner.name()
