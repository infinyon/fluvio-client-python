from dataclasses import dataclass, replace
from typing import Optional, List, Union
from enum import Enum
from .specs import PartitionMap
from ._fluvio_python import TopicSpec as _TopicSpec
from .utils import parse_byte_size
from humanfriendly import parse_timespan


class TopicMode(Enum):
    MIRROR = "mirror"
    ASSIGNED = "assigned"
    COMPUTED = "computed"


class CompressionType(Enum):
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ANY = "any"
    ZSTD = "zstd"


@dataclass(frozen=True)
class TopicSpec:
    mode: TopicMode = TopicMode.COMPUTED
    partitions: int = 1
    replications: int = 1
    ignore_rack: bool = True
    replica_assignment: Optional[List[PartitionMap]] = None
    retention_time: Optional[int] = None
    segment_size: Optional[int] = None
    compression_type: Optional[CompressionType] = None
    max_partition_size: Optional[int] = None
    system: bool = False

    @classmethod
    def new(cls, partitions: int = 1, replication: int = 1, ignore: bool = True):
        return cls(_TopicSpec.new_computed(partitions, replication, ignore))

    @classmethod
    def create(cls) -> "TopicSpec":
        """Alternative constructor method"""
        return cls()

    def with_assignment(self, replica_assignment: List[PartitionMap]) -> "TopicSpec":
        """Set the assigned replica configuration"""
        return replace(
            self, mode=TopicMode.ASSIGNED, replica_assignment=replica_assignment
        )

    def as_mirror_topic(self) -> "TopicSpec":
        """Set as a mirror topic"""
        return replace(self, mode=TopicMode.MIRROR)

    def with_partitions(self, partitions: int) -> "TopicSpec":
        """Set the specified partitions"""
        if partitions < 0:
            raise ValueError("Partitions must be a positive integer")
        return replace(self, partitions=partitions)

    def with_replications(self, replications: int) -> "TopicSpec":
        """Set the specified replication factor"""
        if replications < 0:
            raise ValueError("Replication factor must be a positive integer")
        return replace(self, replications=replications)

    def with_ignore_rack(self, ignore: bool = True) -> "TopicSpec":
        """Set the rack ignore setting"""
        return replace(self, ignore_rack=ignore)

    def with_compression(self, compression: CompressionType) -> "TopicSpec":
        """Set the specified compression type"""
        return replace(self, compression_type=compression)

    def with_retention_time(self, retention_time: Union[str, int]) -> "TopicSpec":
        """Set the specified retention time"""

        if isinstance(retention_time, int):
            return replace(self, retention_time=retention_time)

        parsed_time = parse_timespan(retention_time)
        return replace(self, retention_time=parsed_time)

    def with_segment_size(self, size: Union[str, int]) -> "TopicSpec":
        """Set the specified segment size"""
        if isinstance(size, int):
            return replace(self, segment_size=size)

        parsed_size = parse_byte_size(size)
        return replace(self, segment_size=parsed_size)

    def with_max_partition_size(self, size: Union[str, int]) -> "TopicSpec":
        """Set the specified max partition size"""
        if isinstance(size, int):
            return replace(self, max_partition_size=size)

        parsed_size = parse_byte_size(size)
        return replace(self, max_partition_size=parsed_size)

    def as_system_topic(self, is_system: bool = True) -> "TopicSpec":
        """Set the topic as an internal system topic"""
        return replace(self, system=is_system)

    def build(self) -> _TopicSpec:
        """Build the TopicSpec based on the current configuration"""
        # Similar implementation to the original build method
        if self.mode == TopicMode.ASSIGNED and self.replica_assignment:
            spec = _TopicSpec.new_assigned(self.replica_assignment)
        elif self.mode == TopicMode.MIRROR:
            spec = _TopicSpec.new_mirror()
        else:
            spec = _TopicSpec.new_computed(
                self.partitions, self.replications, self.ignore_rack
            )

        spec.set_system(self.system)

        if self.retention_time is not None:
            spec.set_retention_time(self.retention_time)

        if self.max_partition_size is not None or self.segment_size is not None:
            spec.set_storage(self.max_partition_size, self.segment_size)

        if self.compression_type is not None:
            spec.set_compression_type(self.compression_type.value)

        return spec
