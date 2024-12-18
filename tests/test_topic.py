import unittest

from fluvio import TopicSpec, TopicMode, CompressionType

class TestTopicSpec(unittest.TestCase):
    def test_default_creation(self):
        """Test default topic creation"""
        topic = TopicSpec.create()
        
        self.assertEqual(topic.mode, TopicMode.COMPUTED)
        self.assertEqual(topic.partitions, 1)
        self.assertEqual(topic.replications, 1)
        self.assertTrue(topic.ignore_rack)
        self.assertIsNone(topic.replica_assignment)
        self.assertIsNone(topic.retention_time)
        self.assertIsNone(topic.segment_size)
        self.assertIsNone(topic.compression_type)
        self.assertIsNone(topic.max_partition_size)
        self.assertFalse(topic.system)

    def test_topic_immutability(self):
        """Ensure topic instances are immutable"""
        topic = TopicSpec.create()
        
        # These should create new instances, not modify the original
        topic2 = topic.with_partitions(3)
        topic3 = topic.with_replications(2)
        
        self.assertIsNot(topic, topic2)
        self.assertIsNot(topic, topic3)
        self.assertEqual(topic.partitions, 1)  # Original remains unchanged
        self.assertEqual(topic2.partitions, 3)
        self.assertEqual(topic3.replications, 2)

    def test_topic_with_methods(self):
        """Test all with_* methods create correct instances"""
        topic = (TopicSpec.create()
            .with_partitions(3)
            .with_replications(2)
            .with_ignore_rack()
            .with_retention_time(86400)
            .with_segment_size(1024)
            .with_compression(CompressionType.GZIP)
            .with_max_partition_size(2048)
            .as_system_topic()
        )
        
        self.assertEqual(topic.partitions, 3)
        self.assertEqual(topic.replications, 2)
        self.assertTrue(topic.ignore_rack)
        self.assertEqual(topic.retention_time, 86400)
        self.assertEqual(topic.segment_size, 1024)
        self.assertEqual(topic.compression_type, CompressionType.GZIP)
        self.assertEqual(topic.max_partition_size, 2048)
        self.assertTrue(topic.system)

    def test_system_topic(self):
        """Test system topic configuration"""
        # Default not system
        default_topic = TopicSpec.create()
        self.assertFalse(default_topic.system)
        
        # Explicitly set as system
        system_topic = default_topic.as_system_topic()
        self.assertTrue(system_topic.system)
        
        # Optional parameter for non-system
        non_system_topic = default_topic.as_system_topic(False)
        self.assertFalse(non_system_topic.system)

    def test_compression_types(self):
        """Test all compression types can be set"""
        for compression in CompressionType:
            topic = TopicSpec.create().with_compression(compression)
            self.assertEqual(topic.compression_type, compression)

    def test_chaining_multiple_configurations(self):
        """Test complex chaining of configuration methods"""
        topic = (TopicSpec.create()
            .with_partitions(5)
            .with_replications(3)
            .as_mirror_topic()
            .with_retention_time(172800)  # 2 days
            .as_system_topic()
        )
        
        self.assertEqual(topic.mode, TopicMode.MIRROR)
        self.assertEqual(topic.partitions, 5)
        self.assertEqual(topic.replications, 3)
        self.assertEqual(topic.retention_time, 172800)
        self.assertTrue(topic.system) 

    def test_with_time_and_size_parsing(self):
        """Test creating topics with parsed time and size"""
        topic = (TopicSpec.create()
            .with_retention_time('1d')
            .with_segment_size('500M')
            .with_max_partition_size('1G')
        )
        
        self.assertEqual(topic.retention_time, 86400)
        self.assertEqual(topic.segment_size, 500 * 1000 * 1000)
        self.assertEqual(topic.max_partition_size, 1000 * 1000 * 1000)

        topic = (TopicSpec.create()
            .with_retention_time('2m')
            .with_segment_size('500mb')
            .with_max_partition_size('1gb')
        )

        self.assertEqual(topic.retention_time, 2 * 60)
        self.assertEqual(topic.segment_size, 500 * 1000 * 1000)
        self.assertEqual(topic.max_partition_size, 1000 * 1000 * 1000)

        topic = (TopicSpec.create()
            .with_retention_time('3h')
            .with_segment_size('500MiB')
            .with_max_partition_size('1GiB')
        )

        self.assertEqual(topic.retention_time, 3 * 3600) 
        self.assertEqual(topic.segment_size, 500 * 1024 * 1024)
        self.assertEqual(topic.max_partition_size, 1024 * 1024 * 1024)


        topic = (TopicSpec.create()
            .with_retention_time('4s')
            .with_segment_size('500MB')
            .with_max_partition_size('1GB')
        )
        
        self.assertEqual(topic.retention_time, 4)
        self.assertEqual(topic.segment_size, 500 * 1000 * 1000)
        self.assertEqual(topic.max_partition_size, 1000 * 1000 * 1000)


    def test_with_numeric_inputs(self):
        """Test creating topics with numeric inputs"""
        topic = (TopicSpec.create()
            .with_retention_time(3600)
            .with_segment_size(1024 * 1024)
            .with_max_partition_size(2 * 1024 * 1024 * 1024)
        )
        
        # Verify numeric inputs
        self.assertEqual(topic.retention_time, 3600)
        self.assertEqual(topic.segment_size, 1024 * 1024)
        self.assertEqual(topic.max_partition_size, 2 * 1024 * 1024 * 1024)

    def test_invalid_configurations(self):
        """Test handling of potentially invalid configurations"""
        with self.assertRaises(ValueError):
            TopicSpec.create().with_partitions(-1)
        
        with self.assertRaises(ValueError):
            TopicSpec.create().with_replications(-1)


if __name__ == '__main__':
    unittest.main()
