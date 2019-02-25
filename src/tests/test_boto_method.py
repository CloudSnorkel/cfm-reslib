import unittest

from src.reslib import BotoMethod


class TestBotoMethod(unittest.TestCase):
    def test_hashing(self):
        m1 = BotoMethod("kafka", {"name": "create_cluster"})
        m2 = BotoMethod("kafka", {"name": "delete_cluster", "physical_id_argument": "Id"})
        m3 = BotoMethod("kafka", {"name": "create_cluster"})
        d = dict()

        d[m1] = 1
        d[m2] = 2

        assert len(d) == 2

        d[m3] = 3

        assert len(d) == 2
        assert d[m1] == d[m3]

    def test_iam_op(self):
        method = BotoMethod("kafka", {"name": "create_cluster"})

        assert method.iam_op == "kafka:CreateCluster"

    def test_str(self):
        method = BotoMethod("kafka", {"name": "create_cluster"})

        assert str(method) == "kafka.create_cluster"

    def test_arg_type(self):
        method = BotoMethod("kafka", {"name": "create_cluster"})

        assert method._get_arg_type([], "NumberOfBrokerNodes") == "integer"
        assert method._get_arg_type([], "ClusterName") == "string"
        assert method._get_arg_type([], "BrokerNodeGroupInfo") == "structure"
        assert method._get_arg_type(["BrokerNodeGroupInfo"], "BrokerAZDistribution") == "string"

        assert method._get_arg_type([], "NotAnArgumentThatActuallyExists") == ""
        assert method._get_arg_type(["HelloThere"], "NotAnArgumentThatActuallyExists") == ""
        assert method._get_arg_type(["BrokerNodeGroupInfo"], "NotAnArgumentThatActuallyExists") == ""
        assert method._get_arg_type(["BrokerNodeGroupInfo", "DoesNotExistForSure"], "Something") == ""
        assert method._get_arg_type(["BrokerNodeGroupInfo", "DoesNotExistForSure", "Hello"], "Something") == ""

    def test_coerce_args(self):
        method = BotoMethod("kafka", {"name": "create_cluster"})

        args = {
            "ClusterName": "test",
            "SomethingThatIsNotHere": "hello",
            "NumberOfBrokerNodes": "222",
            "BrokerNodeGroupInfo": {
                "StorageInfo": {
                    "EbsStorageInfo": {
                        "VolumeSize": "123",
                        "FooBar": "123"
                    }
                }
            }
        }
        expected = {
            "ClusterName": "test",
            "SomethingThatIsNotHere": "hello",
            "NumberOfBrokerNodes": 222,
            "BrokerNodeGroupInfo": {
                "StorageInfo": {
                    "EbsStorageInfo": {
                        "VolumeSize": 123,
                        "FooBar": "123"
                    }
                }
            }
        }
        assert dict(method._coerce_args(args)) == expected

        args = {
            "NumberOfBrokerNodes": "not a number",
        }
        assert dict(method._coerce_args(args)) == args
