import collections
import time
from typing import Dict, List, Optional

from botocore.model import DenormalizedStructureBuilder

from cfmreslib.base import AWS_SESSION, CustomResourceHandler
from cfmreslib.boto import BotoResourceHandler


class ElasticTranscoderPipeline(BotoResourceHandler):
    NAME = "ElasticTranscoderPipeline"
    DESCRIPTION = "The ``Custom::ElasticTranscoderPipeline`` resource creates an Elastic Transcoder pipeline."
    SERVICE = "elastictranscoder"
    CREATE_METHOD = {
        "name": "create_pipeline",
        "physical_id_query": "Pipeline.Id",
        "attributes_query": "Pipeline",
    }
    UPDATE_METHODS = [
        {
            "name": "update_pipeline",
            "physical_id_argument": "Id",
        },
        {
            "name": "update_pipeline_notifications",
            "physical_id_argument": "Id",
        },
        {
            "name": "update_pipeline_status",
            "physical_id_argument": "Id",
        },
    ]
    EXISTS_METHOD = {
        "name": "read_pipeline",
        "physical_id_argument": "Id",
    }
    DELETE_METHOD = {
        "name": "delete_pipeline",
        "physical_id_argument": "Id",
    }
    EXTRA_PERMISSIONS = ["iam:PassRole"]
    NOT_FOUND_EXCEPTION = "ResourceNotFoundException"


class KafkaCluster(BotoResourceHandler):
    NAME = "KafkaCluster"
    DESCRIPTION = "The ``Custom::KafkaCluster`` resource creates a Kafka Cluster (MSK). Now offificially available in CloudFormation with `AWS::MSK::Cluster <https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-msk-cluster.html>`_."
    SERVICE = "kafka"
    CREATE_METHOD = {
        "name": "create_cluster",
        "physical_id_query": "ClusterArn",
    }
    UPDATE_METHODS = []
    EXISTS_METHOD = {
        "name": "describe_cluster",
        "physical_id_argument": "ClusterArn",
        "attributes_query": "ClusterInfo",
    }
    EXIST_READY_QUERY = {
        "query": "ClusterInfo.State",
        "expected_value": "ACTIVE",
        "failed_values": ["DELETING", "FAILED"],
    }
    DELETE_METHOD = {
        "name": "delete_cluster",
        "physical_id_argument": "ClusterArn",
    }
    EXTRA_PERMISSIONS = ["ec2:DescribeSubnets", "ec2:DescribeVpcs", "ec2:DescribeSecurityGroups",
                         "iam:CreateServiceLinkedRole"]
    NOT_FOUND_EXCEPTION = "NotFoundException"


class Route53Certificate(CustomResourceHandler):
    # TODO BUGBUG updating this resource will cause creation of new resource and then deletion of old one
    #             deleting the old resource will also delete route 53 records which might be shared with new resource
    NAME = "Route53Certificate"
    DESCRIPTION = "The ``Custom::Route53Certificate`` resource requests an AWS Certificate Manager (ACM) certificate " \
                  "that you can use to enable secure connections. For example, you can deploy an ACM certificate to " \
                  "an Elastic Load Balancer to enable HTTPS support. For more information, see RequestCertificate in " \
                  "the AWS Certificate Manager API Reference.\n\nUnlike ``AWS::CertificateManager::Certificate``, " \
                  "this resource automatically validates the certificate for you. This only works if you request a " \
                  "certificate for a domain that's hosted on Route53."
    REPLACEMENT_REQUIRED_ATTRIBUTES = {"DomainName", "SubjectAlternativeNames"}

    def __init__(self):
        super().__init__()
        self._acm_client = AWS_SESSION.client("acm")
        self._route53_client = AWS_SESSION.client("route53")

    @property
    def input_shape(self):
        # TODO make a copy of the shape somehow
        shape = self._acm_client.meta.service_model.operation_model("RequestCertificate").input_shape
        for m in set(shape.members.keys()):
            if m not in ["DomainName", "SubjectAlternativeNames"]:
                del shape.members[m]
        return shape

    def create(self, args: Dict[str, object]) -> None:
        # TODO validate input. define botocore shape for auto doc?
        response = self._acm_client.request_certificate(
            # DomainName=args["DomainName"],
            # SubjectAlternativeNames=args.get("SubjectAlternativeNames"),
            ValidationMethod="DNS",
            **args
        )
        self.physical_id = response["CertificateArn"]

        self._wait_for_validation_resources()
        self._update_route53("UPSERT")
        self._wait_ready()

    def _wait_for_validation_resources(self):
        while True:
            desc = self._acm_client.describe_certificate(CertificateArn=self.physical_id)

            # check status
            status = desc["Certificate"]["Status"]
            if status == "ISSUED":
                return
            if status != "PENDING_VALIDATION":
                failure_reason = desc["Certificate"].get("FailureReason", "Unknown")
                raise RuntimeError(f"Certificate status is {status}: {failure_reason}")

            # check that all domains have resource record request
            for validation in desc["Certificate"]["DomainValidationOptions"]:
                if "ResourceRecord" not in validation:
                    break
            else:
                # no validation request is empty
                return

            time.sleep(10)

    def _get_domains(self) -> Dict[str, Dict[str, str]]:
        cnames = {}
        desc = self._acm_client.describe_certificate(CertificateArn=self.physical_id)
        for validation in desc["Certificate"]["DomainValidationOptions"]:
            if "ResourceRecord" in validation:
                cnames[validation["ResourceRecord"]["Name"]] = validation["ResourceRecord"]["Value"]

        hosted_zones = {}
        for zone_set in self._route53_client.get_paginator("list_hosted_zones").paginate():
            for zone in zone_set["HostedZones"]:
                hosted_zones[zone["Name"]] = zone["Id"]

        result = collections.defaultdict(dict)
        for cname, value in cnames.items():
            for zone, zone_id in hosted_zones.items():
                cleared_zone = zone.strip(".")
                cleared_cname = cname.strip(".")
                if cleared_cname == cleared_zone or cleared_cname.endswith(f".{cleared_zone}"):
                    result[zone_id][cname] = value
                    break
            else:
                raise RuntimeError(f"Unable to find hosted zone for {cname}, is it hosted on Route 53?")

        return result

    def _update_route53(self, action):
        for zone_id, cnames in self._get_domains().items():
            self._route53_client.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Comment": f"Validation records for {self.physical_id} by CloudFormation",
                    "Changes": [
                        {
                            "Action": action,
                            "ResourceRecordSet": {
                                "Name": cname,
                                "Type": "CNAME",
                                "TTL": 3600,
                                "ResourceRecords": [
                                    {
                                        "Value": value,
                                    },
                                ]
                            }
                        }
                        for cname, value in cnames.items()
                    ]
                }
            )

    def can_update(self, old_args: Dict[str, object], new_args: Dict[str, object], diff: List[str]) -> bool:
        return False

    def delete(self) -> None:
        self._update_route53("UPSERT")  # can't delete something that's not there
        self._update_route53("DELETE")
        self._acm_client.delete_certificate(CertificateArn=self.physical_id)
        self._success(None)

    def data(self) -> Optional[Dict[str, object]]:
        return None

    def exists(self) -> bool:
        try:
            self._acm_client.describe_certificate(CertificateArn=self.physical_id)
            return True
        except self._acm_client.exceptions.ResourceNotFoundException:
            return False

    def ready(self) -> bool:
        desc = self._acm_client.describe_certificate(CertificateArn=self.physical_id)
        status = desc["Certificate"]["Status"]
        if status == "ISSUED":
            return True

        if status == "PENDING_VALIDATION":
            return False

        raise RuntimeError(f"Invalid resource status {status}")

    def get_iam_actions(self) -> List[str]:
        return [
            "acm:DeleteCertificate",
            "acm:DescribeCertificate",
            "acm:RequestCertificate",
            "acm:UpdateCertificateOptions",
            "route53:ChangeResourceRecordSets",
            "route53:ListHostedZones",
        ]


class FindAMI(CustomResourceHandler):
    NAME = "FindAMI"
    DESCRIPTION = "The ``Custom::FindAMI`` resource finds an AMI by owner, name and architecture. The result can then" \
                  "be used with `Ref <https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic" \
                  "-function-reference-ref.html>`_ "
    EXAMPLES = [
        {
            "title": "Create EC2 Instance With Latest Ubuntu",
            "description": "The following example searches for the latest version of Ubuntu 16.04 AMI and creates a new"
                           "EC2 instance with this image.",
            "template": {
                "UbuntuAMI": {
                    "Type": "Custom::FindAMI",
                    "Properties": {
                        "ServiceToken": {"Fn::ImportValue": "cfm-reslib"},
                        "Owner": "099720109477",
                        "Name": "ubuntu/images/hvm-ssd/ubuntu-xenial-16.04*",
                        "Architecture": "x86_64",
                    }
                },
                "UbuntuInstance": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "InstanceType": "t2.micro",
                        "ImageId": {"Ref": "UbuntuAMI"},
                    }
                }
            },
        }
    ]
    REPLACEMENT_REQUIRED_ATTRIBUTES = {"Owner", "Name", "Architecture"}

    def __init__(self):
        super().__init__()
        self._ec2_client = AWS_SESSION.client("ec2")

    @property
    def input_shape(self):
        return DenormalizedStructureBuilder().with_members({
            "Owner": {
                "type": "string",
                "documentation": "Image owner (e.g. \"679593333241\" for CentOS)",
            },
            "Name": {
                "type": "string",
                "documentation": "Image name (e.g. \"CentOS Linux 7 x86_64 HVM EBS *\")",
            },
            "Architecture": {
                "type": "string",
                "documentation": "Image architecture (e.g. \"x86_64\")",
            },
        }).build_model()

    def create(self, args: Dict[str, object]) -> None:
        response = self._ec2_client.describe_images(
            Owners=[args['Owner']],
            Filters=[
                {'Name': 'name', 'Values': [args['Name']]},
                {'Name': 'architecture', 'Values': [args['Architecture']]},
                {'Name': 'root-device-type', 'Values': ['ebs']},
            ],
        )

        ami_list = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)

        if not ami_list:
            raise RuntimeError("No images found")

        self.physical_id = ami_list[0]['ImageId']
        self._success(None)

    def can_update(self, old_args: Dict[str, object], new_args: Dict[str, object], diff: List[str]) -> bool:
        return False

    def delete(self) -> None:
        self._success(None)

    def exists(self) -> bool:
        return True

    def ready(self) -> bool:
        return True

    def get_iam_actions(self) -> List[str]:
        return [
            "ec2:DescribeImages",
        ]


ALL_RESOURCES = [ElasticTranscoderPipeline, KafkaCluster, Route53Certificate, FindAMI]
