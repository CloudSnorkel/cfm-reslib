import collections
import time
from typing import Dict, List, Optional

from cfmreslib import docs
from cfmreslib.base import AWS_SESSION, CustomResourceHandler
from cfmreslib.boto import BotoResourceHandler


class ElasticTranscoderPipeline(BotoResourceHandler):
    NAME = "ElasticTranscoderPipeline"
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

    def __init__(self):
        self._acm_client = AWS_SESSION.client("acm")
        self._route53_client = AWS_SESSION.client("route53")

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

    @classmethod
    def write_docs(cls, doc: docs.DocWriter):
        super().write_docs(doc)


ALL_RESOURCES = [ElasticTranscoderPipeline, KafkaCluster, Route53Certificate]
