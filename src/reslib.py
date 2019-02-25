import collections
import json
import os
import time
import traceback
from typing import Dict, List, Optional

import boto3
from botocore.utils import CachedProperty
from botocore.vendored import requests

SUCCESS = "SUCCESS"
FAILED = "FAILED"
UNABLE_TO_CREATE = "XXXX_UNABLE_TO_CREATE_XXXX"
AWS_SESSION = boto3.Session()


def send_cf_response(event, context, response_status, response_data, physical_resource_id, reason=None):
    """
    Sends a response back to CloudFormation with the result of the resource operation.

    For more information on the request object see:
    https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/template-custom-resources.html
    """
    response_url = event["ResponseURL"]

    response = {
        "Status": response_status,
        "Reason": reason or "See the details in CloudWatch Log Stream: " + context.log_stream_name,
        "PhysicalResourceId": physical_resource_id,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": False,
        "Data": response_data
    }

    print("Responding to CloudFormation with", response)
    cf_response = requests.put(response_url, json=response, headers={"content-type": ""})
    print("CloudFormation result:", cf_response.reason, cf_response.text)


def _select(data, query):
    for q in query.split("."):
        data = data[q]
    return data


def _diff_attributes(old_props, new_props):
    old_attrs = set(old_props.keys())
    new_attrs = set(new_props.keys())

    # attributes only in old or only in new
    yield from old_attrs.symmetric_difference(new_attrs)

    # attributes that changed value
    for k in old_attrs.intersection(new_attrs):
        if old_props[k] != new_props[k]:
            yield k


def _clean_properties(props):
    args = dict(props)
    del args["ServiceToken"]
    return args


class CustomResourceHandler(object):
    def __init__(self):
        self.event = None
        self.context = None
        self.physical_id = None

    def handle(self, event, context):
        self.event = event
        self.context = context
        self.physical_id = event.get("PhysicalResourceId", UNABLE_TO_CREATE)

        try:
            if event["RequestType"] == "WaitReady":
                if self.ready():
                    self._success(self.data())
                else:
                    self._wait_ready()
            elif event["RequestType"] == "WaitDelete":
                if not self.__exists():
                    self._success({})
                else:
                    self._wait_delete()
            elif event["RequestType"] == "Create":
                self.__handle_create()
            elif event["RequestType"] == "Update":
                self.__handle_update()
            elif event["RequestType"] == "Delete":
                self.__handle_delete()
            else:
                self._fail(f"Invalid request type {event['RequestType']}")

        except Exception as e:
            traceback.print_exc()
            self._fail(str(e))

    def __exists(self):
        if self.physical_id == UNABLE_TO_CREATE:
            print("Does not exist because physical id is UNABLE_TO_CREATE")
            return False

        return self.exists()

    def __handle_create(self):
        self.create(_clean_properties(self.event["ResourceProperties"]))

    def __handle_update(self):
        if not self.__exists():
            print("Recreating resource because it doesn't exist")
            return self.__handle_create()

        old_arguments = _clean_properties(self.event["OldResourceProperties"])
        new_arguments = _clean_properties(self.event["ResourceProperties"])
        diff = list(_diff_attributes(old_arguments, new_arguments))
        print("Attributes diff:", diff)

        if self.can_update(old_arguments, new_arguments, diff):
            print("Recreating resource because modified attributes can't be updated")
            return self.__handle_create()

        self.update(old_arguments, new_arguments, diff)

    def __handle_delete(self):
        if not self.__exists():
            print("Resource no longer exists, so delete is technically done")
            return self._success({})

        self.delete()

    def _success(self, data):
        send_cf_response(self.event, self.context, SUCCESS, data, self.physical_id)

    def _fail(self, reason):
        send_cf_response(self.event, self.context, FAILED, {}, self.physical_id, reason=reason)

    def __wait(self, wait_action):
        # TODO limit repeats -- if cloudformation gave up, give up?
        event_copy = self.event.copy()
        event_copy["RequestType"] = wait_action
        event_copy["PhysicalResourceId"] = self.physical_id

        waiter_resource = AWS_SESSION.client("cloudformation").describe_stack_resource(
            StackName=os.getenv("THIS_STACK"),
            LogicalResourceId="Waiter",
        )
        AWS_SESSION.client("stepfunctions").start_execution(
            stateMachineArn=waiter_resource["StackResourceDetail"]["PhysicalResourceId"],
            input=json.dumps(event_copy),
        )

    def _wait_ready(self):
        print("Resource not ready yet, waiting...")
        self.__wait("WaitReady")

    def _wait_delete(self):
        print("Resource not deleted yet, waiting...")
        self.__wait("WaitDelete")

    # to be implemented by subclasses

    def exists(self) -> bool:
        """
        Checks if the resource specified in self.physical_id exists.

        * Must always be implemented
        :return: `True` if the resource exists, `False` if not
        """
        raise NotImplemented()

    def ready(self) -> bool:
        """
        Checks if the resource specified in self.physical_id is ready.

        * Must always be implemented
        * Can just return `True` if a resource existing means it's ready
        :return: `True` if the resource exists, `False` if not
        """
        raise NotImplemented()

    def data(self) -> Optional[Dict[str, object]]:
        """
        Retrieves the current data that should be returned for this resource.

        * Only required if :func:`_wait_ready()` is used
        :return: resource data, can be `None`
        """
        raise NotImplemented()

    def create(self, args: Dict[str, object]) -> None:
        """
        Creates a new resource with supplied arguments.

        * Must set `self.physical_id`
        * Must call :func:`_success`, :func:`_fail` or :func:`_wait_ready`
        * Must always be implemented

        :param args: arguments as passed from CloudFormation
        """
        raise NotImplemented()

    def can_update(self, old_args: Dict[str, object], new_args: Dict[str, object], diff: List[str]) -> bool:
        """
        Checks if a resource can safely be updated or whether a new one has to be created.

        * Must always be implemented, but can just return `False` if needed.
        :param old_args: existing arguments as passed from CloudFormation for the current resource
        :param new_args: requested arguments as passed from CloudFormation
        :param diff: a list of argument names that have changed value
        :return `True` if the resource can be updated or `False` if it needs to be recreated
        """
        raise NotImplemented()

    def update(self, old_args: Dict[str, object], new_args: Dict[str, object], diff: List[str]) -> None:
        """
        Updates the resource specified in self.physical_id based on the old and new arguments.

        * Must call :func:`_success`, :func:`_fail` or :func:`_wait_ready`
        * Only required if :func:`can_update()` ever returns `True`.
        :param old_args: existing arguments as passed from CloudFormation for the current resource
        :param new_args: requested arguments as passed from CloudFormation
        :param diff: a list of argument names that have changed value
        """
        raise NotImplemented()

    def delete(self) -> None:
        """
        Deletes the resource specified in self.physical_id .

        * Must call :func:`_success`, :func:`_fail` or :func:`_wait_delete`
        * Must always be implemented
        """
        raise NotImplemented()

    def get_iam_actions(self) -> List[str]:
        """
        Returns a list of required IAM permissions for all operations.

        * Must always be implemented
        """
        raise NotImplemented()


class BotoMethod(object):
    def __init__(self, service, method_desc):
        self.service = service
        self.method_name = method_desc["name"]
        self.physical_id_argument = method_desc.get("physical_id_argument")
        self.attributes_query = method_desc.get("attributes_query")
        self.physical_id_query = method_desc.get("physical_id_query")
        self._client = AWS_SESSION.client(service)
        self._method = getattr(self._client, self.method_name)

    @CachedProperty
    def method_input(self):
        op = self._client.meta.method_to_api_mapping[self.method_name]
        return self._client.meta.service_model.operation_model(op).input_shape.members

    @CachedProperty
    def iam_op(self):
        op = self._client.meta.method_to_api_mapping[self.method_name]
        return f"{self.service}:{op}"

    def _coerce_args(self, kwargs, path=[]):
        for k, v in kwargs.items():
            if self._get_arg_type(path, k) == "integer":
                try:
                    yield k, int(v)
                except ValueError:
                    yield k, v  # user will get an error saying an integer is expected when executing the method
            elif isinstance(v, dict):
                yield k, dict(self._coerce_args(v, path + [k]))
            else:
                yield k, v

    def _get_arg_type(self, path, name):
        try:
            members = self.method_input
            for p in path:
                members = members[p].members
            return members[name].type_name
        except KeyError:
            print("Unable to find input arg type for", self, path, name)
            return ""

    def __call__(self, **kwargs):
        coerced_kwargs = dict(self._coerce_args(kwargs))
        print("Calling", self, coerced_kwargs)
        return self._method(**coerced_kwargs)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, BotoMethod) and self.service == other.service and self.method_name == other.method_name

    def __str__(self):
        return f"{self.service}.{self.method_name}"


class BotoResourceHandler(CustomResourceHandler):
    NAME = None
    SERVICE = None
    CREATE_METHOD = {}
    UPDATE_METHODS = []
    EXISTS_METHOD = {}
    EXIST_READY_QUERY = {}
    DELETE_METHOD = {}
    NOT_FOUND_EXCEPTION = ""

    REPLACEMENT_REQUIRED_ATTRIBUTES = set()
    UPDATE_ATTRIBUTE_METHOD_MAP: Dict[str, BotoMethod] = {}
    EXTRA_PERMISSIONS = []

    def __init__(self):
        super().__init__()

        self._CLIENT = AWS_SESSION.client(self.SERVICE)

        create_input = self._create_method.method_input

        self.REPLACEMENT_REQUIRED_ATTRIBUTES = set(create_input.keys())
        for update_method in self._update_methods:
            for attr in update_method.method_input.keys():
                self.REPLACEMENT_REQUIRED_ATTRIBUTES.discard(attr)
                self.UPDATE_ATTRIBUTE_METHOD_MAP[attr] = update_method

        self._data = None

    @CachedProperty
    def _create_method(self):
        return BotoMethod(self.SERVICE, self.CREATE_METHOD)

    @CachedProperty
    def _update_methods(self):
        return [BotoMethod(self.SERVICE, m) for m in self.UPDATE_METHODS]

    @CachedProperty
    def _exists_method(self):
        return BotoMethod(self.SERVICE, self.EXISTS_METHOD)

    @CachedProperty
    def _delete_method(self):
        return BotoMethod(self.SERVICE, self.DELETE_METHOD)

    def _get_method_input(self, method_name):
        op = self._CLIENT.meta.method_to_api_mapping[method_name]
        return self._CLIENT.meta.service_model.operation_model(op).input_shape.members

    def create(self, args: Dict[str, object]) -> None:
        response = self._create_method(**args)
        self.physical_id = _select(response, self._create_method.physical_id_query)
        data = None
        if self._create_method.attributes_query:
            data = _select(response, self._create_method.attributes_query)

        if self.ready():
            self._success(data)
        else:
            self._wait_ready()

    def can_update(self, old_args: Dict[str, object], new_args: Dict[str, object], diff: List[str]) -> bool:
        return all(attr not in self.REPLACEMENT_REQUIRED_ATTRIBUTES for attr in diff)

    def update(self, old_args: Dict[str, object], new_args: Dict[str, object], diff: List[str]) -> None:
        ops: Dict[BotoMethod, Dict[str, object]] = collections.defaultdict(dict)
        for attr in diff:
            try:
                update_method = self.UPDATE_ATTRIBUTE_METHOD_MAP[attr]
                ops[update_method][update_method.physical_id_argument] = self.physical_id
                # we need .get(attr, "") here so we reset attributes that are being removed
                # for example ETS pipeline that had AwsKmsKeyArn and now doesn't
                # using None doesn't work as AWS validates the attribute type
                ops[update_method][attr] = self.event["ResourceProperties"].get(attr, "")
            except KeyError:
                return self._fail(f"Invalid attribute: {attr}")

        if not ops:
            return self._fail("Unable to find any update operations to execute")

        data = {}
        for update_method, update_arguments in ops.items():
            response = update_method(**update_arguments)
            data = _select(response, update_method.attributes_query)

        self._success(data)

    def delete(self) -> None:
        args = {
            self._delete_method.physical_id_argument: self.physical_id
        }
        self._delete_method(**args)
        if not self.exists():
            self._success(None)
        else:
            self._wait_delete()

    def data(self) -> Optional[Dict[str, object]]:
        return self._data

    def exists(self) -> bool:
        try:
            args = {
                self._exists_method.physical_id_argument: self.physical_id
            }
            response = self._exists_method(**args)
            if self._exists_method.attributes_query:
                self._data = _select(response, self._exists_method.attributes_query)
            return True
        except getattr(self._CLIENT.exceptions, self.NOT_FOUND_EXCEPTION):
            return False

    def ready(self) -> bool:
        if not self.EXIST_READY_QUERY:
            return True

        query = self.EXIST_READY_QUERY["query"]
        expected_value = self.EXIST_READY_QUERY["expected_value"]
        failed_values = self.EXIST_READY_QUERY["failed_values"]

        args = {
            self._exists_method.physical_id_argument: self.physical_id
        }
        data = self._exists_method(**args)
        value = _select(data, query)

        print(f"Resource state is {value}")

        if value == expected_value:
            print("Ready")
            return True
        if value in failed_values:
            print("Failed")
            raise RuntimeError(f"Invalid resource state {value}")

        print("Not ready")
        return False

    def get_iam_actions(self):
        for method in [self._create_method, self._exists_method, self._delete_method] + self._update_methods:
            yield method.iam_op

        yield from self.EXTRA_PERMISSIONS


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
        # TODO validate input
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


ALL_RESOURCES = [ElasticTranscoderPipeline, KafkaCluster, Route53Certificate]


def handler(event, context):
    """
    Handle resource operation request from CloudFormation.

    For event object description see:
    https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/crpg-ref-requests.html
    """
    print("event:", event)

    try:
        for resource in ALL_RESOURCES:
            if event["ResourceType"] == "Custom::" + resource.NAME:
                resource().handle(event, context)
                break
        else:
            status = FAILED
            if event["RequestType"] == "Delete" and event["PhysicalResourceId"] == UNABLE_TO_CREATE:
                # let users successfully "delete" bad resource types
                status = SUCCESS
            send_cf_response(
                event, context, status, None, UNABLE_TO_CREATE,
                reason=f"Invalid resource type {event['ResourceType']}"
            )
    except Exception as e:
        traceback.print_exc()
        send_cf_response(
            event, context, FAILED, None,
            event.get("PhysicalResourceId", UNABLE_TO_CREATE),
            reason=f"Unknown error {e}"
        )
