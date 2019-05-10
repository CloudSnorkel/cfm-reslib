import json
import os
import traceback
from typing import Optional, Dict, List

import boto3
from botocore.vendored import requests

from cfmreslib import docs

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
    NAME = "<not set>"
    DESCRIPTION = "<not set>"

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

    @classmethod
    def write_docs(cls, doc: docs.DocWriter):
        doc.add_header(f"Custom::{cls.NAME}", "=")
        doc.add_paragraph(cls.DESCRIPTION)