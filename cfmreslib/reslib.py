import traceback

from cfmreslib.base import SUCCESS, FAILED, UNABLE_TO_CREATE, send_cf_response
from cfmreslib.resources import ALL_RESOURCES


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
