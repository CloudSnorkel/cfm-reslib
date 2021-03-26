import argparse
import hashlib
import io
import json
import os
import pathlib
import sys
import typing
import zipfile

import boto3
import requests
import troposphere.awslambda
import troposphere.cloudformation
import troposphere.config
import troposphere.events
import troposphere.firehose
import troposphere.iam
import troposphere.kinesis
import troposphere.logs
import troposphere.s3
import troposphere.sns
import troposphere.stepfunctions


def add_lambda_role(template: troposphere.Template) -> troposphere.iam.Role:
    role = troposphere.iam.Role(
        f"LambdaRole", template,
        AssumeRolePolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "lambda.amazonaws.com"
                        ]
                    },
                    "Action": [
                        "sts:AssumeRole"
                    ]
                }
            ],
        },
        ManagedPolicyArns=["arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"],
        Policies=[
            troposphere.iam.Policy(
                PolicyName="GetWaiterARN",
                PolicyDocument={
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "cloudformation:DescribeStackResource",
                            "Resource": troposphere.StackId,
                        },
                    ]
                }
            ),
            troposphere.iam.Policy(
                PolicyName="CustomAPIs",
                PolicyDocument={
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": list(_get_iam_actions()),
                            "Resource": "*",
                        },
                    ]
                }
            ),
        ]
    )

    troposphere.iam.PolicyType(
        "CallWaiterPolicy", template,
        Roles=[role.ref()],
        PolicyName="CallWaiter",
        PolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "states:StartExecution",
                    "Resource": troposphere.Ref("Waiter"),
                },
            ]
        }
    )

    return role


def add_state_machine_role(template: troposphere.Template,
                           function: troposphere.awslambda.Function) -> troposphere.iam.Role:
    role = troposphere.iam.Role(
        "StateMachineRole", template,
        AssumeRolePolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "states.amazonaws.com"
                        ]
                    },
                    "Action": [
                        "sts:AssumeRole"
                    ]
                }
            ],
        },
        Policies=[
            troposphere.iam.Policy(
                PolicyName="CallHandler",
                PolicyDocument={
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "lambda:InvokeFunction",
                            "Resource": function.get_att("Arn"),
                        },
                    ]
                }
            ),
        ]
    )

    return role


def add_lambda(template: troposphere.Template, bucket: str, zip_name: str) -> troposphere.awslambda.Function:
    return troposphere.awslambda.Function(
        "CustomResourceHandler", template,
        Description="Handle custom resources for cfm-reslib",
        Runtime="python3.6",
        Code=troposphere.awslambda.Code(S3Bucket=bucket, S3Key=zip_name),
        Handler="cfmreslib.reslib.handler",
        Role=add_lambda_role(template).get_att("Arn"),
        Timeout=900,
    )


def add_state_machine(template, function: troposphere.awslambda.Function):
    fn_arn_sub = "${" + function.title + ".Arn}"

    state_machine = troposphere.stepfunctions.StateMachine(
        "Waiter", template,
        DefinitionString=troposphere.Sub(
            json.dumps({
                "StartAt": "Wait",
                "States": {
                    "Wait": {
                        "Type": "Wait",
                        "Seconds": 30,  # TODO configurable?
                        "Next": "Request",
                    },
                    "Request": {
                        "Type": "Task",
                        "Resource": fn_arn_sub,
                        "End": True,
                    },
                }
            })),
        RoleArn=add_state_machine_role(template, function).get_att("Arn"),
    )

    return state_machine


def get_package_zip(package: str) -> zipfile.ZipFile:
    cache_folder = pathlib.Path(".cache")
    cache_folder.mkdir(exist_ok=True)

    package_cache_path = cache_folder.joinpath(f"{package}.whl")

    if package_cache_path.is_file():
        return zipfile.ZipFile(package_cache_path.open("rb"))

    for u in requests.get(f"https://pypi.org/pypi/{package}/json").json()["urls"]:
        if u["packagetype"] == "bdist_wheel":
            content = requests.get(u["url"]).content
            package_cache_path.write_bytes(content)
            return zipfile.ZipFile(io.BytesIO(content))

    raise RuntimeError(f"Unable to get {package}")


def add_package_to_zip(zip_file: zipfile.ZipFile, package: str):
    package_zip = get_package_zip(package)

    for f in package_zip.infolist():
        zip_file.writestr(f, package_zip.read(f), zipfile.ZIP_DEFLATED)


def gen_zip() -> bytes:
    new_zip_file = io.BytesIO()
    new_zip = zipfile.ZipFile(new_zip_file, "w")

    for root, dirs, files in os.walk("cfmreslib"):
        if "__pycache__" in root:
            continue

        for f in files:
            if f.endswith(".pyc"):
                continue

            path = os.path.join(root, f)
            zip_path = os.path.relpath(path, ".")
            zi = zipfile.ZipInfo(zip_path)
            zi.external_attr = 0o644 << 16
            new_zip.writestr(zi, open(path).read(), zipfile.ZIP_DEFLATED)

    add_package_to_zip(new_zip, "boto3")
    add_package_to_zip(new_zip, "botocore")
    add_package_to_zip(new_zip, "urllib3")

    new_zip.close()
    new_zip_file.seek(0)

    return new_zip_file.read()


def _get_iam_actions() -> typing.List[str]:
    import cfmreslib.reslib
    for res in cfmreslib.resources.ALL_RESOURCES:
        yield from res().get_iam_actions()


def _upload_template(bucket: str, tag: str):
    template = troposphere.Template("CloudFormation resource library (cfm-reslib)")

    # TODO auto generate documentation
    print("Building zip...")
    zip_data = gen_zip()
    zip_hash = hashlib.new("sha1", zip_data).hexdigest()
    zip_name = f"{zip_hash}.zip"
    print("Uploading code zip...")
    s3 = boto3.client("s3")
    if "Contents" not in s3.list_objects(Bucket=bucket, Prefix=zip_name):
        s3.put_object(Bucket=bucket, Key=zip_name, Body=gen_zip())
    print("Done")

    print("Generating template...")
    function = add_lambda(template, bucket, zip_name)
    add_state_machine(template, function)
    function.Environment = troposphere.awslambda.Environment(
        Variables={
            "THIS_STACK": troposphere.StackId,
        }
    )

    troposphere.Output(
        "ServiceToken",
        template=template,
        Value=function.get_att("Arn"),
        Description="Ues this value in ServiceToken for your custom resources",
        Export=troposphere.Export("cfm-reslib"),
    )

    print("Uploading template...")
    s3.put_object(Bucket=bucket, Key=f"cfm-reslib-{tag}.template", Body=template.to_yaml())

    print("Done")


def main(argv):
    parser = argparse.ArgumentParser(description="Build and upload cfm-reslib")
    parser.add_argument("bucket", metavar="S3_BUCKET",
                        help="S3 bucket where artifacts will be copied")
    parser.add_argument("-t", "--tag", default="latest",
                        help="Set template name to cfm-reslib-<TAG>.template")

    args = parser.parse_args(argv)

    _upload_template(args.bucket, args.tag)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
