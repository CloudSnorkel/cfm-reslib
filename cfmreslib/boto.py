import collections
from typing import Dict, List, Optional

from botocore.utils import CachedProperty

from cfmreslib.base import AWS_SESSION, CustomResourceHandler


def _select(data, query):
    for q in query.split("."):
        data = data[q]
    return data


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
        return self.input_shape.members

    @CachedProperty
    def input_shape(self):
        op = self._client.meta.method_to_api_mapping[self.method_name]
        return self._client.meta.service_model.operation_model(op).input_shape

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
    NAME = None  #: Custom resource name to be used in CloudFormation with ``Custom::`` prefix.
    SERVICE = None  #: boto3 service name that will be used to create the client (e.g. s3, acm, ec2).
    #: Descriptor for method used to create resource. Requires "name" with the name of the method, and
    #: "physical_id_query" used to query for the physical id of the newly created resource from the method return value.
    CREATE_METHOD = {}
    #: Optional list of descriptor for methods used to update an existing resource. Each item requires "name" with the
    #: name of the method, and "physical_id_argument" with the name of the method argument that needs to have the
    #: physical id of the updated resource.
    UPDATE_METHODS = []
    #: Descriptor for method used to check if resource exists. Requires "name" with the name of the method, and
    #: "physical_id_argument" with the name of the method argument that needs to have the physical id of the checked
    #: resource. This method will raise the exception set in ``NOT_FOUND_EXCEPTION`` when the resource does not exist.
    EXISTS_METHOD = {}
    #: Optional descriptor of query to check against the result of ``EXISTS_METHOD``. When set we will wait until the
    #: resource is ready before finishing with create and update operations. Requires "query" with the query to run over
    #: the exists method result, "expected_value" with the expected value (e.g. READY), and "failed_values" with values
    #: that denote failure and should stop the operation.
    EXIST_READY_QUERY = {}
    #: Descriptor for method used to delete an existing resource. Requires "name" with the name of the method, and
    #: "physical_id_argument" with the name of the method argument that needs to have the physical id of the resource.
    DELETE_METHOD = {}
    #: Name of exception thrown by the exists method if the resource doesn't exist.
    NOT_FOUND_EXCEPTION = ""

    UPDATE_ATTRIBUTE_METHOD_MAP: Dict[str, BotoMethod] = {}
    #: A list of extra permissions required by any operations for this resource. Most permissions will be deduced by
    #: method names, but sometimes extra IAM permissions are required.
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

    @property
    def input_shape(self):
        return self._create_method.input_shape

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
