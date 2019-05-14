Instructions
************


Installation
============

cfm-reslib is delivered as a single CloudFormation template that exports a single output called ``cfm-reslib``. To use
it you must first install it in the account and region where it will be used.

Install
-------

Installation is a simple one-liner. Make sure you have `AWS CLI <https://aws.amazon.com/cli/>`_ installed and configured.

::

    aws cloudformation create-stack --stack-name cfm-reslib --template-url https://s3.amazonaws.com/cfm-reslib/cfm-reslib-latest.template --capabilities CAPABILITY_IAM

You can also download the template and manually install it using `AWS Console <https://aws.amazon.com/console/>`_.

Update
------

If you've already installed this library before, you need to run a different command to update to the latest version.

::

    aws cloudformation update-stack --stack-name cfm-reslib --template-url https://s3.amazonaws.com/cfm-reslib/cfm-reslib-latest.template --capabilities CAPABILITY_IAM

Usage
=====

Once installed cfm-reslib can be used by defining a custom resource with ``ServiceToken`` set to the exported value.

See :ref:`resources` for a list of supported custom resource types.

YAML
----

.. code-block:: yaml

    Resources:
      SomeCustomResource:
        Type: Custom::SomeCustomResourceType
        Properties:
          ServiceToken: !ImportValue cfm-reslib
          SomeParameter: some value

JSON
----

.. code-block:: json

    {
      "Resources": {
        "SomeCustomResource": {
          "Type": "Custom::SomeCustomResourceType",
          "Properties": {
            "ServiceToken": {"Fn::ImportValue": "cfm-reslib"},
            "SomeParameter": "some value"
          }
        }
      }
    }
