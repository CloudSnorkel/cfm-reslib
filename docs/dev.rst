Development
***********

Preparing Environment
=====================

1. Get the source code

.. code-block:: bash

    git clone https://github.com/CloudSnorkel/cfm-reslib.git``

2. Switch to the code directory

.. code-block:: bash

    cd cfm-reslib

3. Install requirements

.. code-block:: bash

    pip install -r requirements.txt


4. Create a virtual environment with all of the requirements

.. code-block:: bash

    poetry install

Run Tests
=========

Unit tests can be executed using py.test or simply with:

.. code-block:: bash

    poetry run py.test tests

Building
========

The building process creates a CloudFormation template that can be deployed and expose ``cfm-reslib`` to be imported by
other CloudFormation stacks. This template uses Lambda and its source code needs to be uploaded to a bucket. The build
script will create both a ZIP file and a template and will upload it to a given S3 bucket.

.. code-block:: bash

    BUCKET=my-bucket-name
    poetry run python build.py $BUCKET

And just like when deploying the released versions of cfm-reslib, you can deploy this with ``aws`` CLI tool.

.. code-block:: bash

    BUCKET=my-bucket-name
    aws cloudformation create-stack --stack-name cfm-reslib --template-url https://s3.amazonaws.com/$BUCKET/cfm-reslib-latest.template --capabilities CAPABILITY_IAM

Or when updating:

.. code-block:: bash

    BUCKET=my-bucket-name
    aws cloudformation update-stack --stack-name cfm-reslib --template-url https://s3.amazonaws.com/$BUCKET/cfm-reslib-latest.template --capabilities CAPABILITY_IAM

Note that you won't be able to deploy multiple stacks of cfm-reslib in the same region because the exported name has to
be unique across all stacks in a certain region.

Adding Custom Resources
=======================

There are two methods to implement a new custom resource. You will need to create a class for your resource in both.

1. If the custom resource uses just one boto3 call to create, update and delete a resource, you can inherit from
   :class:`cfmreslib.boto.BotoResourceHandler`. Simply override all of the constants with the names of the methods that
   need to be called and you're done. Check out ``ElasticTranscoderPipeline`` for an example.
2. If you need more control of the process, inherit from :class:`cfmreslib.base.CustomResourceHandler`. You will have to
   implement some methods that will be called for requests coming from CloudFormation. Check out ``Route53Certificate``
   for an example.

Once you've added your custom resource, make sure to add it to ``ALL_RESOURCES`` at the end of ``resources.py``.

Classes
-------

.. autoclass:: cfmreslib.base.CustomResourceHandler
   :members:
   :member-order: bysource

.. autoclass:: cfmreslib.boto.BotoResourceHandler
   :members: NAME, SERVICE, CREATE_METHOD, UPDATE_METHODS, EXISTS_METHOD, EXIST_READY_QUERY, DELETE_METHOD, NOT_FOUND_EXCEPTION, EXTRA_PERMISSIONS
   :member-order: bysource
