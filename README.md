[![Build Status](https://travis-ci.org/CloudSnorkel/cfm-reslib.svg?branch=master)](https://travis-ci.org/CloudSnorkel/cfm-reslib) [![Documentation Status](https://readthedocs.org/projects/cfm-reslib/badge/?version=latest)](https://cfm-reslib.readthedocs.io/en/latest/?badge=latest)


# cfm-reslib

CloudFormation Resource Library: a collection of useful custom resources that are missing from CloudFormation.

## Available Resources (partial list)

* [Elastic Transcoder](https://aws.amazon.com/elastictranscoder/) pipeline (`Custom::ElasticTranscoderPipeline`)
* [Amazon Managed Streaming for Kafka](https://aws.amazon.com/msk/) cluster (`Custom::KafkaCluster`)
* Automatically Route 53 validated [AWS Certificate Manager](https://aws.amazon.com/certificate-manager/) certificate
(`Custom::Route53Certificate`)

For a complete list see See the [documentation](https://cfm-reslib.cloudsnorkel.com/en/latest/resources/).

Open issues to ask for more resources or pull requests with implementation.

## Instructions

cfm-reslib is delivered as a single CloudFormation template that exports a single output called `cfm-reslib`. To use it
you must first install it in the account and region where it will be used.

See the [documentation](https://cfm-reslib.cloudsnorkel.com/en/latest/instructions.html) for more information.

### Install

    aws cloudformation create-stack --stack-name cfm-reslib --template-url https://s3.amazonaws.com/cfm-reslib/cfm-reslib-latest.template --capabilities CAPABILITY_IAM
    
### Update

    aws cloudformation update-stack --stack-name cfm-reslib --template-url https://s3.amazonaws.com/cfm-reslib/cfm-reslib-latest.template --capabilities CAPABILITY_IAM
    
### Usage

Once installed cfm-reslib can be used by defining a custom resource with `ServiceToken` set to the exported value.

#### YAML

    Resources:
      TranscoderPipeline:
        Type: Custom::ElasticTranscoderPipeline
        Properties:
          ServiceToken: !ImportValue cfm-reslib
          Name: test
          InputBucket: input-bucket
          OutputBucket: output-bucket
          Role: arn:aws:iam::xxxxx:role/foobar
      Certificate:
        Type: Custom::Route53Certificate
        Properties:
          ServiceToken: !ImportValue cfm-reslib
          DomainName: foobar.acme.com
          SubjectAlternativeNames:
            - foobar2.acme.com
            - foobar3.acme.com

#### JSON

    {
      "Resources": {
        "TranscoderPipeline": {
          "Type": "Custom::ElasticTranscoderPipeline",
          "Properties": {
            "ServiceToken": {"Fn::ImportValue": "cfm-reslib"},
            "Name": "test",
            "InputBucket": "input-bucket",
            "OutputBucket": "output-bucket",
            "Role": "arn:aws:iam::xxxxx:role/foobar"
          }
        },
        "Certificate: {
          "Type": "Custom::ElasticTranscoderPipeline",
          "Properties": {
            "ServiceToken": {"Fn::ImportValue": "cfm-reslib"},
            "DomainName": "foobar.acme.com",
            "SubjectAlternativeNames": [
              "foobar2.acme.com",
              "foobar3.acme.com"
            ]
          }
        }
      }
    }