---
title: Install and set up Amazon S3 support
---

Amazon S3 is a service offered by [Amazon Web Services (AWS)](aws.amazon.com).  With it, you can access objects stored in S3 buckets through a web interface.  Great Expectations (GX) uses the Python library `boto3` to access S3, but you will need to configure your Amazon S3 account and credentials through AWS and the AWS command line interface (CLI).

## Prerequisites

- Install the AWS CLI.  See the official AWS documentation [Installing or updating the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) for guidance.

- Configure your AWS credentials.  See the offical AWS documentation [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for guidance.

## Installation

1. Verify the installation of the AWS CLI by running the command:

  ```bash title="Terminal command"
  aws --version
  ```

  If this command does not respond by informing you of the version information of the AWS CLI, you may need to install the AWS CLI or otherwise troubleshoot your current installation.  For detailed guidance on how to do this, please refer to [Amazon's documentation on how to install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))

2. Verify that your AWS credentials are properly configured by running the command:

  ```bash title="Terminal command"
  aws sts get-caller-identity
  ```

  If your credentials are properly configured, this will output your `UserId`, `Account` and `Arn`.  If your credentials are not configured correctly, this will throw an error.

  If an error is thrown, or if you were unable to use the AWS CLI to verify your credentials configuration, you can find additional guidance on configuring your AWS credentials by referencing [Amazon's documentation on configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
  
3. Install the Python dependencies for AWS S3 support.

  :::info 
  
  If you [installed GX in a virtual environment](/core/installation_and_setup/set_up_a_python_environment#optional-create-a-virtual-environment) then that environment should be active when installing these dependencies.
  
  :::

  To install the optional dependencies required by GX to work with AWS S3, run the command:

  ```bash title="Terminal input"
  python -m pip install 'great_expectations[s3]'
  ```

  This will install Great Expectations along with the requirements for the `boto3` Python library.
  
## Next steps

- [Install additional dependencies (if more are required)](/core/installation_and_setup/additional_dependencies/additional_dependencies.md)
- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)