import GxData from '../../_core_components/_data.jsx';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import RecommendedVirtualEnvironment from '../../_core_components/prerequisites/_recommended_virtual_environment.md';
import InfoUsingAVirtualEnvironment from '../../_core_components/admonitions/_if_you_are_using_a_virtual_environment.md';

GX Core uses the Python library `boto3` to access objects stored in Amazon S3 buckets, but you must configure your Amazon S3 account and credentials through AWS and the AWS command line interface (CLI).

## Prerequisites

- The AWS CLI. See [Installing or updating the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- AWS credentials. See [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
- <PrereqPythonInstalled/>
- <RecommendedVirtualEnvironment/>

## Installation

Python interacts with AWS through the `boto3` library. GX Core uses the library in the background when working with AWS. Although you won't use `boto3` directly, must install it in your Python environment.

To set up `boto3` with AWS, and use `boto3` within Python, see the [Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

1. Run the following code to verify the AWS CLI version:

   ```bash title="Terminal input"
   aws --version
   ```

   If this command does not return AWS CLI version information, reinstall or update the AWS CLI.  See [Install or update to the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

2. Run the following terminal command to install `boto3` in your Python environment:

   ```bash title="Terminal input"
   python -m pip install boto3
   ```

   :::tip

   If the `python -m pip install boto3` does not work, try:

   ```bash title="Terminal input"
   python3 -m pip install boto3
   ```
   
   If these `pip` commands don't work, verify that [Python is installed correctly](core/set_up_a_gx_environment/install_gx.md).

   :::

3. Run the following terminal command to verify your AWS credentials are properly configured:

   ```bash title="Terminal input"
   aws sts get-caller-identity
   ```

   When your credentials are properly configured, your `UserId`, `Account`, and `Arn` are returned. If your credentials are not configured correctly, an error message appears. If you received an error message, or you couldn't verify your credentials, see Amazon's documentation for [Configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
  
4. Install the Python dependencies for AWS S3 support.

   Run the following terminal command to install the optional dependencies required by GX Core to work with AWS S3:

   :::info
   <InfoUsingAVirtualEnvironment/>
   :::

   ```bash title="Terminal input"
   python -m pip install 'great_expectations[s3]'
   ```

   GX Core and the requirements for the `boto3` Python library are installed.
