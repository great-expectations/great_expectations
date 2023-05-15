If you have installed the AWS CLI, you can verify that your AWS credentials are properly configured by running the command:

```bash title="Terminal command"
aws sts get-caller-identity
```

If your credentials are properly configured, this will output your `UserId`, `Account` and `Arn`.  If your credentials are not configured correctly, this will throw an error.

If an error is thrown, or if you were unable to use the AWS CLI to verify your credentials configuration, you can find additional guidance on configuring your AWS credentials by referencing [Amazon's documentation on configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

