.. _publishing_data_docs_to_s3:


##############################
Publishing Data Docs to S3
##############################

In this tutorial we will cover publishing a data docs site directly to s3. Publishing a site this way makes
reviewing and acting on validation results easy in a team, and provides a central location to review the expectations
currently configured for data assets under test.

Configuring data docs requires three simple steps:

1. Configure an S3 bucket.

**Modify the bucket name and region for your situation.**

.. code-block:: bash

    > aws s3api create-bucket --bucket data-docs.my_org --region us-east-1
    {
        "Location": "/data-docs.my_org"
    }

Configure your bucket policy to enable appropriate access. **IMPORTANT**: your policy should provide access only
to appropriate users; data-docs can include critical information about raw data and should generally **not** be
publicly accessible. The example policy below **enforces IP-based access**.

**Modify the bucket name and IP addresses below for your situation.**

.. code-block:: json

    {
      "Version": "2012-10-17",
      "Statement": [{
        "Sid": "Allow only based on source IP",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": [
          "arn:aws:s3:::data-docs.my_org",
          "arn:aws:s3:::data-docs.my_org/*"
        ],
        "Condition": {
          "IpAddress": {
            "aws:SourceIp": [
              "192.168.0.1/32",
              "2001:db8:1234:1234::/64"
            ]
          }
        }
      }
      ]
    }

Modify the policy above and save it to a file called `ip-policy.json` in your local directory. Then, run:

.. code-block:: bash

    > aws s3api put-bucket-policy --bucket data-docs.my_org --policy file://ip-policy.json


2. Edit your `great_expectations.yml` file to change the `data_docs_sites` configuration for the site you will publish.
**Add the `s3_site`** section below existing site configuration.

.. code-block:: yaml

    # ... additional configuration above
    data_docs_sites:
      local_site:
        class_name: SiteBuilder
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/
      s3_site:
        class_name: SiteBuilder
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: data-docs.my_org  # UPDATE the bucket name here to match the bucket you configured above.
    # ... additional configuration below


3. Build your documentation:

.. code-block:: bash

    > great_expectations docs build
    Building...

You're now ready to visit the site! Your site will be available at the following URL:
http://data-docs.my_org.s3.amazonaws.com/index.html


Additional Resources
======================

Optionally, you may wish to update static hosting settings for your bucket to enable AWS to automatically serve your
index.html file or a custom error file:

.. code-block:: bash

    > aws s3 website s3://data-docs.my_org/ --index-document index.html

For more information on static site hosting in AWS, see the following:
 - `AWS Website Hosting <https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html>`_
 - `AWS Static Site Access Permissions <https://docs.aws.amazon.com/en_pv/AmazonS3/latest/dev/WebsiteAccessPermissionsReqd.html>`_
 - `AWS Website configuration <https://docs.aws.amazon.com/AmazonS3/latest/dev/HowDoIWebsiteConfiguration.html>`_


