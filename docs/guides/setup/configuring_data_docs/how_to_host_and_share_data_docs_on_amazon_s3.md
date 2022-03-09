---
title: How to host and share Data Docs on Amazon S3
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on AWS S3.
import TechnicalTag from '/docs/term_tags/_tag.mdx';

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- [Set up the AWS Command Line Interface](https://aws.amazon.com/cli/)

</Prerequisites>

Steps
-----

1. **Configure an S3 bucket.**

  You can configure an S3 bucket using the AWS CLI. Make sure you modify the bucket name and region for your situation.

  ```bash
> aws s3api create-bucket --bucket data-docs.my_org --region us-east-1
{
    "Location": "/data-docs.my_org"
}
  ```

2. **Configure your bucket policy to enable appropriate access.**

  The example policy below **enforces IP-based access** - modify the bucket name and IP addresses for your situation. After you have customized the example policy to suit your situation, save it to a file called ``ip-policy.json`` in your local directory.

  :::important

  Your policy should provide access only to appropriate users. Data Docs sites can include critical information about raw data and should generally **not** be publicly accessible.

  :::

  ```json
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
  ```

3. **Apply the policy.**

  Run the following <TechnicalTag relative="../../../" tag="cli" text="CLI" /> command to apply the policy:

  ```bash
  > aws s3api put-bucket-policy --bucket data-docs.my_org --policy file://ip-policy.json
  ```

4. **Add a new S3 site to the data_docs_sites section of your great_expectations.yml**

  You may also replace the default ``local_site`` if you would only like to maintain a single S3 Data Docs site.

  ```yaml
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
  s3_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: data-docs.my_org  # UPDATE the bucket name here to match the bucket you configured above.
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
      show_cta_footer: true
  ```

5. **Test that your configuration is correct by building the site.**

  Use the following CLI command: ``great_expectations docs build --site-name s3_site``. If successful, the CLI will open your newly built S3 Data Docs site and provide the URL, which you can share as desired. Note that the URL will only be viewable by users with IP addresses appearing in the above policy.

  ```bash
  > great_expectations docs build --site-name s3_site

  The following Data Docs sites will be built:

   - s3_site: https://s3.amazonaws.com/data-docs.my_org/index.html

  Would you like to proceed? [Y/n]: Y

  Building Data Docs...

  Done building Data Docs
  ```  

  Note you may want to use the `-y/--yes/--assume-yes` flag which skips the confirmation dialog.
  This can be useful for non-interactive environments.

Additional notes
----------------

- Optionally, you may wish to update static hosting settings for your bucket to enable AWS to automatically serve your
index.html file or a custom error file:

  ```bash
  > aws s3 website s3://data-docs.my_org/ --index-document index.html
  ```


- If you wish to host a Data Docs site in a subfolder of an S3 bucket, add the ``prefix`` property to the configuration snippet in step 4, immediately after the ``bucket`` property.

- If you wish to host a Data Docs site through a private DNS, you can configure a ``base_public_path`` for the <TechnicalTag relative="../../../" tag="data_docs_store" text="Data Docs Store" />.  The following example will configure a S3 site with the ``base_public_path`` set to ``www.mydns.com``.  Data Docs will still be written to the configured location on S3 (for example ``https://s3.amazonaws.com/data-docs.my_org/docs/index.html``), but you will be able to access the pages from your DNS (``http://www.mydns.com/index.html`` in our example)

    ```yaml
    data_docs_sites:
      s3_site:  # this is a user-selected name - you may select your own
        class_name: SiteBuilder
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: data-docs.my_org  # UPDATE the bucket name here to match the bucket you configured above.
          base_public_path: http://www.mydns.com
        site_index_builder:
          class_name: DefaultSiteIndexBuilder
          show_cta_footer: true
    ```


Additional resources
--------------------

- [AWS Website Hosting](https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html)
- [AWS Static Site Access Permissions](https://docs.aws.amazon.com/en_pv/AmazonS3/latest/dev/WebsiteAccessPermissionsReqd.html)
- [AWS Website configuration](https://docs.aws.amazon.com/AmazonS3/latest/dev/HowDoIWebsiteConfiguration.html)
- [Core concepts: Data Docs](../../../reference/data_docs.md)

