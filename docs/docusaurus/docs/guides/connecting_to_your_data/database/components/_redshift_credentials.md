For this guide we will use a `connection_string` like this:

```
redshift+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>
```   

**Note**: Depending on your Redshift cluster configuration, you may or may not need the `sslmode` parameter. For more details, please refer to Amazon's documentation for [configuring security options on Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-ssl-support.html).
