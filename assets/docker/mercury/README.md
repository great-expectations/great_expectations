# Mercury Services

## Starting Services and Running Tests

- Set the environment variables for the Mercury service. The org ID and access token exist in the Mercury dev seed data and this is what is used in OSS CI:

```shell
export GX_CLOUD_BASE_URL=http://localhost:5000
export GX_CLOUD_ORGANIZATION_ID=
export GX_CLOUD_ACCESS_TOKEN=
```

- Install cloud testing dependencies by running:

```shell
pip install invoke
invoke deps --gx-install -m 'cloud' -r test
```

- Run the following `assume` command to authenticate with ECR so you can pull and run the Mercury services from AWS:

```shell
assume dev --exec 'aws ecr get-login-password --region us-east-1' | docker login --username AWS --password-stdin 258143015559.dkr.ecr.us-east-1.amazonaws.com
```

- You must have installed a docker compose version of at least 2.17.0 to use the invoke task. Check your version by running:

```shell
docker compose version
```

- Pull the containers and build by running:

```shell
invoke ci-tests 'cloud' --up-services --verbose
```

- Restart (or start) the containers and re-build by running:

```shell
invoke ci-tests 'cloud' --restart-services --verbose
```

- It will take some time (approximately 90 seconds as of October 2023) for the services to spin up completely because we are running the Mercury dev seed data script as an entrypoint.
