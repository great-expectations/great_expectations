# Mercury Services

## Starting Services and Running Tests

- Set the environment variables for the Mercury service. The org ID and access token only exist in dev seed data:

```shell
export GX_CLOUD_BASE_URL=http://localhost:5000
export GX_CLOUD_ORGANIZATION_ID=0ccac18e-7631-4bdd-8a42-3c35cce574c6
export GX_CLOUD_ACCESS_TOKEN=5266c9ac7a844b91994e7bfc443bdeae.V1.UB3fpuYpsqxVX3XSXipYgfzPrvOG7jAhXE
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
