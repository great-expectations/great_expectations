# Mercury Services

Note: Running Mercury services locally requires access to the GX private docker registry.
      External contributors will not be able to follow these instructions and will only be able to run these tests in CI.

## Starting Services and Running Tests

- Set the environment variables for the Mercury service. The org ID and access token only exist in dev seed data. Get the variable values from DevOps.:

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

- Ensure you have `granted` installed and configured. Follow the instructions at [this confluence page](https://greatexpectations.atlassian.net/wiki/spaces/SUP/pages/450068501/Granted+for+AWS+Access).

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
