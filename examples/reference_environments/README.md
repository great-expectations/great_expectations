# Reference Environments

**WARNING** Reference environments are currently experimental and under development. The API may change rapidly before stabilizing.

Reference environments are intended to be a quick way to get started with Great Expectations and to provide a standard environment for reproducing issues.

As always, we welcome your feedback and contributions!

## Quickstart

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) is installed.
- [Docker Compose](https://docs.docker.com/compose/install/) is installed.
- [Great Expectations](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md) is installed by cloning the repo and following the contributor guidelines (reference environments are not currently available via `pip install great_expectations`).


### Start the reference environment

This example will show you how to start a reference environment for a postgres database.

1. Navigate to the repo root and run:

    ```bash
    great_expectations example postgres
    ```

2. Click on the jupyter notebook link in the output to open the notebook in your browser.

If there are more than one URL being displayed, you can use the following command to only show the URL for the notebook:

```bash
great_expectations example postgres --url
```

That's it!

The notebook contains a quickstart which you can edit to your heart's content.


### To stop the reference environment

1. Navigate to the repo root and run:

    ```bash
    great_expectations example postgres --stop
    ```

### To hop into a bash session instead of a notebook

1. Navigate to the repo root and run:

    ```bash
    great_expectations example postgres --bash
    ```

### To rebuild a reference environment

1. Navigate to the repo root and run:

    ```bash
    great_expectations example postgres --build
    ```

Alternatively you can run `docker ps` to find the container name and then run `docker exec -it <container_name> bash` to hop into a bash session. The above command is just a shortcut.

### What about other reference environments?

- We are working on adding more reference environments. To see the full list of what's available, run:

    ```bash
    great_expectations example --help
    ```

### What if I want to customize the reference environment?

- You can customize the reference environment by editing the files in `great_expectations/examples/reference_environments/` related to the reference environment you are interested in. For example the `compose.yml` file (if it exists for your environment) defines all the services that spin up when you start the environment.
- Look for documentation or comments in the reference environment for help on how it is used and how to customize it.
