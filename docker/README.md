# Overview

This contains Great Expectations Dockerfiles. The only currently actively maintained one is `Dockerfile.tests`.

## Example Commands

These commands will hopefully be scripted but they are presented here for the time being. These commands **require**
they are invoked in the root directory of this repo.

### Building a docker image

The template docker command for building an image is:
```
docker buildx build -f docker/Dockerfile.tests --tag <name>:<tag> --target <target> \
    --build-arg PYTHON_VERSION=<version> --build-arg SOURCE=<src> --build-arg BRANCH=<branch_name> \
    --build-arg GE_USAGE_STATISTICS_URL="<url>".
```

`<name>` and `<tag>` are arbitrary. Examples: `gx39_dev:develop`, `gx37_dev:mybranch`.

`<target>` is in `[dev, test]`. Default is `test`. `dev` installs the dependencies for running Great Expectations with
any of our supported backends. `test` installs test dependencies on top of these.

`<src>` is in `[github, local]`. Default is `local` which will copy your local repo into the image while `github` will
pull a branch from GitHub.

`<version>` is in `[3.8, 3.9, 3.10]`. Default is `3.8`.

`<branch_name>` is a Great Expectations branch present on GitHub. This only works when `<src>` is "github".

`<usage_url>` is the usage statistic url endpoint one wants to set in the image. The default is "" which sets it
to the great expectations default value.

### Running the image

**Run Template**

`docker run -it --rm <name>:<tag> <cmd>`

**Run pytest**

`docker run -it --rm <name>:<tag> pytest -v`

**Bash**

`docker run -it --rm <name>:<tag> bash`

**Bash with mounted repo**

`docker run -it --rm --mount type=bind,source=${PWD},target=/great_expectations -w /great_expectations <name>:<tag> bash`

This will mount your local great expectations repo on top the image's version of great expectations.
You can now edit your repo locally and the changes will be reflected in the docker container.






```
