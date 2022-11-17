######################
Docker release process
######################

The Docker release process is simplified through the ``docker/build.py`` script.

Official Great Expectations images are hosted `on dockerhub: <https://hub.docker.com/r/greatexpectations/great_expectations>`_

Here is a step by step example with the ``0.11.4`` version.

1. Get the release branch you want to release a Docker image for:

.. code-block:: bash

    git fetch origin tag 0.11.4

2. Checkout the tag

.. code-block:: bash

    git checkout tags/0.11.4

3. Build the images

.. code-block:: bash

    python docker/build.py build-images --ge-version 0.11.4 --repository greatexpectations/great_expectations

The script builds images based on different "base" images, allowing users to have Python environments that match their local/testing environments.

The above command would have produced the following tags:
    - ``greatexpectations/great_expectations:python-3.6-buster-ge-0.11.4``
    - ``greatexpectations/great_expectations:python-3.7-buster-ge-0.11.4``

4. Publish the images

.. code-block:: bash

    python docker/build.py push-images --ge-version 0.11.4 --repository greatexpectations/great_expectations

.. note::

    You will need write access to the greatexpectations DockerHub repository in order to publish the Docker images
