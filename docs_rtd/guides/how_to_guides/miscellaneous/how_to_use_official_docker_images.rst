.. _how_to_guides__miscellaneous__how_to_use_official_docker_images:

How to use the Great Expectation Docker images
=================================

This guide will help you use the official Great Expectations Docker images.
This is useful if you wish to have a fully portable Great Expectations runtime that can be used locally or deployed on the cloud.

.. admonition:: Prerequisites: This how-to guide assumes you have:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Installed Docker on your machine

Steps
-----

#. First, choose which image you'd like to use by browsing the official `Great Expectations Docker image registry <https://hub.docker.com/r/greatexpectations/great_expectations/tags>`_.
   **Note**: We do not use the `:latest` tag, so you will need to specify an exact tag.

#. Pull the Docker image down, e.g.:

    .. code-block:: bash

        docker pull greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0

#. Next, we assume you have a Great Expectations project deployed at ``/full/path/to/your/project/great_expectations``. You need to mount the local ``great_expectations`` directory into the container at ``/usr/app/great_expectations``, and from there you can run all non-interactice commands, such as running checkpoints and listing items:

    .. code-block:: bash

        docker run \
        -v /full/path/to/your/project/great_expectations:/usr/app/great_expectations \
        greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0 \
        datasource list


Additional notes
----------------

If you need to run interactive ``great_expectations`` commands, you can simply add the `-it` flags for interactive mode.

    .. code-block:: bash

        docker run -it \
        -v /full/path/to/your/project/great_expectations:/usr/app/great_expectations \
        greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0



Comments
--------

.. discourse::
   :topic_identifier: 317
