.. _how_to_guides__miscellaneous__how_to_use_official_docker_images:

How to use official docker images
=================================

This guide will help you use the official Great Expectations docker images.
This is useful if you wish to have a fully portable Great Expectations runtime that can be used locally or deployed on the clould.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`
  - You have docker installed on your machine.

Steps
-----

#. First, choose which image you'd like to use by browsing the offical `Great Expectations docker image registry <https://hub.docker.com/r/greatexpectations/great_expectations/tags>`_.
   Note we do not use the `:latest` tag so you will need to specify an exact tag.

#. Pull the docker image down.

    .. code-block:: bash

        docker pull greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0

#. Next, from your project directory (the one with the ``great_expectations`` folder in it, run a simple command like ``datasource list``
   Note you must mount the local ``great_expectations`` directory into the container at ``/usr/app/great_expectations``.

    .. code-block:: bash

        docker run \
        -v full/path/to/your/project/great_expectations:/usr/app/great_expectations \
        greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0 \
        datasource list


#. From here you can run all non-interactive commands such as running checkpoints, and listing items.


Additional notes
----------------

If you need to run interactive great_expectations commands they are best done from inside the container by running docker in interactive mode and changing the entrypoint as follows:

    
    .. code-block:: bash

        docker run -it \
        --entrypoint /bin/bash \
        -v full/path/to/your/project/great_expectations:/usr/app/great_expectations \
        greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0


You'll now be at a bash shell in the docker container where you can run any great_expectations command and provide terminal input

    .. code-block:: bash

        $ docker run -it --entrypoint /bin/bash -v full/path/to/your/project/great_expectations:/usr/app/great_expectations greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0
        root@02d6f438181f:/usr/app/great_expectations# great_expectations suite new
        ...

Comments
--------

.. discourse::
   :topic_identifier: 317
