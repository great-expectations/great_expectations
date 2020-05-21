.. _getting_started__customize_your_deployment:

Customize your deployment
=========================

At this point, you have your first, working deployment of Great Expectations. You've also been introduced to the foundational concepts in the library: :ref:`Data Contexts`, :ref:`Datasources`, :ref:`Expectations`, :ref:`Profilers`, :ref:`Data Docs`, :ref:`Validation`, and :ref:`Checkpoints`.

Congratulations! You're off to a very good start.

The next step is to customize your deployment by upgrading specific components of your deployment. :ref:`Data Contexts` make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps---so you can upgrade from a basic demo deployment to a full production deployment at your own pace and be confident that your Data Context will continue to work at every step along the way.

This last section of the :ref:`getting_started` tutorial is designed to present you with clear options for upgrading your deployment. For specific implementation steps, please check out the linked :ref:`how_to_guides`.

Components
--------------------------------------------------

Here's an overview of the components of a typical Great Expectations deployment:

* Great Expectations configs and metadata 

    * :ref:`Options for storing Great Expectations configuration`
    * :ref:`Options for storing Expectations`
    * :ref:`Options for storing Validation Results`

* Integrations to related systems

    * :ref:`Additional Datasources and Generators`
    * :ref:`Options for hosting Data Docs`
    * :ref:`Additional Validation Actions`

* Key workflows

    * :ref:`Creating and editing Expectations`
    * :ref:`Triggering validation`


Options for storing Great Expectations configuration
----------------------------------------------------

#FIXME: Add links once how-to guides are set up.

* How to instantiate a DataContext without a yml file
* How to use environment variables to populate credentials
* How to populate credentials from a secrets store


Options for storing Expectations
--------------------------------

Many teams find it convenient to store Expectations in git. Essentially, this approach treats Expectations like test fixtures: they live adjacent to code and are stored within version control. git acts as a collaboration tool and source of record.

Alternatively, you can treat Expectations like configs, and store them in a blob store. Finally, you can store them in a database.

* How to store Expectations to S3
* How to store Expectations to GCS
* How to store Expectations to Azure blob store
* How to store Expectations to postgresql



Options for storing Validation Results
--------------------------------------
By default, Validation Results are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. The most common pattern is to use a cloud-based blob store such as S3, GCS, or Azure blob store. You can also store Validation Results in a database.

* How to store Validation Results to S3
* How to store Validation Results to GCS
* How to store Validation Results to Azure blob store
* How to store Validation Results to postgresql

Additional DataSources and Generators
-------------------------------------

Great Expectations plugs into a wide variety of Datasources, and the list is constantly getting longer. If you have an idea for a Datasource not listed here, please speak up at [Some location].

#FIXME: Finalize this list after tidying up our How-to Guides

* How to configure a AAAAA Datasource
* How to configure a AAAAA Datasource
* How to configure a AAAAA Datasource
* How to configure a AAAAA Datasource
* How to configure a AAAAA Datasource


Options for hosting Data Docs
-----------------------------

By default, Data Docs are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. A better pattern is usually to deploy to a cloud-based blob store (S3, GCS, or Azure blob store), configured to share a static website.

* How to store Validation Results to S3
* How to store Validation Results to GCS
* How to store Validation Results to Azure blob store
* How to store Validation Results to postgresql


Additional Validation Actions
-----------------------------

* How to store Validation Results as a Validation Action
* How to re-render Data docs as a Validation Action
* How to trigger slack notifications as a Validation Action



Creating and editing Expectations
---------------------------------


Triggering validation
---------------------

As we saw in the previous step of the tutorial, the basic Great Expectations deployment allows you to trigger validation from a notebook. This is great for getting started, but not usually the approach you

There are two primary paths 

* How to validate data using an Airflow BashOperator