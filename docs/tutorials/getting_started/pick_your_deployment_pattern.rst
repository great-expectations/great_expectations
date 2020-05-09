.. _getting_started__customize_your_deployment:

Customize your deployment
=========================

At this point, you have your first, working deployment of Great Expectations. It's pretty basic: most of the storage and execution are handled locally.

The next step is to customize your deployment by upgrading specific components of your deployment. :ref:`Data Contexts` make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps---so you can upgrade from a basic demo deployment to a full production deployment at your own pace and be confident that your Data Context will continue to work at every step along the way.

This last section of the :ref:`getting_started` tutorial is designed to present you with clear options for upgrading your deployment. For specific implementation steps, please check out the linked :ref:`how_to_guides`.

Components
--------------------------------------------------

Here's an overview of the components of a typical Great Expectations deployment:

* Great Expectations configs and metadata 

    * Options for storing Great Expectations configuration
    * Options for storing Expectations
    * Options for storing ValidationResults

* Integrations to related systems

    * Additional DataSources and Generators
    * Options for hosting data documentation
    * Additional ValidationActions

* Key workflows

    * Creating and editing Expectations
    * Triggering validation

Levels of maturity
------------------

The Great Expectations ecosystem is expanding rapidly, which means that there's always a leading edge of integrations and workflows that aren't yet fully mature. We're committed to (1) shipping early, so that the community can benefit from new tools as soon as possible, and (2) clearly communicating levels of maturity, so that you can make good decisions about which components to use.

.. raw:: html

   <embed>
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css">
   </embed>

   <embed>
      <p>Components at the <span class="fas fa-circle" style="color:red"></span> experimental or <span class="fas fa-circle" style="color:yellow"></span> beta stage are marked with one of these icons: <span class="fas fa-circle" style="color:yellow"></span> <span class="fas fa-circle" style="color:red"></span>.
      
      Please see `Feature maturity grid`_ and `Levels of maturity`_ for more details.
      </p>
   </embed>


One of the awesome things about open source is that anyone can help improve the ecosystem. If you really want something built, please join the Great Expectations community and help us build it!


Options for storing Great Expectations configuration
----------------------------------------------------

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


Options for hosting Auto Docs
-----------------------------

By default, Auto Docs are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. A better pattern is usually to deploy to a cloud-based blob store (S3, GCS, or Azure blob store), configured to share a static website.

* How to store Validation Results to S3
* How to store Validation Results to GCS
* How to store Validation Results to Azure blob store
* How to store Validation Results to postgresql


Additional Validation Actions
-----------------------------

* How to store Validation Results as a Validation Action
* How to re-render Auto docs as a Validation Action
* How to trigger slack notifications as a Validation Action



Creating and editing Expectations
---------------------------------


Triggering validation
---------------------

As we saw in the previous step of the tutorial, the basic Great Expectations deployment allows you to trigger validation from a notebook. This is great for getting started, but not usually the approach you

There are two primary paths 

* How to validate data using an Airflow BashOperator