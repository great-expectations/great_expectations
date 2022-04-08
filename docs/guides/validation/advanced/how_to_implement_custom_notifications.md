---
title: How to implement custom notifications
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

If you would like to implement custom notifications that include a link to Data Docs, you can access the Data Docs URL for the respective Validation Results page from your Data Context after a validation run following the steps below. This will work to get the URLs for any type of Data Docs site setup, e.g. S3 or local setup.

<Prerequisites>

  - [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
  - [Created an Expectation Suite to use for validation](../../../tutorials/getting_started/create_your_first_expectations.md)

</Prerequisites>

1. First, this is the standard boilerplate to load a Data Context and run validation on a Batch:

    ```python
    import great_expectations as ge

    # Load the Data Context
    context = ge.data_context.DataContext()

    # The batch_kwargs will differ depending on the type of Datasource,
    # see the docs for help
    batch_kwargs = {'table': 'my_table', 'datasource': 'my_datasource'}
    batch = context.get_batch(batch_kwargs, 'my_suite')

    # Run validation on your Batch
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id='my_run_id')
    ```

2. Next, get the list of IDs of the Validation Results. ``results`` is a list that might have multiple elements if you validate multiple Batches, so we'll have to iterate over it. In this example, we'll just grab the only element:

    ```python
    validation_ids = [res for res in results['run_results']]
    validation_id = validation_ids[0]
    ```

3. Finally, use the ``validation_id`` as an argument to the Data Context's ``get_docs_sites_urls()`` method, and get the right element from the resulting list to access its ``site_url``:

    ```python
    url_dicts = context.get_docs_sites_urls(resource_identifier=validation_id)
    # The method returns a list of dictionaries like so:
    # [{'site_name': 'local_site',
    #  'site_url': 'file://validations/my_suite/my_run_id/20200811T181225.859901Z/123456.html'}]

    # The list will have multiple elements if you have multiple sites set up, e.g.
    # S3 and a local site. In this example, we'll just grab the only element again:
    validation_site_url = url_dicts[0]['site_url']
    ```

4. You can now include ``validation_site_url`` as a link in your custom notifications, e.g. in an email,  which will allow users to jump straight to the relevant Validation Results page.

