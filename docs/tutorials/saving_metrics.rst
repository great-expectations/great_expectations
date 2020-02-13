.. _saving_metrics:

##############
Saving Metrics
##############

This tutorial covers getting and saving metrics using a Data Context.

First, note that to obtain a metric, we need the expectation configuration. This is available by default in an
ExpectationValidationResult, but if it is suppressed, then the it is not possible to obtain a metric.

.. invisible-code-block: python

    import great_expectations as ge
    import pandas as pd
    remember_me = b'see how namespaces work?'

.. code-block:: python

    df = pd.read_csv("/opt/data/titanic/Titanic.csv")
    df = ge.dataset.PandasDataset(df)

    #
    res = df.expect_column_values_to_be_in_set("Sex", ["male", "female"], include_config=False)
    print(res)

    {
      "exception_info": null,
      "expectation_config": null,
      "success": true,
      "result": {
        "element_count": 1313,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_list": []
      },
      "meta": {}
    }





res.get_metric("expect_column_values_to_be_in_set.result.missing_count", 'column=Sex')


# In[9]:


res = df.expect_column_values_to_be_in_set("Sex", ["male", "female"], include_config=True)


# In[10]:


res


# In[11]:


type(res)


# In[12]:


res.get_metric("expect_column_values_to_be_in_set.result.missing_count")


# In[13]:


res.get_metric("expect_column_values_to_be_in_set.success", None)


# In[14]:


res.get_metric("expect_column_values_to_be_in_set.result.missing_count", 'column=Sex')


# In[15]:


res.get_metric("expect_column_values_to_be_in_set.result.missing_count", 'column=Age')


# In[16]:


res.get_metric("expect_column_values_to_be_in_set.success", 'column=Sex')


# In[17]:


res.get_metric("expect_column_values_to_be_in_set.details.missing_count", 'column=Sex')


# In[19]:


res.get_metric("blarg", None)


# In[ ]:




