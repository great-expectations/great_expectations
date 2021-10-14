# Migrating to V3 API

While we are committed to keeping Great Expectations as stable as possible, sometimes breaking changes are necessary to maintain our trajectory. This is especially true as the library has evolved from just a data quality tool to a more capable framework including [data docs](data_docs) and [profilers](profilers) in addition to [validation](validation).

The Batch Request (V3)  API was first introduced in the 0.13 major release of Great Expectations and included a group of new features based on "new style" Datasources and Modular Expectations.  


The V2 (Batch Kwargs) API will be deprecated in the future.



**[Add More information about V3, and difference between 0.13 and 0.14]



Since expectation semantics are usually consistent across versions, there is little change required when upgrading Great Expectations, with some exceptions noted here.

* What do write about V3 API? 
    - it was first introduced in 0.13
    - after 0.14 this is the default behavior that we are encourag

## Main differences

**V3 (Batch Request) API vs The V2 (Batch Kwargs) API**


Main Differences
    - new Style DataSources
    - modular Expectations : 
        - is there more information that can be shared here? 


## Do I need to upgrade?

- To determine if you need to upgrade, run the following script in the CLI


```bash
great_expectations check-config
```

- if you need to upgrade, you will see the following message:
1. run the cli `check-config` command (alert user if they have v2 config e.g. batch_kwargs_generators)

- if you dont, the following message will be displayed


## If you need to upgrade to V3 API

Tell them to manually update AND run upgrader, and tell them what upgrader will remove (validation_operators in addition to what it already does).

run the cli upgrade command

(does upgrade of items that are deterministic, shows Success after you don't have v2 config any longer - so if done before manual upgrade will show that you need to do manual upgrade)

We recommend you do this before the manual part, and check the status of the upgrade

Here is a guide to manually upgrade your datasources (batch_kwargs_generators → data_connectors, etc)

run check-config (recommended) or upgrade and get Success :white_check_mark: if you successfully upgraded, if not provide feedback.

**** MODIFY THIS ****

