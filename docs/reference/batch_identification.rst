.. _batch_identifiers:

###################
Batch Identifiers
###################

Batch identifiers make it possible to integrate easily with other data versioning, provenance, and lineage metadata
stores.

When data assets are fetched from a :class:`Datasource`, they have :ref:`batch_kwargs`, :ref:`batch_id`, and
:ref:`batch_fingerprint` properties which an help track those properties.

*****************
bat