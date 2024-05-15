GX Cloud provides a web interface for using GX to validate your data without creating and running complex Python code. However, GX 1.0 can connect to a GX Cloud account if you want to customize or automate your workflows through Python scripts.

## Installation and setup

To deploy a GX Agent, which serves as an intermediary between GX Cloud's interface and your organization's data stores, see [Connect GX Cloud](/cloud/connect/connect_lp.md). The GX Agent serves all GX Cloud users within your organization.  If a GX Agent has already been deployed for your organization, you can use the GX Cloud online application without further installation or setup.

To connect to GX Cloud from a Python script utilizing a [local installation of GX 1.0](/core/installation_and_setup/install_gx.md?install-location=local) instead of the GX Agent, see [Connect to an existing Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=gx_cloud#connect-to-an-existing-data-context).