import GxData from '../../_core_components/_data.jsx';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import RecommendedVirtualEnvironment from '../../_core_components/prerequisites/_recommended_virtual_environment.md';
import InfoUsingAVirtualEnvironment from '../../_core_components/admonitions/_if_you_are_using_a_virtual_environment.md';

To validate Google Cloud Platform (GCP) data with GX Core, you create your GX Python environment, configure your GCP credentials, and install GX Core locally with the additional dependencies to support GCP.

## Prerequisites

- A [GCP service account](https://cloud.google.com/iam/docs/service-account-overview) with permissions to access GCP resources and storage Objects.
- The `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set. See the Google documentation [Set up Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc). 
- Google Cloud API authentication is set up. See the Google documentation [Set up authentication](https://cloud.google.com/storage/docs/reference/libraries#authentication).
- <PrereqPythonInstalled/>
- <RecommendedVirtualEnvironment/>

## Installation

1. Ensure your GCP credentials are correctly configured. This process includes:

   - Creating a Google Cloud Platform (GCP) service account.
   - Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable,
   - Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.
   
   For more information, see the GCP documentation on how to verify [authentication for the Google Cloud API](https://cloud.google.com/docs/authentication/getting-started).

2. Install the Python dependencies for GCP support. 

   Run the following terminal command to install GX Core with the additional dependencies for GCP support:

   :::info
   <InfoUsingAVirtualEnvironment/>
   :::

   ```bash title="Terminal input"
   python -m pip install 'great_expectations[gcp]'
   ```
