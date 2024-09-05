import GxData from '../../_core_components/_data.jsx';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import RecommendedVirtualEnvironment from '../../_core_components/prerequisites/_recommended_virtual_environment.md';
import InfoUsingAVirtualEnvironment from '../../_core_components/admonitions/_if_you_are_using_a_virtual_environment.md';

To validate data while using Spark to read from dataframes or file formats such as `.csv` and `.parquet` with GX Core, you create your GX Python environment, install GX Core locally, and then configure the necessary dependencies.

## Prerequisites

- <PrereqPythonInstalled/>
- <RecommendedVirtualEnvironment/>

## Installation

1. Optional. Activate your virtual environment.

   <InfoUsingAVirtualEnvironment/>

2. Run the pip command to install the dependencies for Spark:

   ```bash title="Terminal input"
   python -m pip install 'great_expectations[spark]'
   ```
