import ReleaseVersionBox from '../../../components/versions/_gx_version_code_box.mdx'
import GxData from '../../_core_components/_data.jsx'

GX Core is a Python library and as such can be used with a local Python installation to access the functionality of GX through Python scripts.

### Installation and setup

1. Optional. Activate your virtual environment.

   If you created a virtual environment for your GX Python installation, browse to the folder that contains your virtual environment and run the following command to activate it:

   ```bash title="Terminal input"
   source my_venv/bin/activate
   ```

2. Ensure you have the latest version of `pip`:

   ```bash title="Terminal input"
   python -m ensurepip --upgrade
   ```

3. Install the GX Core library:

   ```bash title="Terminal input"
   pip install great_expectations
   ```

4. Verify that GX installed successfully.

   In Python, run the following code:

   ```python title="Python" name="docs/docusaurus/docs/core/set_up_a_gx_environment/_install_gx/_local_installation_verification.py - full code example"
   ```
   
   If GX was installed correctly, the version number of the installed GX library will be printed.