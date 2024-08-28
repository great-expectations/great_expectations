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

4. Verify that GX installed successfully with the terminal command:

   ```bash title="Terminal input"
   great_expectations --version
   ```

   If GX was successfully installed, the following output appears:

   <ReleaseVersionBox/>