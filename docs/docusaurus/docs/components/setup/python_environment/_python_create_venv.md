As a best practice, we recommend using a virtual environment to partition your GX installation from any other Python projects that may exist on the same system.  This ensures that there will not be dependency conflicts between the GX installation and other Python projects.

Once we have confirmed that Python 3 is installed locally, we can create a virtual environment with `venv`.

:::tip Why use `venv`?
We have chosen to use `venv` for virtual environments in this guide because it is included with Python 3. You are not limited to using `venv`, and can just as easily install Great Expectations into virtual environments with tools such as `virtualenv`, `pyenv`, etc.
:::

We will create our virtual environment by running:

```console title="Terminal command"
python -m venv my_venv
```

This command will create a new directory called `my_venv`.  Our virtual environment will be located in this directory.

In order to activate the virtual environment we will run:

```console title="Terminal command"
source my_venv/bin/activate
```

:::tip Why name it `my_venv`?
You can name your virtual environment anything you like.  Simply replace `my_venv` in the examples above with the name that you would like to use.
:::