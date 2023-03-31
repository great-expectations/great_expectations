import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

We can create a new <TechnicalTag relative="../../../" tag="data_context" text="Data Context" /> without opening a Python interpreter by using Great Expectations' <TechnicalTag tag="cli" text="CLI" />.

From the directory where you want to deploy Great Expectations run the following command:

```bash title="Terminal command"
great_expectations init
```

You should be presented with this output and prompt:

```console title="Terminal output"
  ___              _     ___                  _        _   _
 / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
| (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
 \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                |_|
             ~ Always know what to expect from your data ~

Let's create a new Data Context to hold your project configuration.

Great Expectations will create a new directory with the following structure:

    great_expectations
    |-- great_expectations.yml
    |-- expectations
    |-- checkpoints
    |-- plugins
    |-- .gitignore
    |-- uncommitted
        |-- config_variables.yml
        |-- data_docs
        |-- validations

OK to proceed? [Y/n]:
```

When you see the prompt to proceed, enter `Y` or simply press the `enter` key to continue.  Great Expectations will then build out the directory structure and configuration files it needs for you to proceed.