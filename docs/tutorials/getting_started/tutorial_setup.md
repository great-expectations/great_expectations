---  
title: 'Tutorial, Step 1: Setup'  
---  
<table class="borderless center">
<tr>
    <td>
        <img
          src={require('../../images/universal_map/Gear-active.png').default}
          alt="Setup"
        />
    </td>
    <td>
        <img
          src={require('../../images/universal_map/Arrow.png').default}
          alt="Arrow"
        />
    </td>
    <td>
        <img
          src={require('../../images/universal_map/Outlet-inactive.png').default}
          alt="Connect to Data"
        />
    </td>
    <td>
        <img
          src={require('../../images/universal_map/Arrow.png').default}
          alt="Arrow"
        />
    </td>
    <td>
        <img
          src={require('../../images/universal_map/Flask-inactive.png').default}
          alt="Create Expectations"
        />
    </td>
    <td>
        <img
          src={require('../../images/universal_map/Arrow.png').default}
          alt="Arrow"
        />
    </td>
    <td>
        <img
          src={require('../../images/universal_map/Checkmark-inactive.png').default}
          alt="Validate Data"
        />
    </td>
</tr>
</table>

:::note Prerequisites
In order to work with Great Expectations, you will need:

- A working Python install
- The ability to pip install for Python
  - Note: A best practice would be to do this in a virtual environment!
- A working Git install
- A working internet browser install (for viewing Data Docs in steps 3 and 4).

If you need assistance with setting up any of these utilities, we have links to their documentation on our page for [supporting resources.](../../reference/supporting_resources.md)
:::

### Setting up the tutorial data
The first thing we'll need is a copy of the data that this tutorial will work with.  Fortunately, we've already put that data into a convenient repository that you can clone to your machine.

Clone the [ge_tutorials](https://github.com/superconductive/ge_tutorials) repository to download the data.  This repository also contains directories with the final versions of the tutorial, which you can use for reference.

To clone the repository and go into the directory you'll be working from, start from your working directory and enter the following commands into your terminal:

```console
git clone https://github.com/superconductive/ge_tutorials
cd ge_tutorials
```

The repository you cloned contains several directories with final versions for this and our other tutorials. The final version for this tutorial is located in the `getting_started_tutorial_final_v3_api` folder. You can use the final version as a reference or to explore a complete deployment of Great Expectations, but **you do not need it for this tutorial**.

### Install Great Expectations and dependencies

Great Expectations requires Python 3 and can be installed using pip. If you haven’t already, install Great Expectations by running:

```bash
pip install great_expectations
```

You can confirm that installation worked by running

```bash
great_expectations --version
```

This should return something like:

```console
great_expectations, version 0.13.46
```

For detailed installation instructions, see [How to install Great Expectations locally](/docs/guides/setup/installation/local).

<details>
  <summary>Other deployment patterns</summary>
  <div>
    <p>

This tutorial deploys Great Expectations locally. Note that other options (e.g. running Great Expectations on an EMR Cluster) are also available. You can find more information in the [Reference Architectures](../../deployment_patterns/index.md) section of the documentation.

</p>
  </div>
</details>

### Create a Data Context

In Great Expectations, your Data Context manages your project configuration, so let’s go and create a Data Context for our tutorial project!

When you installed Great Expectations, you also installed the Great Expectations command line interface (CLI). It provides helpful utilities for deploying and configuring Data Contexts, plus a few other convenience methods.

To initialize your Great Expectations deployment for the project, run this command in the terminal from the `ge_tutorials` directory:

```console
great_expectations init
```

You should see this:
```console
Using v3 (Batch Request) API

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

OK to proceed? [Y/n]: <press Enter>
```

When you see the prompt, press enter to continue.  Great Expectations will build out the directory structure and configuration files it needs for you to proceed.  All of these together are your Data Context.

:::note

Your Data Context will contain the entirety of your Great Expectations project.  It is also the entry point for accessing all of the primary methods for creating elements of your project, configuring those elements, and working with the metadata for your project.  That is why the first thing you do when working with Great Expectations is to initialize a Data Context!

[You can follow this link to read more about Data Contexts.](../../reference/data_context.md)

:::

<details>
  <summary>About the <code>great_expectations</code> directory structure</summary>
  <div>
    <p>
      After running the <code>init</code> command, your <code>great_expectations</code> directory will contain all of the important components of a local Great Expectations deployment. This is what the directory structure looks like
    </p>
    <ul>
      <li><code>great_expectations.yml</code> contains the main configuration of your deployment.</li>
      <li>The <code>expectations/</code> directory stores all your Expectations as JSON files. If you want to store them somewhere else, you can change that later.</li>
      <li>The <code>plugins/</code> directory holds code for any custom plugins you develop as part of your deployment.</li>
      <li>The <code>uncommitted/</code> directory contains files that shouldn’t live in version control. It has a .gitignore configured to exclude all its contents from version control. The main contents of the directory are:
        <ul>
          <li><code>uncommitted/config_variables.yml</code>, which holds sensitive information, such as database credentials and other secrets.</li>
          <li><code>uncommitted/data_docs</code>, which contains Data Docs generated from Expectations, Validation Results, and other metadata.</li>
          <li><code>uncommitted/validations</code>, which holds Validation Results generated by Great Expectations.</li>
        </ul>
      </li>
    </ul>
  </div>
</details>

Congratulations, that's all there is to Step 1: Setup with Great Expectations.  You've finished the first step!  Let's move on to [Step 2: Connect to Data](./tutorial_connect_to_data.md)