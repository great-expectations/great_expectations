---
title: Contributing a Package
---
import Prerequisites from './components/prerequisites.jsx'

This guide demonstrates how to bundle your own custom Expectations, Metrics, and Profilers into an official Great Expectations contributor package.

<Prerequisites>

* Created an account on [PyPi](https://pypi.org/account/register/)

</Prerequisites>

## Steps

### 1. Install the `great_expectations_contrib` CLI Tool

To streamline the process of contributing a package to Great Expectations, we've developed a CLI tool to
abstract away some of the complexity and help you adhere to our codebases' best practices. Please 
utilize the tool during your development to ensure that your package meets all of the necessary requirements.

To install the tool, first ensure that you are in the root of the `great_expectations` codebase:
```bash
cd contrib/cli
```

Next, use pip to install the CLI tool:
```bash
pip install -e great_expectations_contrib
```

You can verify your installation by running the following and confirming that a help message appears in your terminal:
```bash
great_expectations_contrib
```

`great_expectations_contrib` is designed to fulfill three key tasks:
1. Initialize your package structure
2. Perform a series of checks to determine the validity of your package
3. Publish your package to PyPi for you and others to use

### 2. Initialize a project

Once the CLI tool is enabled, we need to intialize an empty package.

To do so, go ahead and run:
```bash
great_expectations_contrib init
```
This will prompt you to answer a number of questions, such as:
* The name of your package
* What your package is about
* Your GitHub and PyPi usernames

The answers to these questions will be leveraged when
publishing your package. Upon completing the required prompts, you'll receive a confirmation
message and be able to view your package in its initial state.

To access your configured package, run the following:
```bash
cd <PACKAGE_NAME>
tree
```

Your file structure should look something like this:
```bash
.
├── LICENSE
├── README.md
├── assets
├── package_info.yml
├── requirements.txt
├── setup.py
├── tests
│   ├── __init__.py
│   ├── expectations
│   │   └── __init__.py
│   ├── metrics
│   │   └── __init__.py
│   └── profilers
│       └── __init__.py
└── <YOUR_PACKAGE_SOURCE_CODE>
    ├── __init__.py
    ├── expectations
    │   └── __init__.py
    ├── metrics
    │   └── __init__.py
    └── profilers
        └── __init__.py
```

To ensure consistency with other packages and the rest of the Great Expectations ecosystem,
please maintain this general structure during your development.

### 3. Contribute to your package

Now that your package has been initialized, it's time to get coding!

You'll want to capture any dependencies in your `requirements.txt`, validate your code
in `tests`, detail your package's capabilities in `README.md`, and update any relevant 
publishing details in `setup.py`. 

If you'd like to update your package's metadata or assign code owners/domain experts,
please follow the instructions in `package_info.yml`.

As you iterate on your work, you can check your progress using:

```
great_expectations_contrib check
```

This command will run a series of checks on your package, including:
* Whether or not your code is linted/formatted properly
* Whether or not you've type annotated function sigantures
* Whether or not your Expectations are properly documented
* And more!

Using `great_expectations_contrib` as part of your development loop will help you
keep on track and provide you with a checklist of necessary items to get your package
across the finish line!

### 4. Publish your package

Once you've written your package, tested its behavior, and documented its capabilities,
the final step is to get your work published.

The CLI tool wraps around `twine` and `wheel`, allowing you to run:
```
great_expectations_contrib publish
```

As long as you've passed the necessary checks, you'll be prompted to provide your
PyPi username and password, and your package will be published!

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just published your first Great Expectations contributor package! &#127881;
</b></p>
</div>

### 5. Contribution (Optional)

Your package can also be submitted as a contribution to the Great Expectations codebase, under the same [Maturity Level](./contributing_maturity.md#contributing-expectations) requirements as [Custom Expectations](../guides/expectations/creating_custom_expectations/overview.md).
