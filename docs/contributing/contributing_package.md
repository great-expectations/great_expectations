---
title: Contributing a Package
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'

This guide demonstrates how to bundle your own custom Expectations, Metrics, and Profilers into an official Great Expectations contributor package.

<Prerequisites>

* Clone the official `great_expectations` repo on your local machine.

</Prerequisites>

## Steps

### 1. Install the `great_expectations_contrib` CLI Tool

To streamline the process of contributing a package to Great Expectations, we've developed a CLI tool to
abstract away some of the complexity and help you adhere to our codebases' best practices.

To install the tool, use the following commands:
```bash
# Ensure you are in the root of the `great_expectations/` codebase
cd contrib/cli
pip install -e great_expectations_contrib
```

You can verify your installation by running the folowing and confirming that a help message appears in your terminal:
```bash
great_expectations_contrib
```

`great_expectations_contrib` is designed to fulfill three key tasks:
* Initialize your package structure
* Perform a series of checks to determine the validity of your package
* Publish your package to PyPi for you and others to use

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

Please answer these questions to the best of your ability as they will be leveraged when
publishing your package. Upon completing the required prompts, you'll receive a confirmation
message and be able to view your package.

```bash
cd <PACKAGE_NAME>
tree
```

Your file structure should look something like this:
```bash
.
├── LICENSE
├── README.md
├── <MY_PACKAGE>
│   ├── __init__.py
│   ├── expectations
│   │   └── __init__.py
│   ├── metrics
│   │   └── __init__.py
│   └── profilers
│       └── __init__.py
├── requirements.txt
├── setup.py
└── tests
    ├── __init__.py
    ├── expectations
    │   └── __init__.py
    ├── metrics
    │   └── __init__.py
    └── profilers
        └── __init__.py
```

### 3. Contribute to your package

Add relevant text about writing new Expectations, Metrics, and Profilers!

```
great_expectations_contrib check
```

### 4. Publish your package

```
great_expectations_contrib publish
```
