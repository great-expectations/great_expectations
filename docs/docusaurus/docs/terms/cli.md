---
title: CLI (Command Line Interface)
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='active' connect='active' create='active' validate='active'/> 

## Overview

### Definition

CLI stands for Command Line Interface.

### Features and promises

The CLI provides useful convenience functions covering all the steps of working with Great Expectations.  CLI commands consist of a noun indicating what you want to operate on, and a verb indicating the operation to perform.  All CLI commands have help documentation that can be accessed by including the `--help` option after the command.  Running `great_expectations` without any additional arguments or `great_expectations --help` will display a list of the available commands.

### Relationship to other objects

The CLI provides commands for performing operations on your Great Expectations deployment, as well as on <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" />, <TechnicalTag relative="../" tag="datasource" text="Datasources" />, <TechnicalTag relative="../" tag="data_docs" text="Data Docs" />, <TechnicalTag relative="../" tag="store" text="Stores" />, and <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />.  

You will usually also initialize your <TechnicalTag relative="../" tag="data_context" text="Data Context" /> through the CLI.

Most CLI commands will either execute entirely in the terminal, or will open Jupyter Notebooks with boilerplate code and additional commentary to help you accomplish a task that requires more complicated configuration.

## Use cases

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

The CLI provides functionality at every stage in your use of Great Expectations.  What commands you'll want to execute, however, will differ from step to step.

<SetupHeader/>

Every Great Expectations project starts with initializing your Data Context, which is typically done with the command:
```bash title="Terminal command"
great_expectations init
```

You can also utilize the project commands to check config files for validity and help with migrations when updating versions of Great Expectations.  You can read about these commands in the CLI with the command:

```bash title="Terminal command"
great_expectations project --help
```

<ConnectHeader/>

To assist with connecting to data, the CLI provides commands for creating, listing, and deleting Datasources.  You can read about these commands in the CLI with the command:

```bash title="Terminal command"
great_expectations datasource --help
```

<CreateHeader/>

To assist you in creating Expectation Suites, the CLI provides commands for listing available suites, creating new empty suites, creating new suites with scaffolding, editing existing suites, and deleting expectation suites.  You can read about these commands in the CLI with the command:

```bash title="Terminal command"
great_expectations suite --help
```

<ValidateHeader/>

To assist you in Validating your data, the CLI provides commands for listing existing Checkpoints, running an existing Checkpoint, creating new Checkpoints, and creating Python scripts that will run a Checkpoint.  You can read about these commands in the CLI with the command:

```bash title="Terminal command"
great_expectations checkpoint --help
```

There are also commands available through the CLI for building, deleting, and listing your available Data Docs.  You can read about these commands in the CLI with the command:

```bash title="Terminal command"
great_expectations docs --help
```

## Features

### Convenience commands

The CLI provides commands that will list, create, delete, or edit almost anything you may want to list, create, delete, or edit in Great Expectations.  If the CLI does not perform the operation directly in the terminal, it will provide you with a Jupyter Notebook that has the necessary code boilerplate and contextual notes to get you started on the process.

## API basics

For an in-depth guide on using the CLI, see [our document on how to use the Great Expectations CLI](../guides/miscellaneous/how_to_use_the_great_expectations_cli.md) or read the CLI documentation directly using the following command:

```bash title="Terminal command"
great_expectations --help
```

