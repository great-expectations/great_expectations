---
title: How to deploy a scheduled Checkpoint with cron
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you deploy a scheduled <TechnicalTag tag="checkpoint" text="Checkpoint" /> with cron.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- You have created a Checkpoint.

</Prerequisites>

## Steps

### 1. Verify Checkpoint suitability

First, verify that your Checkpoint is runnable via shell:

```bash
great_expectations checkpoint run my_checkpoint
```

### 2. Get `great_expectations` full path

To prepare for editing the cron file, you'll need the full path of the project's ``great_expectations`` directory.  You can get full path to the ``great_expectations`` executable by running:

```bash
which great_expectations
/full/path/to/your/environment/bin/great_expectations
```

### 3. Open your cron schedule

A text editor can be used to open the cron schedule. On most operating systems, ``crontab -e`` will open your cron file in an editor.

### 4. Add your Checkpoint to the cron schedule

To run the Checkpoint ``my_checkpoint`` every morning at 0300, add the following line in the text editor that opens:

```bash
0  3  *  *  *    /full/path/to/your/environment/bin/great_expectations checkpoint run ratings --directory /full/path/to/my_project/great_expectations/
```

:::note
- The five fields at the start of your cron schedule correspond to the minute, hour, day of the month, month, and day of the week.
- It is critical that you use full paths to both the ``great_expectations`` executable in your project's environment and the full path to the project's ``great_expectations/`` directory.
:::

### 5. Save your changes to the cron schedule

Once you have added the line that runs your Checkpoint at the desired time, save the text file of the cron schedule and exit the text editor.

## Additional notes

If you have not used cron before, we suggest searching for one of the many excellent cron references on the web.
