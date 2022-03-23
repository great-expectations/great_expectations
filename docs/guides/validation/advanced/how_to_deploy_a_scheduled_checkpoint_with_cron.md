---
title: How to deploy a scheduled Checkpoint with cron
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you deploy a scheduled Checkpoint with cron.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- You have created a Checkpoint.

</Prerequisites>

Steps
-----

1. First, verify that your Checkpoint is runnable via shell:

    ```bash
    great_expectations checkpoint run my_checkpoint
    ```

2. Next, to prepare for editing the cron file, you'll need the full path of the project's ``great_expectations`` directory.

3. Next, get full path to the ``great_expectations`` executable by running:

    ```bash
    which great_expectations
    /full/path/to/your/environment/bin/great_expectations
    ```

4. Next, open the cron schedule in a text editor. On most operating systems, ``crontab -e`` will open your cron file in an editor.

5. To run the Checkpoint ``my_checkpoint`` every morning at 0300, add the following line in the text editor that opens:

    ```bash
    0  3  *  *  *    /full/path/to/your/environment/bin/great_expectations checkpoint run ratings --directory /full/path/to/my_project/great_expectations/
    ```

6. Finally save the text file and exit the text editor.

Additional notes
----------------

The five fields correspond to the minute, hour, day of the month, month and day of the week.

It is critical that you have full paths to both the ``great_expectations`` executable in your project's environment and the full path to the project's ``great_expectations/`` directory.

If you have not used cron before, we suggest searching for one of the many excellent cron references on the web.
