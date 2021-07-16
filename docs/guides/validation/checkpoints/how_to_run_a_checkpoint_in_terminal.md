---
title: How to run a Checkpoint in terminal
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you run a Checkpoint in a terminal.
This is useful if your pipeline environment or orchestration engine does not have shell access.

<Prerequisites>

- Created at least one [Checkpoint](./how-to-create-a-new-checkpoint)

</Prerequisites>

Steps
-----

1. Checkpoints can be run like applications from the command line by running:

```bash
great_expectations --v3-api checkpoint run my_checkpoint
Validation failed!
```

2. Next, observe the output which will tell you if all validations passed or failed.

Additional notes
----------------

This command will return posix status codes and print messages as follows:

    +-------------------------------+-----------------+-----------------------+
    | **Situation**                 | **Return code** | **Message**           |
    +-------------------------------+-----------------+-----------------------+
    | all validations passed        | 0               | Validation succeeded! |
    +-------------------------------+-----------------+-----------------------+
    | one or more validation failed | 1               | Validation failed!    |
    +-------------------------------+-----------------+-----------------------+
