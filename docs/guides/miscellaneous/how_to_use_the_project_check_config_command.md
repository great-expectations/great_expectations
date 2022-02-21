---
title: How to use the project check-config command
---

To facilitate this substantial config format change, starting with version 0.8.0
we introduced `project check-config` to sanity check your config files. From your
project directory, run:

```bash
great_expectations project check-config
```

This can be used at any time and will grow more robust and helpful as our
internal config typing system improves.

You will most likely be prompted to install a new template. Rest assured that
your original YAML file will be archived automatically for you. Even so, it's
in your source control system already, right? ;-)
