---
title: TEMPLATE How to guide {stub}
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import NextSteps from '../connecting_to_your_data/components/next_steps.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you {do something.} {That something is important or useful, because of some reason.}

<Prerequisites>

- Additional guide-specific prerequisites go here.

</Prerequisites>

:::warning Do not introduce additional heading hierarchy or headers.
- Guides must have a single h2 `## Steps` heading with numbered h3 `### 1. xxx` headings below it. This ensures consistency and supports a built in TOC seen on the upper right of all guides.
- Guides may have an optional `## Additional Notes` and `## Next Steps` headings as shown at the end of this template.
:::

## Steps

:::tip What qualifies as a step?
To qualify as a step, the things within it must either:
- require user input (change a name, add credentials, etc)
- or require user to run something and view output
:::

### 1. First do this

Run this code to {do a thing}.

```python file=../../../tests/integration/docusaurus/template/script_example.py#L1
```

### 2. Next do this.

{Concise description of what the user is going to do}.

```python file=../../../tests/integration/docusaurus/template/script_example.py#L7
```

### 3. Finally, do this.

:::tip Use tabs to represent choices.
When using Great Expectations there are sometimes choices to be made that do not warrant a separate how to guide. Use tabs to represent these.
:::

Next, do {concise description of what the user is going to do} using either {choice in tab 1} or {choice in tab 2}.

<Tabs
  groupId="tab1-or-tab2"
  defaultValue='tab1'
  values={[
  {label: 'Using the CLI', value:'cli'},
  {label: 'Using Python', value:'python'},
  ]}>
  <TabItem value="cli">

  Run this command in the CLI.

```console
great_expectations suite new
```

  </TabItem>
<TabItem value="python">

Run this code in Python.

```python file=../../../tests/integration/docusaurus/template/script_example.py#L1-L3
```

</TabItem>
</Tabs>

Congratulations!
You successfully {did the thing this guide is about}.

## Additional Notes

:::tip Additional Notes are optional and may contain:
- links to any scripts used in this guide to facilitate user feedback via GitHub
- additional context (use extremely sparingly)
:::

To view the full scripts used in this page, see them on GitHub:

- [postgres_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/database/postgres_yaml_example.py)
- [postgres_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/database/postgres_python_example.py)

## Next Steps

:::tip Next steps are optional
Include them only if it makes sense.
:::

Now that you've {done a thing}, you'll want to work on these {other things}:

- [How to xxx](#)
- [How to yyy](#)
