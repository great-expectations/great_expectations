---
title: How to write a how-to-guide
---

This guide shows how to create a new how-to guide in Great Expectations. By writing guides with consistent structure and styling, you can get your PRs approved faster and make the Great Expectations docs discoverable, useful, and maintainable.

## Steps

1. Copy the How-to guide template file to the appropriate subdirectory of docs/guides/how_to_guides/, and rename it.

2. Write a title and purpose paragraph.

3. Decide whether you’re writing a code-heavy or process-heavy guide, and adjust your formatting appropriately.

4. Fill out the Prerequisites info box (see How-to guide template file). The header of the info box says: “This how-to guide assumes you have already:”. Place each prerequisite under its own bullet and phrase it using the style in the template: “done something”.

5. Fill in the Steps section, making sure to include bash, yml, and code snippets as appropriate. These will typically be included in a separate file that is in the `great_expectations/test/integrations` folder and be referenced in the how-to-doc.  For additional details, please see the "Structure of a How-to-guide" section below.  
 
6. If needed, add content to Additional Notes and/or Additional Resources. These sections supplement the article with information that would be distracting to include in Steps. It’s fine for them to be empty.

7. Scan your article to make sure it follows the Style guide. If you’re not familiar with the Style Guide, that’s okay: your PR reviewer will also check for style and let you know if we find any issues.

8. Locally run integration tests for any code that was included as part of the guide.

9. Submit your PR!

# Additional Notes

## Purpose of a how-to guide
The purpose of a how-to guide is to replicate, **NOT** to teach or explain. Teaching and explaining Great Expectations concepts are covered in the **Core Concepts** reference section.

Assume that the user has a working deployment of Great Expectations, whether it is set up on the file system by runing `great_expectations init` or configured in-memory. 

Assume that the user is already familiar with core concepts in Great Expectations: Expectations, Data Contexts, Validation, Datasources, etc. etc. You don’t need to spend any time explaining these things.

If you’re integrating with another system, assume that the user is familiar with that system. ie. If you’re writing the "How to configure a Snowflake Datasource," don’t spend any words explaining Snowflake or any of its core concepts.

If there are important, non-obvious principles for how Great Expectations relates to other systems, you may include them in the guide. If they are short (1-2 sentences, max), they can go in the Steps section. Otherwise, please put them in Additional Notes.

Remember, the goal is to help users successfully replicate specific steps as simply as possible. Surprisingly often, it turns out to best to not include explanation at all, since it can distract from the main purpose of the guide. If you feel you must include it, shorter is better.


## Structure of a how-to guide

With rare exceptions, How-to guides follow this structure:

Title

Purpose

Steps

Additional Notes (optional)

Additional Resources (optional)

Title: “How to X”. See the Style guide for specific guidance on how to phrase and format titles.

Purpose paragraph: A single, short paragraph to state the purpose of the guide, and motivate it if necessary.

“This guide will help you publish an Data Docs site directly to S3. Publishing a site this way makes reviewing and acting on Validation Results easy in a team, and provides a central location to review Expectations.”

Sometimes motivation can be a simple statement of purpose:

“This guide will help you connect to a MongoDB Datasource.”

If the user has data in Mongo and wants to configure a Datasource, no additional justification is needed.

Steps: Steps describe the golden path steps for successful replication.

Most steps will include inline code, such as a bash command, or an example yml snippet or two.

Most steps will also require user input, such as a connection string that needs to be replace, or a step that allows for testing (such as running `test_yaml_config()). 

Snippets should be as short as possible, but no shorter. In general, you can think of the snippet like a diff: what needs to change to accomplish this step?

We currently require that any new how-to-guides be integration tested. This means that the script that demonstrates what is being shown in the how-to-guide will live in the `tests/integration` folder

and will run with our integration tests. For more details please refer to the section below. 

Steps should be linear. “Do A, then B, then C.” Avoid complex loops and/or branching. If loops or branching are needed, it is likely a sign that the scope of the guide is too big. In that case, consider options for splitting it into more than one how-to guide.

Additional notes: This section covers errata that would be distracting to include in Steps. It’s fine for it to be empty.

Additional resources: Additional resources, usually external (i.e. not within the Great Expectations documentation) and usually shown as a list. To avoid link rot, please use this section sparingly, and prefer links to stable, well-maintained resources.

All of these styles are modeled in the this .rst file.

Using tabs to differentiate guides for different APIs.

During the process of writing documentation for Great Expectations 0.13, there rose a need to differentiate between documentation for GE up to 0.12.x, and GE 0.13 and beyond.

The use of content-tabs allows for both documentation to co-exist in the same how-to-doc.

The following code snippet shows how two tabs (Great and Expectations) can be created with the associated title and content. For more information on content-tabs, please refer to the following link : https://docusaurus.io/docs/next/markdown-features/tabs

```markdown

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs
  defaultValue="Great"
  values={[
    {label: 'Great', value: 'great'},
    {label: 'Expectations', value: 'expectations'},
  ]}>
  <TabItem value="great">This is Great!</TabItem>
  <TabItem value="expectations">These are Expectations</TabItem>
</Tabs>;
```


