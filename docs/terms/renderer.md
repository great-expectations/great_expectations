---
id: renderer
title: Renderer
hoverText: A class for converting Expectations, Validation Results, etc. into Data Docs or other output such as email notifications or slack messages.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

A Renderer is a class for converting Expectations, Validation Results, etc. into Data Docs or other output such as email notifications or slack messages.


NOTES: Everything under here is temporary, for use in writing the detailed page.
----------
Used to convert Expectations and Validation Results into Data Docs (and other rendered output such as email notifications, slack messages, etc.). Each Expectation has its own renderer(s) that allows that expectation to be expressed in data docs.  From an architectural/contributor standpoint, Expectations are rendered into an intermediate canonical format and then from there turned into docs and other output (think of it more of a rendering pipeline)