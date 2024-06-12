# Migration Guide to 1.0

## Overview of changes

Releasing GX 1.0 is a big milestone for us, and we have consolidated a lot of learning to make this release possible. We focusd on four key areas:

1. GX should be easier to use, with configuration that is much more straightforward. 
- There should be one way to do things, and it should be easy to understand.
- Our Public API should be clear and consistent.
2. Documentation should be less complex and have more substantive guidance on how to use GX.
3. Contributors should find it simpler to understand and implement their ideas.
4. New releases should be able to focus on adding functionality not fixing bugs. 

To make that possible we needed to make quite a few breaking changes. This migration guide covers the most important changes and how to adapt your code to the new version.

### Better Code Organization: Updated Imports

Imports are now organized in a more logical way. This means that you will need to update your imports to reflect the new organization. 

The most important changes is that Expectations should now be imported and created directly as classes, instead of by calling methods on the validator. In fact, the Validator class is completely removed from the public API and plays a behind-the-scenes role only.

Instead of:

```python
context.get_validator()
```

### Clearer Public API

### Simplified Functional Interfaces

- no longer can store evaluatino parameters
- no longer can store batch parameters for chekcpoint

### Better IDE Integration

## Code Changes

get_context search behavior



### Parameter names updated for clarity

- evaluation parameters -> expectation parameters
- partition -> distribution
- splitter -> batch definition


## Configuration Changes

