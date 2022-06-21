"""A script to generate a markdown codeblock that contains the console output of
the CLI command `great_expectations --version`.

    Usage:
        1. Run the script.
        2. The script generates the `tutorial_version_snippet.mdx` file.
        3. The tutorial uses an import statement to include the contents of the generated file as the example of
            what the user should expect to see when they run the `great_expectations --version` CLI command.
        4. That's it.  You were already done after step 1.

    NOTES:
        - The script doesn't require that you enter the paths for the file containing the deployment versio or the path
          where the snippet should be generated.  Instead, these are stored as global variables (so you can find them
          right at the top of the script if they ever need to be changed for some reason).
        - The script doesn't actually run the `great_expectations --version` command or use
          great_expectations.__version__ to generate the output snippet, as both of those would presumably include
          additional information about the current "dirty" state of the code that a user shouldn't be seeing when
          they run the command after a fresh install of the most recent release.
"""
# from great_expectations import __version__ as ge_version

# Using the contents of VERSION_PATH directly is preferable to using ge_version, as ge_version will include "dirty"
# designations that the user shouldn't have to worry about seeing on their fresh install.
VERSION_PATH = "../great_expectations/deployment_version"
SNIPPET_PATH = "../docs/tutorials/getting_started/tutorial_version_snippet.mdx"


def update_version_mdx():
    """Creates a `.mdx` file containing the expected output of the CLI command `great_expectations --version` in a
     markdown codeblock.

    If the .mdx file already exists, it is overwritten when the script runs.

    The version information is pulled directly from the file path set in the VERSION_PATH global variable, and the
    snippet is written to the path set in the SNIPPET_PATH global variable.
    """
    with open(VERSION_PATH) as version_file:
        version = version_file.read()
    with open(SNIPPET_PATH, "w") as snippet_file:
        lines = ("```\n", f"great_expectations, version {version}", "```")
        snippet_file.writelines(lines)


if __name__ == "__main__":
    update_version_mdx()
