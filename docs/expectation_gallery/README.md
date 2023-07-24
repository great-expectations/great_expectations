README
======

Before ANY pull request introducing a new Expectation is accepted, be sure to checkout the contributor's branch locally (using the [GitHub CLI tool](https://cli.github.com) with `gh pr checkout {PR-NUMBER}`) and run the `build_gallery.py` script against that new Expectation!

```
python ./build_gallery.py --backends "pandas, spark, sqlite, postgresql" expect_whatever_new_snake_case_name
```
