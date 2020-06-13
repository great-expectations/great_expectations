.. _contributing_miscellaneous:

Miscellaneous
==============

Core team
------------------------

* `James Campbell <https://github.com/jcampbell>`__
* `Abe Gong <https://github.com/abegong>`__
* `Eugene Mandel <https://github.com/eugmandel>`__
* `Rob Lim <https://github.com/roblim>`__
* `Taylor Miller <https://github.com/Aylr>`__
* `Alex Shertinsky <https://github.com/alexsherstinsky>`__
* `Tal Gluck <https://github.com/talagluck>`__
* `Kyle Eaton <https://github.com/kyleaton>`__
* `Sam Bail <https://github.com/spbail>`__
* `William Shin <https://github.com/Shinnnyshinshin>`__
* `Ben Castleton <https://github.com/bhcastleton>`__


.. _contributing_cla:

Contributor license agreement (CLA)
---------------------------------------

*When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.*

Please make sure you have signed our Contributor License Agreement (either `Individual Contributor License Agreement v1.0 <https://docs.google.com/forms/d/e/1FAIpQLSdA-aWKQ15yBzp8wKcFPpuxIyGwohGU1Hx-6Pa4hfaEbbb3fg/viewform?usp=sf_link>`__ or `Software Grant and Corporate Contributor License Agreement ("Agreement") v1.0 <https://docs.google.com/forms/d/e/1FAIpQLSf3RZ_ZRWOdymT8OnTxRh5FeIadfANLWUrhaSHadg_E20zBAQ/viewform?usp=sf_link>`__).

We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.


Release checklist
-----------------------------------------

GE core team members use this checklist to ship releases.

1. If this is a major release (incrementing either the first or second version number) the manual acceptance testing must be completed.

  * This `private google doc <https://docs.google.com/document/d/16QJPSCawEkwuEjShZeHa01TlQm9nbUwS6GwmFewJ3EY>`_ outlines the procedure. (Note this will be made public eventually)

2. Merge all approved PRs into ``develop``.
3. Make a new branch from ``develop`` called something like ``release-prep-2020-06-01``.
4. In this branch, update the version number in the ``.travis.yml`` file (look in the deploy section). (This sed snippet is handy if you change the numbers ``sed -i '' 's/0\.9\.6/0\.9\.7/g' .travis.yml``)

5. Update the ``changelog.rst``: move all things under ``develop`` under a new heading with the new release number.

  * Verify that any changes to requirements are specifically identified in the changelog

6. Submit this as a PR against ``develop``
7. After successful checks, get it approved and merged.
8. Update your local branches and switch to main: ``git fetch --all; git checkout main; git pull``.
9. Merge the now-updated ``develop`` branch into ``main`` and trigger the release: ``git merge origin/develop; git push``
10. Wait for all the build to complete. It should include 4 test jobs and a deploy job, which handles the actual publishing of code to pypi. You can watch the progress of these builds on Travis.
11. Check `PyPI <https://pypi.org/project/great-expectations/#history>`__ for the new release
12. Create an annotated git tag:

  * Run ``git tag -a <<VERSION>> -m "<<VERSION>>"`` with the correct new version. (Note: this is now done automatically as part of the deploy job.)
  * Push the tag up by running ``git push origin <<VERSION>>`` with the correct new version. (Note: this might be done automatically as part of the deploy job.)
  * Merge ``main`` into ``develop`` so that the tagged commit becomes part of the history for ``develop``: ``git checkout develop; git pull; git merge main``
  * On develop, add a new "develop" section header to changelog.rst, and push the updated file with message "Update changelog for develop"

13. `Create the release on GitHub <https://github.com/great-expectations/great_expectations/releases>`__ with the version number. Copy the changelog notes into the release notes, and update any rst-specific links to use github issue numbers.

  * The deploy step will automatically create a draft for the release.
  * Generally, we use the name of the tag (Ex: "0.11.2") as the release title.
  
14. Notify kyle@superconductive.com about any community-contributed PRs that should be celebrated.
15. Socialize the release on GE slack by copying the changelog with an optional nice personal message (thank people if you can)
16. Review the automatically-generated PR for conda-forge (https://github.com/conda-forge/great-expectations-feedstock/pulls), updating requirements as necessary and verifying the build status.

Beta Release Notes

* To ship a beta release, follow the above checklist, but use the branch name ``v0.11.x`` as the equivalent of ``main`` and ``v0.11.x-develop`` as the equivalent of ``develop``
* Ship the release using beta version numbers when updating the ``.travis.yml`` and when creating the annotated tag (e.g. `0.11.0b0`)
