.. _contributing:

############
Contributing
############

Welcome to the Great Expectations project! We're very glad you want to help out by contributing.

Our goal is to make your experience as great as possible. Please follow these steps to contribute:

.. raw:: html

   <embed>
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css">
      <style>
         .custom-indented-paragraph {
            margin-left: 70px;
         }
      </style>
   </embed>


   <embed>
      <h2><span class="fa-stack">
         <span class="fa fa-circle-o fa-stack-2x"></span>
         <strong class="fa-stack-1x">
            1
         </strong>
      </span> Join the community on Slack</h2>
   </embed>

.. container:: custom-indented-paragraph

   Go to `greatexpectations.io/slack <https://greatexpectations.io/slack>`__ and introduce yourself in the ``#contributors-contributing`` channel.

.. raw:: html

   <embed>
      <h2><span class="fa-stack">
         <span class="fa fa-circle-o fa-stack-2x"></span>
         <strong class="fa-stack-1x">
            2
         </strong>
      </span> Set up your development environment</h2>
   </embed>

.. container:: custom-indented-paragraph

   Follow :ref:`these instructions <contributing_setting_up_your_dev_environment>` to set up your dev environment.

   Alternatively, for small changes that don't need to be tested locally (e.g. documentation changes), you can :ref:`make changes directly through Github <contributing_make_changes_through_github>`.

.. raw:: html

   <embed>
      <h2><span class="fa-stack">
         <span class="fa fa-circle-o fa-stack-2x"></span>
         <strong class="fa-stack-1x">
            3
         </strong>
      </span> Identify the type of contribution that you want to make</h2>
   </embed>

.. container:: custom-indented-paragraph

    Issues in GitHub are a great place to start. Check out the `help wanted <https://github.com/great-expectations/great_expectations/labels/help%20wanted>`__ and `good first issue <https://github.com/great-expectations/great_expectations/labels/good%20first%20issue>`__ labels. Comment to let everyone know you’re working on it.

    If there’s no issue for what you want to work on, please create one. Add a comment to let everyone know that you're working on it. We prefer small, incremental commits, because it makes the thought process behind changes easier to review.

    Some Expectations are implemented in the core library. Many others are contributed by the community of data practitioners that bring their domain knowledge and share it as Expectations.
    This :ref:`how-to guide <how_to_guides__creating_and_editing_expectations__how_to_template>` will walk you through the steps required to develop and contribute a new Expectation.

    Our :ref:`contributing__levels_of_maturity` grid provides guidelines for how the maintainers of Great Expectations evaluate levels of maturity of a feature.

..
   The `public test grid <https://grid.greatexpectations.io>`__ is another good entry point. If there's an element in the test grid that you want to work on, please create an issue in Github to let others know that you're taking it on. (#FIXME: This can't go live until the test grid is real.)

.. raw:: html

   <embed>
      <h2><span class="fa-stack">
         <span class="fa fa-circle-o fa-stack-2x"></span>
         <strong class="fa-stack-1x">
            4
         </strong>
      </span> Start developing</h2>
   </embed>

.. container:: custom-indented-paragraph

   Make sure to reference the :ref:`contributing__style_guide` and instructions on :ref:`contributing_testing` when developing your code. When your changes are ready, run through our :ref:`contributing_contribution_checklist` for pull requests.

   Note that if it’s your first contribution, there is a standard :ref:`contributing_cla` to sign.

..
   Each :ref:`type of contribution <contributing_types_of_contributions>` has its own development flow and testing requirements.


.. toctree::
   :maxdepth: 1
   :hidden:

   /contributing/setting_up_your_dev_environment
   /contributing/contribution_checklist
   /contributing/make_changes_through_github
   /contributing/testing
   /contributing/levels_of_maturity
   /contributing/style_guide
   /contributing/miscellaneous


