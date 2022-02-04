.. _how_to_guides__miscellaneous__how_to_add_comments_to_a_page_in_documentation:

How to add comments to a page on docs.greatexpectations.io
==========================================================

Many of the pages in Great Expectations' documentation allow comments through our discussion forum. Here's how to enable comments, specifically on :ref:`how_to_guides`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Written a new How-to Guide <how_to_guides__miscellaneous__how_to_write_a_how_to_guide>`


Steps
-----

1. Create and publish an article on the `Great Expectations discussion forum <https://discuss.greatexpectations.io/>`_.

	- Please use the title of your How-to Guide as the title of the article (Ex: "How to configure an ActionListValidationOperator").
	- Please copy and paste this text into the body of the article, and replace the link with your guide's link.

    .. code-block::

        This article is for comments to: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/{some_path}/{your_guide_name}.html
        
        Please comment +1 if this How-to Guide is important to you.
	
2. After publishing the article, find the ``topic_id`` at the end of the article's URL: (Ex: ``219`` in ``/t/how-to-configure-an-actionlistvalidationoperator/219``). Please add this code to the bottom of your Guide, and replace ``{topic_id}`` with the real id.

.. raw:: html

    <blockquote>
    <div><div class="highlight-rst notranslate"><div class="highlight"><pre><span>
        Comments
        --------

        .. discourse::
            :topic_identifier: {topic_id}
    </span>
    </pre></div>
    </div>
    </div></blockquote>

Additional Notes
----------------

The ``sphinxcontrib-discourse`` plugin used for this integration requires a verified domain, in this case ``docs.greatexpectations.io``. During local development and PR review, you will see something like this:

.. image:: /images/comments-box-without-confirmed-domain.png

Don't be alarmed. This is normal. Your comment box will appear as soon as the page goes live on ``docs.greatexpectations.io``.

.. image:: /images/comments-box-with-confirmed-domain.png

After at least one comment has been added, it will look like this.

.. image:: /images/comments-box-with-comment.png


Additional Resources
--------------------

- `https://github.com/pdavide/sphinxcontrib-discourse <https://github.com/pdavide/sphinxcontrib-discourse>`_

Comments
--------

.. discourse::
   :topic_identifier: 230
