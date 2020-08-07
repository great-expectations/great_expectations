###########################################################################
#          auto-created readthedocs.org specific configuration            #
###########################################################################


#
# The following code was added during an automated build on readthedocs.org
# It is auto created and injected for every build. The result is based on the
# conf.py.tmpl file found in the readthedocs.org codebase:
# https://github.com/rtfd/readthedocs.org/blob/master/readthedocs/doc_builder/templates/doc_builder/conf.py.tmpl
#


import importlib
import sys
import os.path
from six import string_types

from sphinx import version_info

# Get suffix for proper linking to GitHub
# This is deprecated in Sphinx 1.3+,
# as each page can have its own suffix
if globals().get('source_suffix', False):
    if isinstance(source_suffix, string_types):
        SUFFIX = source_suffix
    elif isinstance(source_suffix, (list, tuple)):
        # Sphinx >= 1.3 supports list/tuple to define multiple suffixes
        SUFFIX = source_suffix[0]
    elif isinstance(source_suffix, dict):
        # Sphinx >= 1.8 supports a mapping dictionary for multiple suffixes
        SUFFIX = list(source_suffix.keys())[0]  # make a ``list()`` for py2/py3 compatibility
    else:
        # default to .rst
        SUFFIX = '.rst'
else:
    SUFFIX = '.rst'

# Add RTD Static Path. Add to the end because it overwrites previous files.
if not 'html_static_path' in globals():
    html_static_path = []
if os.path.exists('_static'):
    html_static_path.append('_static')

# Add RTD Theme only if they aren't overriding it already
using_rtd_theme = (
        (
                'html_theme' in globals() and
                html_theme in ['default'] and
                # Allow people to bail with a hack of having an html_style
                'html_style' not in globals()
        ) or 'html_theme' not in globals()
)
if using_rtd_theme:
    theme = importlib.import_module('sphinx_rtd_theme')
    html_theme = 'sphinx_rtd_theme'
    html_style = None
    html_theme_options = {}
    if 'html_theme_path' in globals():
        html_theme_path.append(theme.get_html_theme_path())
    else:
        html_theme_path = [theme.get_html_theme_path()]

if globals().get('websupport2_base_url', False):
    websupport2_base_url = 'https://readthedocs.com/websupport'
    websupport2_static_url = 'https://assets.readthedocs.com/'

# Add project information to the template context.
context = {
    'using_theme': using_rtd_theme,
    'html_theme': html_theme,
    'current_version': "0.11.9",
    'version_slug': "0.11.9",
    'MEDIA_URL': "https://media.readthedocs.com/media/",
    'STATIC_URL': "https://assets.readthedocs.com/",
    'PRODUCTION_DOMAIN': "readthedocs.com",
    'versions': [
        ("latest", "/en/latest/"),
        ("stable", "/en/stable/"),
        ("0.11.9", "/en/0.11.9/"),
        ("0.11.8", "/en/0.11.8/"),
    ],
    'downloads': [
    ],
    'subprojects': [
    ],
    'slug': 'great-expectations-great-expectations',
    'name': u'great_expectations',
    'rtd_language': u'en',
    'programming_language': u'py',
    'canonical_url': 'https://greatexpectations.io/en/latest/',
    'analytics_code': 'UA-138955219-1',
    'single_version': False,
    'conf_py_path': '/docs/',
    'api_host': 'https://readthedocs.com',
    'github_user': 'great-expectations',
    'proxied_api_host': '/_',
    'github_repo': 'great_expectations',
    'github_version': '0.11.9',
    'display_github': True,
    'bitbucket_user': 'None',
    'bitbucket_repo': 'None',
    'bitbucket_version': '0.11.9',
    'display_bitbucket': False,
    'gitlab_user': 'None',
    'gitlab_repo': 'None',
    'gitlab_version': '0.11.9',
    'display_gitlab': False,
    'READTHEDOCS': True,
    'using_theme': (html_theme == "default"),
    'new_theme': (html_theme == "sphinx_rtd_theme"),
    'source_suffix': SUFFIX,
    'ad_free': False,
    'docsearch_disabled': False,
    'user_analytics_code': 'UA-138955219-1',
    'global_analytics_code': 'UA-17997319-2',
    'commit': '3b49078e',
}




if 'html_context' in globals():

    html_context.update(context)

else:
    html_context = context

# Add custom RTD extension
if 'extensions' in globals():
    # Insert at the beginning because it can interfere
    # with other extensions.
    # See https://github.com/rtfd/readthedocs.org/pull/4054
    extensions.insert(0, "readthedocs_ext.readthedocs")
else:
    extensions = ["readthedocs_ext.readthedocs"]

# Add External version warning banner to the external version documentation
if 'tag' == 'external':
    extensions.insert(1, "readthedocs_ext.external_version_warning")

project_language = 'en'

# User's Sphinx configurations
language_user = globals().get('language', None)
latex_engine_user = globals().get('latex_engine', None)
latex_elements_user = globals().get('latex_elements', None)

# Remove this once xindy gets installed in Docker image and XINDYOPS
# env variable is supported
# https://github.com/rtfd/readthedocs-docker-images/pull/98
latex_use_xindy = False

chinese = any([
    language_user in ('zh_CN', 'zh_TW'),
    project_language in ('zh_CN', 'zh_TW'),
    ])

japanese = any([
    language_user == 'ja',
    project_language == 'ja',
    ])

if chinese:
    latex_engine = latex_engine_user or 'xelatex'

    latex_elements_rtd = {
        'preamble': '\\usepackage[UTF8]{ctex}\n',
    }
    latex_elements = latex_elements_user or latex_elements_rtd
elif japanese:
    latex_engine = latex_engine_user or 'platex'

# Make sure our build directory is always excluded
exclude_patterns = globals().get('exclude_patterns', [])
exclude_patterns.extend(['_build'])