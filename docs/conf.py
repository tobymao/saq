# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import datetime
import os
import re
import sys

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

def get_version():
    verstrline = open("../saq/__init__.py", "rt").read()
    mob = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", verstrline, re.M)
    if mob:
        return mob.group(1)
    else:
        raise RuntimeError("Unable to find version string")

project = 'SAQ'
year = datetime.datetime.now().year
copyright = f'{year}, Toby Mao'
author = 'Toby Mao'

# The short X.Y version
version = get_version()
# The full version, including alpha/beta/rc tags
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'myst_parser',
    'autoapi.extension',
    'sphinx_design',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'redis': ('https://redis-py.readthedocs.io/en/stable/', None),
    'typing_extensions': ('https://typing-extensions.readthedocs.io/en/latest/', None),
}

autoapi_dirs = ['../saq']
autoapi_ignore = [
    '*/saq/__main__.py',
    '*/saq/web/common.py'
]
autoapi_options = [
    'members',
    'undoc-members',
    'private-members',
    'show-inheritance',
    'show-module-summary',
    'special-members',
    'imported-members',
]
napoleon_use_admonition_for_notes = True
napoleon_preprocess_types = True
napoleon_attr_annotations = True
highlight_language = 'python'

myst_enable_extensions = [
    "fieldlist",
    "colon_fence",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']

html_theme_options = {
    'source_repository': 'https://github.com/tobymao/saq/'
}
