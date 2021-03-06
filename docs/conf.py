# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys
sys.path.insert(0, os.path.abspath('..'))


# -- Project information -----------------------------------------------------

project = 'cfm-reslib'
copyright = '2019, CloudSnorkel'
author = 'CloudSnorkel'

# The full version, including alpha/beta/rc tags
release = '0.1'

master_doc = 'index'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


# -- Hooks for generating docs-------------------------------------------------

INDEX = """.. _resources:

Available Custom Resources
==========================

.. toctree::
   :maxdepth: 5
   :caption: Contents:
   :glob:
   :includehidden:

   res_*
"""


def build_inited_handler(app):
    app.add_javascript("js/github.js")

    import os.path
    from glob import glob
    import subprocess

    # ugly workaround for https://github.com/rtfd/readthedocs.org/issues/3181
    if os.getenv("READTHEDOCS"):
        subprocess.check_call(["poetry", "config", "virtualenvs.create", "false"])
        subprocess.check_call(["poetry", "install"])

    import cfmreslib.docs
    import cfmreslib.resources

    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources")
    os.makedirs(base, exist_ok=True)

    for rst in glob(os.path.join(base, "*.rst")):
        os.unlink(rst)

    with open(os.path.join(base, "index.rst"), "w") as index:
        index.write(INDEX)

    for res in cfmreslib.resources.ALL_RESOURCES:
        with cfmreslib.docs.DocWriter(base, f"res_{res.NAME}") as doc:
            res.write_docs(doc)


def setup(app):
    app.connect('builder-inited', build_inited_handler)
