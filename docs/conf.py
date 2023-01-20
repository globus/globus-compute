import funcx  # noqa:E402

# -- Project information -----------------------------------------------------

project = "funcX"
copyright = "2019, The University of Chicago"
author = "The funcX Team"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = funcx.__version__.rsplit(".", 1)[0]
# The full version, including alpha/beta/rc tags.
release = funcx.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

autoclass_content = "both"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# theming, styles, and color options
pygments_style = "friendly"
pygments_dark_style = "monokai"  # this is a furo-specific option
html_show_sourcelink = True
html_theme = "furo"
html_static_path = ["_static"]
html_theme_options = {
    "light_logo": "images/funcX-light-cropped.png",
    "dark_logo": "images/funcX-dark-cropped.png",
}
