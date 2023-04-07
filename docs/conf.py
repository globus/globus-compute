import globus_compute_sdk  # noqa:E402

# -- Project information -----------------------------------------------------

project = "Globus Compute"
copyright = "2019, The University of Chicago"
author = "The Globus Compute Team"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = globus_compute_sdk.__version__.rsplit(".", 1)[0]
# The full version, including alpha/beta/rc tags.
release = globus_compute_sdk.__version__


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

# Potentially different pngs for light/dark in the future
html_theme_options = {
    "light_logo": "images/globus-300x300-blue.png",
    "dark_logo": "images/globus-300x300-blue.png",
}
