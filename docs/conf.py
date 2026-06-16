# Sphinx configuration for nirs4all-cluster.
from __future__ import annotations

import nirs4all_cluster  # import-safe: never pulls in nirs4all

# -- Project information -----------------------------------------------------
project = "nirs4all-cluster"
author = "nirs4all"
copyright = "2026, nirs4all"

release = nirs4all_cluster.__version__
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
]

source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# The bundled design docs (included from the repo root) carry their own internal
# relative links and heading styles; don't fail the build on those.
suppress_warnings = [
    "myst.xref_missing",
    "myst.header",
    "toc.not_included",
    "misc.highlighting_failure",
]

# -- MyST --------------------------------------------------------------------
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "tasklist",
    "linkify",
    "substitution",
    "attrs_inline",
    "smartquotes",
]
myst_heading_anchors = 3
myst_fence_as_directive = ["mermaid"]

# -- Autodoc / Napoleon ------------------------------------------------------
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_class_signature = "separated"
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "exclude-members": "model_config, model_fields, model_computed_fields",
}
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# -- intersphinx -------------------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
}

# -- HTML output (furo) ------------------------------------------------------
html_theme = "furo"
html_title = f"nirs4all-cluster {version}"
html_static_path = ["_static"]
html_favicon = "_static/favicon.ico"
html_theme_options = {
    "light_logo": "horizontal.svg",
    "dark_logo": "horizontal-dark.svg",
    "sidebar_hide_name": True,
    "source_repository": "https://github.com/GBeurier/nirs4all-cluster",
    "source_branch": "main",
    "source_directory": "docs/",
}

# -- copybutton --------------------------------------------------------------
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regex = True

# -- mermaid -----------------------------------------------------------------
mermaid_version = "11.4.0"
