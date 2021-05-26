#!/usr/bin/env python3

import sys
import os

sys.path.append( os.path.abspath('../../'))
extensions = ['sphinx.ext.todo', 'sphinx.ext.viewcode', 'sphinx.ext.autodoc',"sphinx.ext.intersphinx"]
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
source_suffix = '.rst'
#source_encoding = 'utf-8-sig'
master_doc = 'index'

project = 'CacheIntervals'
author = 'Cyril Godart'

version = '0.2'
release = '0.2'

language = None
exclude_patterns = []
pygments_style = 'sphinx'
todo_include_todos = False
html_theme = 'sphinx_rtd_theme'

htmlhelp_basename = 'CacheIntervalsdoc'

latex_elements = {}
latex_documents = [
  (master_doc, 'CacheIntervals.tex', 'CacheIntervals Documentation',
   'Cyril Godart', 'manual'),
]
man_pages = [
    (master_doc, 'CacheIntervals', 'CacheIntervals Documentation',
     [author], 1)
]
texinfo_documents = [
  (master_doc, 'CacheIntervals', 'CacheIntervals Documentation',
   author, 'CacheIntervals', 'One line description of project.',
   'Miscellaneous'),
]
