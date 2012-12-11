##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Default Sphinx configuration file for metamagic projects"""


extensions = ['sphinx.ext.autodoc', 'sphinx.ext.todo',
              'sphinx.ext.coverage', 'sphinx.ext.viewcode',
              'sphinx.ext.intersphinx']


templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

exclude_patterns = ['_build']
pygments_style = 'sphinx'

html_theme = 'default'

html_static_path = ['_static']
intersphinx_mapping = {'python': ('http://docs.python.org/3.2', None)}
autoclass_content = 'both'
