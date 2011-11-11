##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Default Sphinx configuration file for semantix projects"""


extensions = ['sphinx.ext.autodoc', 'sphinx.ext.todo',
              'sphinx.ext.coverage', 'sphinx.ext.viewcode']


templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

exclude_patterns = ['_build']
pygments_style = 'sphinx'

html_theme = 'default'

html_static_path = ['_static']
