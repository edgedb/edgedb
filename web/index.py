import cherrypy
import cherrypy.lib.static
import os
import json


import cgi
from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import HtmlFormatter

from semantix import datasources, readers, db
from semantix.caos import Concept, Class
from semantix.caos.backends.meta.pgsql import MetaBackend as PgSQLMetaBackend
from semantix.caos.backends.data.pgsql import DataBackend as PgSQLDataBackend

from docs.datasources.entities_tree_level import EntitiesTreeLevel

class HTMLConceptTemlates(object):
    @staticmethod
    def render_article(entity, default_title=None, tags_as_title=False, level=0):
        output = '<div class="section c-article">'

        tags = ''
        if 'attrs' in entity.attrs:
            tags = cgi.escape(entity.attrs['tags'])

        title = None
        if entity.attrs['title']:
            title = entity.attrs['title']
        else:
            if default_title:
                title = default_title

        if not title and tags and tags_as_title:
            title = tags

        if title:
            output += '<h%(level)d semantix:entity-id="%(id)s" class="semantix-draggable article-title">%(title)s</h%(level)d>' % {
                                                                    'title': cgi.escape(title),
                                                                    'level': level + 1,
                                                                    'id': entity.id
                                                            }

        content = entity.attrs['content']
        if entity.name == 'code-snippet':
            lang = iter(entity.links['language']).next().target
            if lang.attrs['name'] == 'code':
                content = highlight(content, get_lexer_by_name('javascript'), HtmlFormatter())
                tags += ' highlight'
            elif lang.attrs['name'] == 'css':
                content = highlight(content, get_lexer_by_name('css'), HtmlFormatter())
                tags += ' highlight'
            elif lang.attrs['name'] == 'html':
                content = highlight(content, get_lexer_by_name('html'), HtmlFormatter())
                tags += ' highlight'
        else:
            content = cgi.escape(content)

        if entity.attrs['content']:
            output += '<div class="article-p %s">%s</div>' % (tags, content)

        for section in entity.links['section']:
            output += HTMLConceptTemlates.render_article(section.target, level=level+1, tags_as_title=tags_as_title)

        return output + '</div>'

    @staticmethod
    def render_function(entity):
        def render_function_header(entity):
            output = '<div class="function">'

            if entity.links['return'] is not None:
                output += '<span class="returns">&lt;%s&gt;</span> ' % cgi.escape(iter(entity.links['return']).next().target.attrs['name'])

            output += '<span class="name">%s</span><span class="aop">(</span>' % cgi.escape(entity.attrs['name'])
            if entity.links['argument'] is not None:
                args = []
                for arg in entity.links['argument']:
                    a = ''

                    if arg.target.links['type'] is not None:
                        a += '<span class="arg-type">&lt;%s&gt;</span> ' % cgi.escape(iter(arg.target.links['type']).next().target.attrs['name'])

                    a += cgi.escape(arg.target.attrs['name'])

                    args.append(a)

                output += '<span class="dlm">, </span>'.join(args)
            output += '<span class="acp">)</span>'

            if entity.attrs['description']:
                output += '<div class="desc">%s</div>' % cgi.escape(entity.attrs['description'])

            return output + '</div>'


        output = '<div class="section c-function">'
        output += render_function_header(entity)

        for text in entity.links['long-description']:
            output += HTMLConceptTemlates.render_article(text.target, default_title='Description', level=1)

        if entity.links['example']:
            i = 0
            for example in entity.links['example']:
                i += 1
                output += HTMLConceptTemlates.render_article(example.target, default_title='Example #%s' % i, level=1, tags_as_title=True)

        return output + '</div>'

    @staticmethod
    def render_selector(entity):
        return HTMLConceptTemlates.render_function(entity)

    @staticmethod
    def default(entity):
        output = '<div class="section default">'

        if entity.attrs['description']:
            output += '<div class="desc">%s</div>' % cgi.escape(entity.attrs['description'])

        output += '<dl>'
        attrs_output = ''
        for attr_name, attr in entity.attrs.items():
            if attr_name not in ('name', 'description'):
                attrs_output += '<dt>%s</dt><dd>%s</dd>' % (cgi.escape(attr_name), cgi.escape(str(attr)))

        if attrs_output:
            output += '<dl>' + attrs_output + '</dl>';

        output += '<h2>Links:</h2>'
        output += '<dl>'
        for link in entity.links:

            output += '<dt>%s</dt>' % cgi.escape(link.link_type)
            if 'name' in link.target.attrs:
                name = link.target.attrs['name']
            else:
                name = 'UNKNOWN NAME FOR %s' % link.target.name
            output += '<dd><a href="#" semantix:entity-id="%s" class="semantix-draggable">%s: %s</a>&nbsp;</a></dd>' % (
                                    link.target.id, cgi.escape(link.target.name), cgi.escape(name)
                        )

        output += '</dl>'

        return output + '</div>'

    @staticmethod
    def render(entity):
        concept = entity.name
        method = 'render_' + concept.replace('-', '_')

        output = '<div class="topic"><h1 semantix:entity-id="%s" class="semantix-draggable">%s' % (
                                        entity.id, cgi.escape(entity.name.capitalize())
                                    )

        if entity.attrs['name']:
            output += ': %s' % cgi.escape(entity.attrs['name'])
        output += '</h1>'

        output += getattr(HTMLConceptTemlates, method, HTMLConceptTemlates.default)(entity)

        return output + '</div>'



class Srv(object):
    def __init__(self, config):
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

        config = readers.read(config)
        config['/public'] = {
                                'tools.staticdir.on': True,
                                'tools.staticdir.dir': os.path.join(self.current_dir, 'public')
                            }

        Class.data_backend = PgSQLDataBackend(db.connection)
        Class.meta_backend = PgSQLMetaBackend(db.connection)

        cherrypy.quickstart(self, '/', config=config)

    @cherrypy.expose
    def get(self, id=None):
        ouput = """
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                <link rel="stylesheet" type="text/css" href="/public/ext/resources/css/ext-all.css" />
                <link rel="stylesheet" type="text/css" href="/public/resources/highlight.css" />
                <link rel="stylesheet" type="text/css" href="/public/resources/base.css" />
            </head>
            <body>
        """
        entity = Concept(int(id))
        return ouput + HTMLConceptTemlates.render(entity) + '</body></html>'

    @cherrypy.expose
    def index(self, *args, **kw):
        return cherrypy.lib.static.serve_file(os.path.join(self.current_dir, 'public', 'index.html'))

    @cherrypy.expose
    def get_tree_level(self, node=None):
        entity_id = node
        if entity_id is not None:
            if entity_id and entity_id != 'root':
                entity_id = int(entity_id)
            else:
                entity_id = None

        return json.dumps(
                                    EntitiesTreeLevel.fetch(entity_id=entity_id)
                                )

    @cherrypy.expose
    def get_topic(self, entity_id):
        if entity_id == 'root':
            return ''

        entity = Concept(int(entity_id))
        return HTMLConceptTemlates.render(entity)

Srv('config.yml')
