import cherrypy
import cherrypy.lib.static
import os
import simplejson

from semantix.lib import datasources, readers
from semantix.lib.binder.concept.entity import EntityFactory

class Srv(object):
    def __init__(self, config):
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

        config = readers.read(config)
        config['/public'] = {
                                'tools.staticdir.on': True,
                                'tools.staticdir.dir': os.path.join(self.current_dir, 'public')
                            }

        cherrypy.quickstart(self, '/', config=config)

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

        return simplejson.dumps(
                                    datasources.fetch('entities.tree.level', entity_id=entity_id)
                                )

    @cherrypy.expose
    def get_topic(self, entity_id):
        if entity_id == 'root':
            return ''

        entity = EntityFactory.get(int(entity_id))

        output = '<div class="topic">'

        output += '<h1>%s</h1>' % entity.concept_name

        output += '<h2>Attributes:</h2>'
        output += '<dl>'
        for attr in entity.attributes:
            output += '<dt>%s</dt><dd>%s</dd>' % (attr.name, attr.value)
        output += '</dl>'

        output += '<h2>Links:</h2>'
        output += '<dl>'
        for link in entity.links:
            output += '<dt>%s</dt>' % (link)

            for el in entity.links[link]:
                output += '<dd><a href="#" id="%s">%s: %s</a>&nbsp;</a></dd>' % (el.id, el.concept_name, el.attributes['name'])
        output += '</dl>'


        return output + '</div>'

Srv('config.yml')
