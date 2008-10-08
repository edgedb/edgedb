import cherrypy
import cherrypy.lib.static
import os
import simplejson
from semantix.lib import datasources, readers

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

Srv('config.yml')
