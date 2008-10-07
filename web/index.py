import cherrypy
import cherrypy.lib.static
import os
from semantix.lib import datasources, readers

current_dir = os.path.dirname(os.path.abspath(__file__))

class Srv(object):
    @cherrypy.expose
    def index(self, *args, **kw):
        return cherrypy.lib.static.serve_file(os.path.join(current_dir, 'public', 'index.html'))

config = readers.read('config.yml')
config['/public'] = {
                        'tools.staticdir.on': True,
                        'tools.staticdir.dir': os.path.join(current_dir, 'public')
                    }

cherrypy.quickstart(Srv(), '/', config=config)
