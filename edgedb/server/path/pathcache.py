from semantix.lib import helper, datasources, readers, db, config
from semantix.lib.caos import BaseConcept, Concept, EntityCollection

class PathCache(object):
    def __init__(self, heap, config):
        super(TreeCache, self).__init__(heap, config)
        self.cursor = db.connection.cursor()
