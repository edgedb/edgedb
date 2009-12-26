import copy

import semantix

from semantix.utils import merge, graph
from semantix import lang
from semantix.caos import MetaError

from semantix.caos.backends.meta import MetaBackend, RealmMeta, Atom, Concept, ConceptLinkType


class Backend(MetaBackend):

    def __init__(self, source_path):
        super().__init__()
        self.metadata = next(lang.load(source_path))

    def getmeta(self):
        meta = RealmMeta()

        data = self.metadata

        self.read_atoms(data, meta)

        concepts = graph.normalize(self.read_concepts(data, meta), merger=self.merge_concepts)

        link_types = self.read_link_types(concepts)

        for node in concepts:

            concept = Concept(name=node['name'], base=node['extends'], backend=data['backend'])
            meta.add(concept)

        for node in concepts:
            concept = meta.get(node['name'])
            links = {}
            link_target_types = {}

            for (link_name, target), link in node["links"].items():
                if (link_name, target) in links:
                    raise MetaError('%s --%s--> %s link redefinition' % (node['name'], link_name, target))

                target_obj = meta.get(target)

                if not target_obj:
                    raise MetaError('reference to an undefined node "%s" in "%s"' %
                                    (target, node['name'] + '/links/' + link_name))

                if isinstance(target_obj, Atom):
                    if link_name in link_target_types and link_target_types[link_name] != 'atom':
                        raise MetaError('%s link is already defined as a link to non-atom')

                    if 'mods' in link and link['mods'] is not None:
                        # Got an inline atom definition.
                        # We must generate a unique name here
                        atom_name = '__' + node['name'] + '__' + link_name
                        atom = Atom(name=atom_name, base=target, default=link['default'], automatic=True, backend=data['backend'])
                        self.add_atom_mods(atom, link['mods'])
                        meta.add(atom)

                        del link['mods']
                        del link['default']
                        link['target'] = atom_name

                    if 'mapping' in link and link['mapping'] != '11':
                        raise MetaError('%s: links to atoms can only have a "1 to 1" mapping' % link_name)

                    link_target_types[link_name] = 'atom'
                else:
                    if link_name in link_target_types and link_target_types[link_name] == 'atom':
                        raise MetaError('%s link is already defined as a link to atom')

                    link_target_types[link_name] = 'concept'

                link_obj = ConceptLinkType(
                                            source=meta.get(node['name']),
                                            targets={meta.get(link['target'])},
                                            link_type=link_name,
                                            required=link['required'],
                                            mapping=link['mapping'])
                concept.add_link(link_obj)


            """ XXX: is this needed?
            if node['extends'] is not None:
                for parent in node["extends"]:
                    self._concepts[parent]["children"].add(node["name"])
            """

        return meta

    def add_atom_mods(self, atom, mods):
        for mod in mods:
            mod_type, mod = list(mod.items())[0]
            if isinstance(mod, str):
                mod = mod.strip()
            atom.add_mod(mod_type, mod)

    def read_atoms(self, data, meta):
        if 'atoms' in data and data['atoms'] is not None:
            for atom_name, atom_desc in data['atoms'].items():
                atom = Atom(name=atom_name, base=atom_desc['extends'], default=atom_desc['default'], backend=data['backend'])

                if atom_desc['mods'] is not None:
                    self.add_atom_mods(atom, atom_desc['mods'])

                meta.add(atom)

    def read_concepts(self, data, meta):
        if 'concepts' in data and data['concepts'] is not None:
            concepts = copy.deepcopy(data['concepts'])
        else:
            concepts = {}

        concept_graph = {}

        for concept_name, concept in concepts.items():
            if meta.get(concept_name):
                raise MetaError('%s already defined' % concept_name)

            if concept is None:
                concept = {}

            concept["name"] = concept_name

            concept_graph[concept_name] = {"item": concept, "merge": [], "deps": []}

            if concept['extends'] is not None:
                if not isinstance(concept["extends"], list):
                    concept["extends"] = list((concept["extends"],))
                else:
                    concept["extends"] = list(concept["extends"])
                concept_graph[concept_name]["merge"].extend(concept['extends'])
            else:
                concept["extends"] = []

            concept["children"] = set()

            links = {}

            for llink in concept["links"]:
                (link_name, link) = list(llink.items())[0]

                if isinstance(link, str):
                    target = link
                    properties = {
                                  'default': None,
                                  'required': False,
                                  'mods': None,
                                  'extends': None,
                                  'mapping': '11'
                                  }
                else:
                    (target, properties) = list(link.items())[0]

                properties['target'] = target
                links[(link_name, target)] = properties

            concept['links'] = links

        return concept_graph

    def read_link_types(self, concept_graph):
        link_types = []
        for node in concept_graph:
            if node['links'] is not None:
                for (link_name, target) in node["links"]:
                    link_types.append(link_name)

        return link_types

    @staticmethod
    def merge_concepts(left, right):
        result = merge.merge_dicts(left, right)

        if result["heuristics"] is not None:
            if result["heuristics"]["comparison"] is not None:
                attrs = []
                heuristics = []
                for rule in reversed(result["heuristics"]["comparison"]):
                    if "attribute" not in rule or rule["attribute"] not in attrs:
                        heuristics.append(rule)
                        if "attribute" in rule:
                            attrs.append(rule["attribute"])

                result["heuristics"]["comparison"] = heuristics

        result["extends"] = copy.deepcopy(right["extends"])

        return result
