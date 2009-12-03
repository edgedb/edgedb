import copy

import semantix

from semantix.utils import merge, graph
from semantix.caos import Class, ConceptLinkType, MetaError

from semantix.caos.backends.meta import BaseMetaBackend
from semantix.caos.backends.yaml.meta.schemas import Semantics as SemanticsSchema


class MetaData(object):
    def __init__(self, dct=None, validate=True):
        if dct:
            if validate:
                self.dct = SemanticsSchema.check(dct)
            else:
                self.dct = dct
        else:
            self.dct = dict()


class MetaDataIterator(object):
    def __init__(self, helper, iter_atoms):
        self.helper = helper
        self.iter_atoms = iter_atoms

        if iter_atoms:
            self.iter = iter([v for n, v in helper._atoms.items() if not n.startswith('__')])
        else:
            self.iter = iter(helper.concepts_list)

    def __iter__(self):
        return self

    def __next__(self):
        concept = next(self.iter)
        return Class(concept['name'], meta_backend=self.helper)


class MetaBackend(BaseMetaBackend):

    def __init__(self, metadata):
        super().__init__()

        data = metadata.dct['semantic_network']

        self._atoms = self.read_atoms(data)

        self.concepts_list = graph.normalize(self.read_concepts(data), merger=self.merge_concepts)
        self._concepts = {node['name']: node for node in self.concepts_list}

        link_types = self.read_link_types(self.concepts_list)

        for name, node in self._concepts.items():
            links = {}
            link_target_types = {}

            for (link_name, target), link in node["links"].items():
                if (link_name, target) in links:
                    raise MetaError('%s --%s--> %s link redefinition' % (name, link_name, target))

                if self.is_atom(target):
                    if link_name in link_target_types and link_target_types[link_name] != 'atom':
                        raise MetaError('%s link is already defined as a link to non-atom')

                    if 'mods' in link and link['mods'] is not None:
                        # Got an inline atom definition.
                        # We must generate a unique name here
                        atom_name = '__' + node['name'] + '__' + link_name
                        self._atoms[atom_name] = {'name': atom_name,
                                                  'extends': target,
                                                  'mods': link['mods'],
                                                  'default': link['default']}
                        del link['mods']
                        del link['default']
                        link['target'] = atom_name

                    if 'mapping' in link and link['mapping'] != '11':
                        raise MetaError('%s: links to atoms can only have a "1 to 1" mapping' % link_name)

                    link_target_types[link_name] = 'atom'
                else:
                    if link_name in link_target_types and link_target_types[link_name] == 'atom':
                        raise MetaError('%s link is already defined as a link to atom')

                    if target not in self._concepts:
                        raise MetaError('reference to an undefined node "%s" in "%s"' %
                                        (target, node['name'] + '/links/' + link_name))

                    link_target_types[link_name] = 'concept'

            if node['extends'] is not None:
                for parent in node["extends"]:
                    self._concepts[parent]["children"].add(node["name"])

    def read_atoms(self, data):
        ret = {}

        if 'atoms' in data and data['atoms'] is not None:
            atoms = data['atoms']
        else:
            atoms = {}

        for atom_name, atom in atoms.items():
            atom['name'] = atom_name
            ret[atom_name] = atom

        return ret

    def read_concepts(self, data):
        if 'concepts' in data and data['concepts'] is not None:
            concepts = copy.deepcopy(data['concepts'])
        else:
            concepts = {}

        concept_graph = {}

        for concept_name, concept in concepts.items():
            if concept_name in self._atoms:
                raise MetaError('%s already defined as an atom' % concept_name)

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
                for (link_name, target), link in node["links"].items():
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

    def iter_concepts(self):
        return MetaDataIterator(self, False)

    def iter_atoms(self):
        return MetaDataIterator(self, True)

    def is_atom(self, name):
        return super().is_atom(name) or name in self._atoms

    def do_load(self, name):
        atom = False

        if self.is_atom(name):
            return self.load_atom(self._atoms[name])

        if name not in self._concepts:
            raise MetaError('reference to an undefined concept "%s"' % name)

        concept = self._concepts[name]
        dct = {}

        dct['concept'] = name

        links = {}

        for link_key, link in concept['links'].items():
            if link_key[0] in links:
                links[link_key[0]].targets.append(link['target'])
            else:
                links[link_key[0]] = ConceptLinkType(
                                        source=name,
                                        targets=[link['target']],
                                        link_type=link_key[0],
                                        required=link['required'],
                                        mapping=link['mapping'])

        dct['links'] = links

        bases = tuple()
        if len(concept['extends']) > 0:
            for parent in concept['extends']:
                bases += (Class(parent, meta_backend=self),)

        dct['parents'] = concept['extends']

        return bases, dct

    def store(self, cls, phase):
        pass
