import copy

import semantix

from semantix.utils import merge, graph
from semantix.caos import Class, ConceptLinkType, MetaError

from semantix.caos.backends.meta import BaseMetaBackend

class MetaData(object):
    dct = {}

    @classmethod
    def _create_class(cls, meta, dct):
        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'dct': merge.merge_dicts(dct, base.dct)})

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

        self._atoms = {}
        self._concepts = {}

        data = metadata.dct['semantic_network']

        if 'atoms' in data and data['atoms'] is not None:
            atoms = data['atoms']
        else:
            atoms = {}

        for atom_name, atom in atoms.items():
            atom['name'] = atom_name
            self._atoms[atom_name] = atom

        if 'concepts' in data and data['concepts'] is not None:
            concepts = data['concepts']
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

        concept_graph = graph.normalize(concept_graph, merger=MetaBackend.merge_concepts)
        link_types = []

        for node in concept_graph:
            if node['links'] is not None:
                for llink in node["links"]:
                    for link_name, link in llink.items():
                        link_types.append(link_name)

        concept_graph_dict = {}

        for node in concept_graph:
            concept_graph_dict[node["name"]] = node

        for node in concept_graph:
            atom_links = {}
            links = {}

            if node["links"] is not None:
                for llink in node["links"]:
                    for link_name, link in llink.items():
                        if 'atom' in link['target']:
                            if not self.is_atom(link['target']['atom']):
                                raise MetaError('reference to an undefined atom "%s" in "%s"' %
                                                (link['target']['atom'], node['name'] + '/links/' + link_name))
                            if 'mods' in link['target'] and link['target']['mods'] is not None:
                                """
                                Inline atom definition
                                We must generate a unique name here
                                """
                                atom_name = '__' + node['name'] + '__' + link_name
                                self._atoms[atom_name] = {'name': atom_name,
                                                          'extends': link['target']['atom'],
                                                          'mods': link['target']['mods'],
                                                          'default': link['target']['default']}
                                del link['target']['mods']
                                del link['target']['default']
                                link['target']['atom'] = atom_name

                            if link_name in links:
                                raise MetaError('%s is already defined as a concept link'
                                                % link_name)

                            atom_links[link_name] = True

                        elif 'concept' in link["target"]:
                            if link['target']['concept'] not in concept_graph_dict:
                                raise MetaError('reference to an undefined concept "%s" in "%s"' %
                                                (link['target']['concept'], node['name'] + '/links/' + link_name))

                            link_key = (link_name, link['target']['concept'])
                            if link_key in links:
                                raise MetaError('%s -> %s link redefinition'
                                                % (link_name, link['target']['concept']))
                            elif link_name in atom_links:
                                raise MetaError('%s is already defined as a link to atom'
                                                % link_name)
                            else:
                                links[link_key] = True
                                links[link_name] = True
            else:
                node["links"] = []

            if node['extends'] is not None:
                for parent in node["extends"]:
                    concept_graph_dict[parent]["children"].add(node["name"])

        self._concepts = concept_graph_dict
        self.concepts_list = concept_graph

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

        for llink in concept['links']:
            for link_name, link in llink.items():
                if 'atom' in link['target']:
                    links[link_name] = ConceptLinkType(source=name, targets=[link['target']['atom']],
                                                       link_type=link_name, required=link['required'],
                                                       mapping='11')
                else:
                    if link_name in links:
                        links[link_name].targets.append(link['target']['concept'])
                    else:
                        links[link_name] = ConceptLinkType(name, [link['target']['concept']], link_name,
                                                           link['target']['mapping'], link['required'])

        dct['links'] = links

        bases = tuple()
        if len(concept['extends']) > 0:
            for parent in concept['extends']:
                bases += (Class(parent, meta_backend=self),)

        dct['parents'] = concept['extends']

        return bases, dct

    def store(self, cls, phase):
        pass
