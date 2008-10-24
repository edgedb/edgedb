import copy

import semantix
from semantix.lib import merge, graph
from semantix.lib.caos import ConceptClass, ConceptAttributeType, ConceptLinkType, DomainClass, MetaError

from .schemas.semantics import Semantics

class MetaData(object):
    dct = {}

    @classmethod
    def _create_class(cls, meta, dct):
        Semantics.validate(meta, dct)

        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'dct': merge.merge_dicts(dct, base.dct)})

class MetaDataIterator(object):
    def __init__(self, helper):
        self.helper = helper
        self.iter = iter(helper.semantics_list)

    def __iter__(self):
        return self

    def next(self):
        concept = next(self.iter)
        return ConceptClass(concept['name'], meta_backend=self.helper.meta_backend)

class MetaBackendHelper(object):
    def __init__(self, metadata, meta_backend):
        self.semantics = metadata.dct['semantic_network']
        self.meta_backend = meta_backend

        concept_graph = {}

        for concept_name, concept in self.semantics['concepts'].items():
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

        concept_graph = graph.normalize(concept_graph, merger=MetaBackendHelper.merge_concepts)
        link_types = []

        for node in concept_graph:
            if node['links'] is not None:
                for llink in node["links"]:
                    for link_name, link in llink.items():
                        if link['type'] is not None:
                            link_types.append(link["type"])
                        else:
                            link_types.append(link_name)

        concept_graph_dict = {}

        for node in concept_graph:
            for attr in node['attributes'].values():
                if attr['required'] is None:
                    attr['required'] = False

            concept_graph_dict[node["name"]] = node

        for node in concept_graph:
            if node["links"] is not None:
                for llink in node["links"]:
                    for link_name, link in llink.items():
                        if link["target"] not in concept_graph_dict:
                            raise MetaError('reference to an undefined concept "%s" in "%s"' %
                                            (link['target'], node['name'] + '/links/' + link_name))
            else:
                node["links"] = []

            if node['extends'] is not None:
                for parent in node["extends"]:
                    concept_graph_dict[parent]["children"].add(node["name"])

        self.semantics = concept_graph_dict
        self.semantics_list = concept_graph

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

    def __iter__(self):
        return MetaDataIterator(self)

    def load(self, name):
        if name not in self.semantics:
            raise MetaError('reference to an undefined concept "%s"' % name)

        concept = self.semantics[name]

        dct = {}

        dct['name'] = name
        dct['attributes'] = {}
        dct['links'] = {}

        for attr_name, attr in concept['attributes'].items():
            if isinstance(attr['domain'], dict):
                attr['domain']['name'] = name + '__' + attr_name

            dct['attributes'][attr_name] = ConceptAttributeType(DomainClass(attr['domain'],
                                                                            meta_backend=self.meta_backend),
                                                                attr['required'], attr['default'])

        for llink in concept['links']:
            for link_name, link in llink.items():
                dct['links'][(link_name, link['target'])] = ConceptLinkType(name, link['target'],
                                                                            link_name, link['mapping'])

        # FIXME:
        dct['rlinks'] = {}

        bases = tuple()
        if len(concept['extends']) > 0:
            for parent in concept['extends']:
                bases += (ConceptClass(parent, meta_backend=self.meta_backend),)

        dct['parents'] = concept['extends']

        return bases, dct


    def store(self, cls, phase):
        pass
