import copy

import semantix
from semantix.lib import merge, graph
from semantix.lib.caos.domain import DomainClass
from semantix.lib.caos.concept import ConceptClass, ConceptAttribute
from semantix.lib.caos.datasources.introspection.table import *
from semantix.lib.caos.backends.meta.base import MetaError
from semantix.lib.caos.backends.meta.yaml.schemas.semantics import Semantics

class MetaData(object):
    dct = {}

    @classmethod
    def _create_class(cls, meta, dct):
        Semantics.validate(meta)

        base = semantix.Import(meta['class']['parent_module'], meta['class']['parent_class'])
        return type(meta['class']['name'], (base,), {'dct': merge.merge_dicts(dct, base.dct)})

class MetaBackendHelper(object):
    def __init__(self, metadata):
        self.semantics = metadata.dct['semantic_network']

        concept_graph = {}

        for concept_name, concept in self.semantics['concepts'].items():
            if concept is None:
                concept = {}

            concept["name"] = concept_name

            concept_graph[concept_name] = {"item": concept, "merge": [], "deps": []}
            if "extends" in concept:
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
            if "links" in node:
                for llink in node["links"]:
                    for link_name, link in llink.items():
                        if "type" in link:
                            link_types.append(link["type"])
                        else:
                            link_types.append(link_name)

        concept_graph_dict = {}

        for node in concept_graph:
            for attr in node['attributes'].values():
                if 'required' not in attr:
                    attr['required'] = False
                if 'default' not in attr:
                    attr['default'] = None

            concept_graph_dict[node["name"]] = node

        for node in concept_graph:
            if "extends" in node:
                for parent in node["extends"]:
                    concept_graph_dict[parent]["children"].add(node["name"])

        self.semantics = concept_graph_dict

    @staticmethod
    def merge_concepts(left, right):
        result = merge.merge_dicts(left, right)

        if "heuristics" in result:
            if "comparison" in result["heuristics"]:
                attrs = []
                heuristics = []
                for rule in reversed(result["heuristics"]["comparison"]):
                    if "attribute" not in rule or rule["attribute"] not in attrs:
                        heuristics.append(rule)
                        if "attribute" in rule:
                            attrs.append(rule["attribute"])

                result["heuristics"]["comparison"] = heuristics

        result["extends"] = copy.deepcopy(right["extends"])

        if left["name"] in result["extends"]:
            result["extends"].remove(left["name"])

        result["extends"].append(left["name"])

        for cls in left["extends"]:
            if cls in result["extends"]:
                result["extends"].remove(cls)
            result['extends'].append(cls)

        return result

    def load(self, name):
        if name not in self.semantics:
            raise MetaError('reference to an undefined concept "%s"' % name)

        concept = self.semantics[name]

        dct = {}

        dct['name'] = name
        dct['attributes'] = {}

        for attr_name, attr in concept['attributes'].items():
            dct['attributes'][attr_name] = ConceptAttribute(DomainClass(attr['domain']),
                                                            attr['required'], attr['default'])

        bases = tuple()
        if len(concept['extends']) > 0:
            for parent in concept['extends']:
                bases += (ConceptClass(parent),)

        return bases, dct

    def store(self, cls):
        pass
