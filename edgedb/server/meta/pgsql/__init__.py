from semantix.lib.caos.backends.meta.pgsql import concept, domain, link
import semantix.lib.caos.domain

backends = {}

def load(cls, name):
    if issubclass(cls, semantix.lib.caos.domain.DomainClass):
        bases = tuple((semantix.lib.caos.domain.Domain,))

        if domain.DomainBackend not in backends:
            backends[domain.DomainBackend] = domain.DomainBackend()

        bases += tuple(backends[domain.DomainBackend].load(cls, name))
    elif issubclass(cls, semantix.lib.caos.concept.ConceptClass):
        bases = tuple((semantix.lib.caos.concept.Concept,))

        if concept.ConceptBackend not in backends:
            backends[concept.ConceptBackend] = concept.ConceptBackend()

        bases += tuple(backends[concept.ConceptBackend].load(cls, name))

    return bases

def store(cls):
    if issubclass(cls, semantix.lib.caos.domain.DomainClass):
        if domain.DomainBackend not in backends:
            backends[domain.DomainBackend] = domain.DomainBackend()

        backends[domain.DomainBackend].store(cls)
