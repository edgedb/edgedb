data_backend = None
meta_backend = None

def storemeta(cls, backend, phase=None):
    backend.store(cls, phase)

def syncmeta(from_backend, to_backend):
    for d in from_backend.domains():
        storemeta(d, to_backend)

    for c in from_backend.semantics():
        storemeta(c, to_backend, 1)

    for c in from_backend.semantics():
        storemeta(c, to_backend, 2)


__all__ = ['data_backend', 'meta_backend', 'storemeta', 'syncmeta']
