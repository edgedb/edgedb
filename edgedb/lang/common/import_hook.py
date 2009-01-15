import builtins, os, sys, imp

class ImportHook(object):
    @classmethod
    def _resolve_name(cls, name, package, level):
        """Return the absolute name of the module to be imported."""
        level -= 1
        try:
            if package.count('.') < level:
                raise ValueError("attempted relative import beyond top-level "
                                  "package")
        except AttributeError:
            raise ValueError("__package__ not set to a string")
        base = package.rsplit('.', level)[0]
        if name:
            return "{0}.{1}".format(base, name)
        else:
            return base

    @classmethod
    def _find_package(cls, name, has_path):
        """Return the package that the caller is in or None."""
        if has_path:
            return name
        elif '.' in name:
            return name.rsplit('.', 1)[0]
        else:
            return None

    @classmethod
    def import_hook(cls, name, globals={}, locals={}, fromlist=[], level=-1):
        caller_name = globals.get('__name__')
        package = globals.get('__package__')

        if caller_name and not package:
            package = cls._find_package(caller_name, '__path__' in globals)

        if package and package not in sys.modules:
            if not hasattr(package, 'rsplit'):
                raise ValueError("__package__ not set to a string")
            msg = ("Parent module {0!r} not loaded, "
                    "cannot perform relative import")
            raise SystemError(msg.format(package))

        if level > 0:
            name = cls._resolve_name(name, package, level)

        parent = cls.determine_parent(globals)
        q, tail = cls.find_head_package(parent, name)
        m = cls.load_tail(q, tail)

        if not fromlist:
            return q

        if hasattr(m, "__path__"):
            cls.ensure_fromlist(m, fromlist)

        return m


    @classmethod
    def determine_parent(cls, globals):
        if not globals or not '__name__' in globals:
            return None

        pname = globals['__name__']

        if '__path__' in globals:
            parent = sys.modules[pname]
            assert globals is parent.__dict__
            return parent

        if '.' in pname:
            i = pname.rfind('.')
            pname = pname[:i]
            parent = sys.modules[pname]
            assert parent.__name__ == pname
            return parent

        return None


    @classmethod
    def unknown_module(cls, partname, name, parent):
        raise ImportError("No module named " + name)


    @classmethod
    def find_head_package(cls, parent, name):
        if '.' in name:
            i = name.find('.')
            head = name[:i]
            tail = name[i+1:]
        else:
            head = name
            tail = ""

        if parent:
            qname = "%s.%s" % (parent.__name__, head)
        else:
            qname = head

        q = cls.import_module(head, qname, parent)

        if q:
            return q, tail

        if parent:
            qname = head
            parent = None

            q = cls.import_module(head, qname, parent)

            if q:
                return q, tail

        return cls.unknown_module(head, qname, parent)


    @classmethod
    def load_tail(cls, q, tail):
        m = q
        while tail:
            i = tail.find('.')
            if i < 0: i = len(tail)
            head, tail = tail[:i], tail[i+1:]
            mname = "%s.%s" % (m.__name__, head)

            m = cls.import_module(head, mname, m)

            if not m:
                return cls.unknown_module(head, mname, m)
        return m


    @classmethod
    def ensure_fromlist(cls, m, fromlist, recursive=0):
        for sub in fromlist:
            if sub == "*":
                if not recursive:
                    try:
                        all = m.__all__
                    except AttributeError:
                        pass
                    else:
                        cls.ensure_fromlist(m, all, 1)
                continue
            if sub != "*" and not hasattr(m, sub):
                subname = "%s.%s" % (m.__name__, sub)

                submod = cls.import_module(sub, subname, m)

                if not submod:
                    return cls.unknown_module(sub, subname, m, True)


    @classmethod
    def import_module(cls, partname, fqname, parent):
        try:
            return sys.modules[fqname]
        except KeyError:
            pass

        try:
            fp, pathname, stuff = imp.find_module(partname, parent.__path__ if hasattr(parent, '__path__') else None)
        except ImportError:
            return None

        try:
            m = imp.load_module(fqname, fp, pathname, stuff)
        finally:
            if fp:
                fp.close()

        if parent:
            setattr(parent, partname, m)

        return m


    @classmethod
    def install(cls):
        ImportHook.original_import = builtins.__import__
        builtins.__import__ = cls.import_hook


    @classmethod
    def uninstall(cls):
        builtins.__import__ = ImportHook.original_import
