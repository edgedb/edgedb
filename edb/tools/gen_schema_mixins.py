import types
import typing
import textwrap
import enum

from edb import schema
from edb.schema import objects as s_objects
from edb.schema import abc as s_abc
from edb.common import typing_inspect

from edb.tools.edb import edbcommands


@edbcommands.command("gen-schema-mixins")
def main() -> None:

    for name, item in schema.__dict__.items():
        if not isinstance(item, types.ModuleType):
            continue

        gen_for_module(name, item)


def gen_for_module(mod_name: str, mod: types.ModuleType):

    schema_object_classes: typing.List[s_objects.Object] = []
    imports = set()
    has_fields = False

    for cls in mod.__dict__.values():
        if not (isinstance(cls, type) and issubclass(cls, s_objects.Object)):
            continue

        # fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        # fields = getattr(cls, fa)
        fields = cls._schema_fields

        if len(fields) > 0:
            has_fields = True

        for field in fields.values():
            imports.update(collect_imports(field.type, mod.__name__))

        schema_object_classes.append(cls)
    if not schema_object_classes:
        return

    f = open(f'edb/schema/generated/{mod_name}.py', 'w')

    f.write(
        textwrap.dedent(
            '''\
            # DO NOT EDIT. This file was generated with:
            #
            # $ edb gen-schema-mixins

            """Type definitions for generated methods on schema classes"""
            '''
        )
    )

    if has_fields:
        f.write(
            textwrap.dedent(
                '''\

                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from edb.schema import schema as s_schema
                '''
            )
        )

    for imp in imports:
        parts = imp.split('.')
        if len(parts) > 1:
            path = '.'.join(parts[0:-1])
            f.write(f'from {path} import {parts[-1]}\n')
        else:
            f.write(f'import {parts[-1]}\n')

    for cls in schema_object_classes:
        f.write(f'\n\nclass {cls.__name__}Mixin:\n')

        fields = cls._schema_fields

        for field in fields.values():
            fn = field.name

            ty = codegen_ty(field.type, mod.__name__)

            f.write(
                '\n'
                f'    def get_{fn}(\n'
                f'        self, schema: \'s_schema.Schema\'\n'
                f'    ) -> \'{ty}\':\n'
            )

            if issubclass(field.type, s_abc.Reducible):
                f.write(
                    f'        field = type(self).get_field(\'{fn}\')\n'
                    f'        data = schema.get_obj_data_raw(self)\n'
                    f'        v = data[{field.index}]\n'
                    f'        if v is not None:\n'
                    f'            return field.type.schema_restore(v)\n'
                    f'        else:\n'
                    f'            try:\n'
                    f'                return field.get_default()\n'
                    f'            except ValueError:\n'
                    f'                pass\n'
                    f'            from edb.schema import objects as s_obj\n'
                    f'            raise s_obj.FieldValueNotFoundError(\n'
                    f'                \'{cls.__name__} object has no value \'\n'
                    f'                \'for field `{fn}`\'\n'
                    f'            )\n'
                )
            elif field.default is s_objects.NoDefault:
                f.write(
                    f'        data = schema.get_obj_data_raw(self)\n'
                    f'        v = data[{field.index}]\n'
                    f'        if v is not None:\n'
                    f'            return v\n'
                    f'        else:\n'
                    f'            from edb.schema import objects as s_obj\n'
                    f'            raise s_obj.FieldValueNotFoundError(\n'
                    f'                \'{cls.__name__} object has no value \'\n'
                    f'                \'for field `{fn}`\'\n'
                    f'            )\n'
                )
            elif field.default is s_objects.DEFAULT_CONSTRUCTOR:
                f.write(
                    f'        data = schema.get_obj_data_raw(self)\n'
                    f'        v = data[{field.index}]\n'
                    f'        if v is not None:\n'
                    f'            return v\n'
                    f'        else:\n'
                    f'            field = type(self).get_field(\'{fn}\')\n'
                    f'            return field.get_default()\n'
                )
            else:
                f.write(
                    f'        data = schema.get_obj_data_raw(self)\n'
                    f'        v = data[{field.index}]\n'
                    f'        if v is not None:\n'
                    f'            return v\n'
                    f'        else:\n'
                    f'            return {codegen_const(field.default, mod)}\n'
                )

        if len(fields) == 0:
            f.write('    pass\n')


def codegen_const(value: typing.Any, mod: types.ModuleType) -> str:
    if value is None:
        return 'None'
    elif isinstance(value, enum.Enum):
        def_ty = codegen_ty(type(value), mod.__name__)
        return f"{def_ty}.{value}"
    elif isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, (bool, str, int)):
        return str(value)
    else:
        raise NotImplementedError(f'default type: {type(value)}')


def collect_imports(ty: type, current_module: str) -> typing.Set[str]:
    r = set()

    if isinstance(ty, str):
        r.add(current_module)
        return r

    if not isinstance(ty, type):
        return r

    if ty.__module__ == 'builtins':
        return r
    if is_generic(ty):
        r.add(ty.__base__.__module__)
        for arg in ty.orig_args:
            r.update(collect_imports(arg, current_module))
        return r
    r.add(ty.__module__)
    return r


def codegen_ty(ty: type, current_module: str):
    if isinstance(ty, str):
        mod_name = current_module.split('.')[-1]
        return f"{mod_name}.{ty}"

    if not isinstance(ty, type):
        return f'\'{ty}\''

    if ty.__module__ == 'builtins':
        return ty.__qualname__

    if is_generic(ty):
        mod_name = ty.__base__.__module__.split('.')[-1]
        base_name = ty.__base__.__qualname__
        base_name = base_name.split('[')[0]
        base = f"{mod_name}.{base_name}"

        args = ', '.join(
            (codegen_ty(arg, current_module) for arg in ty.orig_args)
        )
        return f'{base}[{args}]'

    # base case
    mod_name = ty.__module__.split('.')[-1]
    return f"{mod_name}.{ty.__qualname__}"


def is_generic(ty: type) -> bool:
    return ty.__name__ not in {
        'FuncParameterList',
        'ExpressionList',
    } and typing_inspect.is_generic_type(ty)
