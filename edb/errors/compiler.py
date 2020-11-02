from edb import errors
from edb import schema


class BadTypeObject(errors.InvalidReferenceError):
    def __init__(self, obj_id, obj_type, target_type,
                 obj_name=None, context=None):
        self._obj_id = obj_id
        self._obj_type = obj_type.get_schema_class_displayname()
        self._obj_name = obj_name
        self._target_type = target_type.get_schema_class_displayname()
        self._context = context
        super().__init__(
            f'schema object {obj_id!r} exists, but is not '
            f'{target_type.get_schema_class_displayname()}')

    def update(self, obj_name=None, source_context=None, statement=None):
        self._obj_name = obj_name or self._obj_name
        if source_context:
            self._context = source_context
            self.set_source_context(source_context)

        if statement:
            # Time to finalize our error, with potentially new details
            if self._obj_name:
                message = (
                    f'schema object `{self._obj_name}` '
                    f'(uuid: {self._obj_id}) exists, but is not '
                    f'{self._target_type}'
                )
            else:
                message = (
                    f'schema object {self._obj_id!r} exists, but is not '
                    f'{self._target_type}'
                )

            # specific error hints go here
            if isinstance(statement, schema.objtypes.CreateObjectType):
                if self._obj_type == 'scalar type':
                    return errors.InvalidReferenceError(
                        message,
                        hint=f'name refers to the {self._obj_type}',
                        details=f'consider using CREATE SCALAR TYPE',
                        context=self._context,
                    )

            # if nothing else, just convert to regular error
            return errors.InvalidReferenceError(message,
                                                context=self._context)

        return self

    def __reduce__(self):
        # This error is internal and should be converted into
        # InvalidReferenceError on process boundary
        return (errors.InvalidReferenceError, self.args)
