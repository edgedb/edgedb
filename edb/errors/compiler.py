from edb import errors
from edb import schema


class BadTypeObject(errors.InvalidReferenceError):
    def __init__(self, obj_id, obj_type, target_type, obj_name=None):
        self.obj_id = obj_id
        self.obj_type = obj_type.get_schema_class_displayname()
        self.obj_name = obj_name
        self.target_type = target_type.get_schema_class_displayname()
        super().__init__(
            f'schema object {obj_id!r} exists, but is not '
            f'{target_type.get_schema_class_displayname()}')

    def update(self, obj_name=None, statement=None):
        self.obj_name = obj_name or self.obj_name

        if statement:
            # Time to finalize our error, with potentially new details
            if self.obj_name:
                message = (
                    f'schema object `{self.obj_name}` '
                    f'(uuid: {self.obj_id}) exists, but is not '
                    f'{self.target_type}'
                )
            else:
                message = (
                    f'schema object {self.obj_id!r} exists, but is not '
                    f'{self.target_type}'
                )

            # specific error hints go here
            if isinstance(statement, schema.objtypes.CreateObjectType):
                if self.obj_type == 'scalar type':
                    return errors.InvalidReferenceError(
                        message,
                        hint=f'name refers to the {self.obj_type}',
                        details=f'consider using CREATE SCALAR TYPE',
                    )

            # if nothing else, just convert to regular error
            return errors.InvalidReferenceError(message)

        return self

    def __reduce__(self):
        # This error is internal and should be converted into
        # InvalidReferenceError on process boundary
        return (errors.InvalidReferenceError, self.args)
