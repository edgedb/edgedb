import types

def _default_merge_func(left, right):
    if right is None:
        return left

    if left is None:
        return right

    if isinstance(left, list):
        if isinstance(right, list):
            return left + right
        else:
            raise TypeError('cannot merge %s and %s' % (type(left), type(right)))
    elif isinstance(left, set):
        if isinstance(right, set):
            return left | right
        else:
            raise TypeError('cannot merge %s and %s' % (type(left), type(right)))

    return right

def merge_dicts(left_dict, right_dict, merge_function=_default_merge_func):
    """merge two dictionaries, returning the combination.
       recursively operates on dicts within dicts.

       You must supply a function that accepts two parameters and returns a
       conceptually 'merged' value.

       All type checking must be done by the merge_function you supply.
    """

    return_dict = right_dict.copy()

    # check that we actually have a function
    if type(merge_function) != types.FunctionType:
        raise TypeError("The merge_function supplied was not a valid function.")

    for left_key in left_dict:
        if left_key in right_dict:

            # cache the values
            left_value = left_dict[left_key]
            right_value = right_dict[left_key]

            # recurse on dictionaries
            if type(left_value) == dict:
                if right_value is not None:
                    return_dict[left_key] = merge_dicts(left_value, right_value, merge_function)
                else:
                    return_dict[left_key] = left_value
                continue

            # apply the merge function
            return_dict[left_key] = merge_function(left_value, right_value)

        else:
            return_dict[left_key] = left_dict[left_key]

    for right_key in right_dict:
        if right_key not in left_dict:
            return_dict[right_key] = right_dict[right_key]

    return return_dict
