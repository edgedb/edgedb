.. list-table::
    :class: funcoptable

    * - std::expression on (expr)
      - Custom constraint expression

    * - std::one_of(VARIADIC members: anytype)
      - A list of allowable values

    * - std::max_value(max: anytype)
      - Maximum value numerically/lexicographically

    * - std::max_ex_value(max: anytype)
      - Maximum value numerically/lexicographically (exclusive range)

    * - std::max_len_value(max: int64)
      - Maximum length (strings only)

    * - std::min_value(min: anytype)
      - Maximum value numerically/lexicographically

    * - std::min_ex_value(min: anytype)
      - Maximum value numerically/lexicographically (exclusive range)

    * - std::min_len_value(min: int64)
      - Minimum length (strings only)

    * - std::regexp(pattern: str)
      - Regex constraint (strings only)

    * - std::exclusive
      - Enforce uniqueness among all instances of the containing type
