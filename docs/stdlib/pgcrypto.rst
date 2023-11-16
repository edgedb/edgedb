.. versionadded:: 4.0

.. _ref_ext_pgcrypto:

=============
ext::pgcrypto
=============

This extension provides tools for your hashing and encrypting needs.

The Postgres that comes packaged with the EdgeDB 4.0+ server includes
``pgcrypto``, as does EdgeDB Cloud. It you are using a separate
Postgres backend, you will need to arrange for it to be installed.

To activate this functionality you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension pgcrypto;

That will give you access to the ``ext::pgcrypto`` module where you may find
the following functions:

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::pgcrypto::digest`
      - :eql:func-desc:`ext::pgcrypto::digest`

    * - :eql:func:`ext::pgcrypto::hmac`
      - :eql:func-desc:`ext::pgcrypto::hmac`

    * - :eql:func:`ext::pgcrypto::gen_salt`
      - :eql:func-desc:`ext::pgcrypto::gen_salt`

    * - :eql:func:`ext::pgcrypto::crypt`
      - :eql:func-desc:`ext::pgcrypto::crypt`


------------


.. eql:function:: ext::pgcrypto::digest(data: str, type: str) -> bytes
                  ext::pgcrypto::digest(data: bytes, type: str) -> bytes

    Computes a hash of the *data* using the specified algorithm.

    The *data* may come as a :eql:type:`str` or :eql:type:`bytes`. The value
    of *type* argument determines the hashing algorithm that will be used.
    Valid algorithms are: ``md5``, ``sha1``, ``sha224``, ``sha256``,
    ``sha384`` and ``sha512``. Also, any digest algorithm OpenSSL supports is
    automatically picked up as well.

    The result is always a binary hash.

    .. code-block:: edgeql-repl

      db> select ext::pgcrypto::digest('encrypt this', 'sha1');
      {b'\x05\x82\xd8YLF\xe7\xd4\x12\x91\n\xdb$\xf1!v\xf9\xd4\x89\xc4'}
      db> select ext::pgcrypto::digest(b'encrypt this', 'md5');
      {b'\x15\xd6\x14y\xcb\xf2"\xa1+Z]8\xf8\xcf\x0c['}


------------


.. eql:function:: ext::pgcrypto::hmac(data: str, key: str, type: str) \
                    -> bytes
                  ext::pgcrypto::hmac(data: bytes, key: bytes, type: str) \
                    -> bytes

    Computes a hashed MAC for *data* using *key* and the specified algorithm.

    The *data* may come as a :eql:type:`str` or :eql:type:`bytes`. The *key*
    type must match the *data* type. The value of *type* argument determines
    the hashing algorithm that will be used. Valid algorithms are: ``md5``,
    ``sha1``, ``sha224``, ``sha256``, ``sha384`` and ``sha512``. Also, any
    digest algorithm OpenSSL supports is automatically picked up as well.

    The result is always a binary hash.

    The main difference between :eql:func:`ext::pgcrypto::digest` and this
    function is that it's impossible to recalculate the hash without the key.

    .. code-block:: edgeql-repl

      db> select ext::pgcrypto::hmac('encrypt this', 'my key', 'sha1');
      {b'\x01G\x12\xb7\xe76H\x8b\xa4T1\x0fj\x87\xdf\x86n\x8f\xed\x15'}
      db> select ext::pgcrypto::hmac(b'encrypt this', b'my key', 'md5');
      {b'\xa9{\xc7\x9e\xc9"7e\xab\x83\xeb\x0c\xde\x02Nn'}


------------


.. eql:function:: ext::pgcrypto::gen_salt() -> str
                  ext::pgcrypto::gen_salt(type: str) -> str
                  ext::pgcrypto::gen_salt(type: str, iter_count: int64) -> str

    Generates a new random salt string.

    When generating the salt string *type* may be specified. Valid salt types
    are: ``des``, ``xdes``, ``md5``, and ``bf`` (default).

    .. code-block:: edgeql-repl

      db> select ext::pgcrypto::gen_salt();
      {'$2a$06$5D2rBj3UY5/UYvPIUNILvu'}
      db> select ext::pgcrypto::gen_salt('des');
      {'o9'}
      db> select ext::pgcrypto::gen_salt('xdes');
      {'_J9..efC8'}

    The *iter_count* specifies the number of iterations for algorithms that
    allow iterations (``xdes`` and ``bf``). The ``xdes`` algorithm has an
    additional requirement that *iter_count* must be odd. The higher the
    iteration count the longer it takes to compute the hash and therefore it
    also takes longer to break the encryption. However, if the count is too
    high, it can take impractically long.

    .. code-block:: edgeql-repl

      db> select ext::pgcrypto::gen_salt('bf', 10);
      {'$2a$10$fAQS9/UKS42OI.ftjHkj2O'}
      db> select ext::pgcrypto::gen_salt('xdes', 5);
      {'_3...oN2c'}


------------


.. eql:function:: ext::pgcrypto::crypt(password: str, salt: str) -> str

    Calculates a crypt(3)-style hash of password.

    Typically you would use :eql:func:`ext::pgcrypto::gen_salt` to generate a
    salt value for a new password:

    .. code-block:: edgeql-repl

      db> with module ext::pgcrypto
      ... select crypt('new password', gen_salt('des'));
      {'0ddkJUiOnUFq6'}

    To check the password against a stored encrypted value use the hash value
    itself as salt and see if the result matches:

    .. code-block:: edgeql-repl

      db> with hash := '0ddkJUiOnUFq6'
      ... select hash = ext::pgcrypto::crypt(
      ...   'new password',
      ...   hash,
      ... );
      {true}