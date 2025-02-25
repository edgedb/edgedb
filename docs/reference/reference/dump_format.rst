Dump file format
================

This description uses the same :ref:`conventions <ref_protocol_conventions>`
as the protocol description.


General Structure
-----------------

Dump file is structure as follows:

1. Dump file format marker ``\xFF\xD8\x00\x00\xD8EDGEDB\x00DUMP\x00``
   (17 bytes)
2. Format version number ``\x00\x00\x00\x00\x00\x00\x00\x01`` (8 bytes)
3. Header block
4. Any number of data blocks


General Dump Block
------------------

Both header and data blocks are formatted as follows:

.. code-block:: c

    struct DumpHeader {
        int8            mtype;

        // SHA1 hash sum of block data
        byte            sha1sum[20];

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // Block data. Should be treated in opaque way by a client.
        byte            data[message_length];
    }

Upon receiving a protocol dump data message, the dump client should:

* Replace packet type:
    * ``@`` (0x40) → ``H`` (0x48)
    * ``=`` (0x3d) → ``D`` (0x44)
* Prepend SHA1 checksum to the block
* Append the entire dump protocol message disregarding the
  first byte (the message type).


Header Block
------------

Format:

.. code-block:: c

    struct DumpHeader {
        // Message type ('H')
        int8            mtype = 0x48;

        // SHA1 hash sum of block data
        byte            sha1sum[20];

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // A set of message headers.
        Headers         headers;

        // Protocol version of the dump
        int16           major_ver;
        int16           minor_ver;

        // Schema data
        string          schema_ddl;

        // Type identifiers
        int32           num_types;
        TypeInfo        types[num_types];

        // Object descriptors
        int32           num_descriptors;
        ObjectDesc      descriptors[num_descriptors]
    };

    struct TypeInfo {
        string          type_name;
        string          type_class;
        byte            type_id[16];
    }

    struct ObjectDesc {
        byte            object_id[16];
        bytes           description;

        int16           num_dependencies;
        byte            dependency_id[num_dependencies][16];
    }

Known headers:

* 101 ``BLOCK_TYPE`` -- block type, always "I"
* 102 ``SERVER_TIME`` -- server time when dump is started as a floating point
  unix timestamp stringified
* 103 ``SERVER_VERSION`` -- full version of server as string
* 105 ``SERVER_CATALOG_VERSION`` -- the catalog version of the server, as
  a 64-bit integer. The catalog version is an identifier that is incremented
  whenever a change is made to the database layout or standard library.


Data Block
----------

Format:

.. code-block:: c

    struct DumpBlock {
        // Message type ('=')
        int8            mtype = 0x3d;

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // A set of message headers.
        Headers         headers;
    }

Known headers:

* 101 ``BLOCK_TYPE`` -- block type, always "D"
* 110 ``BLOCK_ID`` -- block identifier (16 bytes of UUID)
* 111 ``BLOCK_NUM`` -- integer block index stringified
* 112 ``BLOCK_DATA`` -- the actual block data
