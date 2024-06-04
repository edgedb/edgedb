.. _ref_guide_jupyter_notebook:

================
Jupyter Notebook
================

:edb-alt-title: Using EdgeDB with Jupyter Notebook

1. `Install Jupyter Notebook
   <https://docs.jupyter.org/en/latest/install/notebook-classic.html>`__

2. Install the EdgeDB Python library with ``pip install edgedb``

3. Set the appropriate `connection environment variables
   <https://docs.edgedb.com/database/reference/connection>`__ required for your
   EdgeDB instance

   **For EdgeDB Cloud instances**

   - ``EDGEDB_INSTANCE``- your instance name (``<org-name>/<instance-name>``)
   - ``EDGEDB_SECRET_KEY``- a secret key with permissions for the selected instance.

     .. note::

         You may create a secret key with the CLI by running ``edgedb cloud
         secretkey create`` or in the `EdgeDB Cloud UI
         <https://cloud.edgedb.com/>`__.

   **For other remote instances**

   - ``EDGEDB_DSN``- the DSN of your remote instance

     .. note::

        DSNs take the following format:
        ``edgedb://<username>:<password>@<hostname-or-ip>:<port>/<branch>``.
        Omit any segment, and EdgeDB will fall back to a default value listed
        in `our DSN specification
        <https://docs.edgedb.com/database/reference/dsn#ref-dsn>`__

   **For local EdgeDB instances**

   - ``EDGEDB_INSTANCE``- your instance name
   - ``EDGEDB_USER`` & ``EDGEDB_PASSWORD``

   .. note :: Usernames and passwords

      EdgeDB creates an ``edgedb`` user by default, but the password is
      randomized. You may set the password for this role by running ``alter
      role edgedb { set password := '<password>'; };`` or you may create a new
      role using ``create superuser role <name> { set password := '<password>';
      };``.

4. Start your notebook by running ``jupyter notebook``. Make sure this process
   runs in the same environment that contains the variables you set in step 3.

5. Create a new notebook.

6. In one of your notebook's blocks, import the EdgeDB library and run a query.

   .. code-block:: python

      import edgedb

      client = edgedb.create_client()

      def main():
          query = "SELECT 1 + 1;" # Swap in any query you want
          result = client.query(query)
          print(result[0])

      main()

      client.close()
