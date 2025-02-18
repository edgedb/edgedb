.. _ref_guide_jupyter_notebook:

================
Jupyter Notebook
================

:edb-alt-title: Using Gel with Jupyter Notebook

1. `Install Jupyter Notebook
   <https://docs.jupyter.org/en/latest/install/notebook-classic.html>`__

2. Install the Gel Python library with ``pip install gel``

3. Set the appropriate :ref:`connection environment variables
   <ref_reference_connection>` required for your
   Gel instance

   **For Gel Cloud instances**

   - :gelenv:`INSTANCE`- your instance name (``<org-name>/<instance-name>``)
   - :gelenv:`SECRET_KEY`- a secret key with permissions for the selected instance.

     .. note::

         You may create a secret key with the CLI by running :gelcmd:`cloud
         secretkey create` or in the `Gel Cloud UI
         <https://cloud.geldata.com/>`__.

   **For other remote instances**

   - :gelenv:`DSN`- the DSN of your remote instance

     .. note::

        DSNs take the following format:
        :geluri:`<username>:<password>@<hostname-or-ip>:<port>/<branch>`.
        Omit any segment, and Gel will fall back to a default value listed
        in :ref:`our DSN specification <ref_dsn>`

   **For local Gel instances**

   - :gelenv:`INSTANCE`- your instance name
   - :gelenv:`USER` & :gelenv:`PASSWORD`

   .. note :: Usernames and passwords

      Gel creates an |admin| user by default, but the password is
      randomized. You may set the password for this role by running ``alter
      role admin { set password := '<password>'; };`` or you may create a new
      role using ``create superuser role <name> { set password := '<password>';
      };``.

4. Start your notebook by running ``jupyter notebook``. Make sure this process
   runs in the same environment that contains the variables you set in step 3.

5. Create a new notebook.

6. In one of your notebook's blocks, import the Gel library and run a query.

   .. code-block:: python

      import gel

      client = gel.create_client()

      def main():
          query = "SELECT 1 + 1;" # Swap in any query you want
          result = client.query(query)
          print(result[0])

      main()

      client.close()
