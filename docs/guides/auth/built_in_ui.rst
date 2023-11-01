.. _ref_guide_auth_built_in_ui:

===========
Built-in UI
===========

:edb-alt-title: Integrating EdgeDB Auth's built-in UI

To use the built-in UI for EdgeDB Auth, enable the built-in Auth UI by clicking
the "Enable UI" button under "Login UI" in the configuration section of the
EdgeDB UI. Set these configuration values:

-  ``redirect_to``- Once the authentication flow is complete, EdgeDB will
   redirect the user’s browser back to this URL in your application’s
   backend.
-  ``redirect_to_on_signup``- If this is a new user, EdgeDB will redirect
   the user’s browser back to this URL in your application’s backend.
-  ``app_name``- Used in the built-in UI to show the user the
   application’s name in a few important places.
-  ``logo_url``- If provided, will show in the built-in UI as part of the
   page design.
-  ``dark_logo_url``- If provided and the user’s system has indicated
   that they prefer a dark UI, this will show instead of ``logo_url`` in
   the built-in UI as part of the page design.
-  ``brand_color``- If provided, used in the built-in UI as part of the
   page design.


Configuring
===========

We secure authentication tokens and other sensitive data by using PKCE
(Proof Key of Code Exchange).

1. Your application server should create a 32-byte base64 URL encoded
   string (which will be 43-44 bytes after encoding), which we will call
   the ``verifier``. You need to store this value for the duration of
   the flow. One way to accomplish this bit of state is to use an
   HttpOnly cookie when the browser makes a request to the server for
   this value, which you can then use to retrieve it from the cookie
   store at the end of the flow.

2. Take this ``verifier`` string, and then hash it with SHA256, and then
   base64url encode the resulting string. This new string is called the
   ``challenge``.

   .. code-block:: tsx

      import crypto from "node:crypto";

      function initiatePKCE() {
      	const verifier = crypto
          .randomBytes(32)
          .toString("base64url");

      	cookies().set("edgedb_pkce_verifier", verifier);
      	const challenge = crypto
      	  .createHash("sha256")
      	  .update(verifier)
      	  .digest("base64url");

        return challenge;
      }

3. Once you’ve generated the ``verifier`` and ``challenge``, you should
   redirect or link the user to the following url:

   .. code-block::

      {edgedb_host}[:port]/db/{db_name}/ext/auth/ui/signin?challenge={challenge}

4. At the very end of the flow, the EdgeDB server will redirect the
   user’s browser to the ``redirect_to`` address with a single query
   parameter: ``code``. This route should be a server route that has
   access to the ``verifier``.

5. Next, you take that ``code`` and the ``verifier`` you stored in step
   1, and make the following request to EdgeDB:

   .. code-block::

      GET {edgedb_host}[:port]/db/{db_name}/ext/auth/token?code={code}&verifier={verifier}

   -  ``code`` the ``code`` from the query string that you were passed
   -  ``verifier`` the randomly generated string you made in step 1.

   The EdgeDB server will then respond with a JSON body that includes
   ``auth_token`` and ``identity_id`` properties.

