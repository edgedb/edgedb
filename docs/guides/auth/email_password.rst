.. _ref_guide_auth_email_password:

==================
Email and password
==================

:edb-alt-title: Integrating EdgeDB Auth's email and password provider

Along with using the :ref:`built-in UI <ref_guide_auth_built_in_ui>`, you can also
create your own UI that calls to your own web application backend.

UI considerations
=================

Similar to how the built-in UI works, you can query the database configuration
to discover which providers are configured and dynamically build the UI.

.. code-block:: edgeql

  select cfg::Config.extensions[is ext::auth::AuthConfig].providers {
      name,
      [is ext::auth::OAuthProviderConfig].display_name,
  };

The ``name`` is a unique string that identifies the Identity Provider. OAuth
providers also have a ``display_name`` that you can use as a label for links or
buttons. In later steps, you'll be providing this ``name`` as the ``provider``
in various endpoints.


Example implementation
======================

We will demonstrate the various steps below by building a NodeJS HTTP server in
a single file that we will use to simulate a typical web application. For this example,
we will require email verification to demonstrate the full flow, but you can
configure your provider to not require verification by setting the
``require_verification`` setting to ``false``.

.. note::

    The details below show the inner workings of how data is exchanged with the
    Auth extension from a web app using HTTP. You can use this as a guide to
    integrate with your application written in any language that can send and
    receive HTTP requests.


Start the PKCE flow
-------------------

We secure authentication tokens and other sensitive data by using PKCE
(Proof Key of Code Exchange).

Your application server creates a 32-byte Base64 URL-encoded string (which will
be 43 bytes after encoding), called the ``verifier``. You need to store this
value for the duration of the flow. One way to accomplish this bit of state is
to use an HttpOnly cookie when the browser makes a request to the server for
this value, which you can then use to retrieve it from the cookie store at the
end of the flow. Take this ``verifier`` string, hash it with SHA256, and then
base64url encode the resulting string. This new string is called the
``challenge``.

.. note::

   Since ``=`` is not a URL-safe character, if your Base64-URL encoding
   function adds padding, you should remove the padding before hashing the
   ``verifier`` to derive the ``challenge`` or when providing the ``verifier``
   or ``challenge`` in your requests.

.. note::

   If you are familiar with PKCE, you will notice some differences from how RFC
   7636 defines PKCE. Our authentication flow is not an OAuth flow, but rather a
   strict server-to-server flow with Proof Key of Code Exchange added for
   additional security to avoid leaking the authentication token. Here are some
   differences between PKCE as defined in RFC 7636 and our implementation:

   - We do not support the ``plain`` value for ``code_challenge_method``, and
     therefore do not read that value if provided in requests.
   - Our parameters omit the ``code_`` prefix, however we do support
     ``code_challenge`` and ``code_verifier`` as aliases, preferring
     ``challenge`` and ``verifier`` if present.

.. lint-off

.. code-block:: javascript

   import http from "node:http";
   import { URL } from "node:url";
   import crypto from "node:crypto";

   /**
    * You can get this value by running `edgedb instance credentials`.
    * Value should be:
    * `${protocol}://${host}:${port}/branch/${branch}/ext/auth/
    */
   const EDGEDB_AUTH_BASE_URL = process.env.EDGEDB_AUTH_BASE_URL;
   const SERVER_PORT = 3000;

   /**
    * Generate a random Base64 url-encoded string, and derive a "challenge"
    * string from that string to use as proof that the request for a token
    * later is made from the same user agent that made the original request
    *
    * @returns {Object} The verifier and challenge strings
    */
   const generatePKCE = () => {
      const verifier = crypto.randomBytes(32).toString("base64url");

      const challenge = crypto
         .createHash("sha256")
         .update(verifier)
         .digest("base64url");

      return { verifier, challenge };
   };

.. lint-on

.. note::

    For EdgeDB versions before 5.0, the value for ``EDGEDB_AUTH_BASE_URL``
    in the above snippet should have the form:

    ``${protocol}://${host}:${port}/db/${database}/ext/auth/``


Sign-in and sign-up
-------------------

Next, we implement routes that handle registering a new user and authenticating
an existing user.

.. lint-off

.. code-block:: javascript

   const server = http.createServer(async (req, res) => {
     const requestUrl = getRequestUrl(req);

     switch (requestUrl.pathname) {
       case "/auth/signup": {
         await handleSignUp(req, res);
         break;
       }

       case "/auth/signin": {
         await handleSignIn(req, res);
         break;
       }

       case "/auth/verify": {
         await handleVerify(req, res);
         break;
       }

       case "/auth/send-password-reset-email": {
         await handleSendPasswordResetEmail(req, res);
         break;
       }

       case "/auth/ui/reset-password": {
         await handleUiResetPassword(req, res);
         break;
       }

       case "/auth/reset-password": {
         await handleResetPassword(req, res);
         break;
       }

       default: {
         res.writeHead(404);
         res.end("Not found");
         break;
       }
     }
   });

   /**
    * Handles sign up with email and password.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleSignUp = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const pkce = generatePKCE();
       const { email, password, provider } = JSON.parse(body);
       if (!email || !password || !provider) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'email', 'password', and 'provider' keys, but got: ${body}`,
         );
         return;
       }

       const registerUrl = new URL("register", EDGEDB_AUTH_BASE_URL);
       const registerResponse = await fetch(registerUrl.href, {
         method: "post",
         headers: {
           "Content-Type": "application/json",
         },
         body: JSON.stringify({
           challenge: pkce.challenge,
           email,
           password,
           provider,
           verify_url: `http://localhost:${SERVER_PORT}/auth/verify`,
         }),
       });

       if (!registerResponse.ok) {
         const text = await registerResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       const registerJson = await registerResponse.json();

       if ("code" in registerJson) {
         // No verification required, we can immediately get an auth token
         const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
         tokenUrl.searchParams.set("code", registerJson.code);
         tokenUrl.searchParams.set("verifier", pkce.verifier);
         const tokenResponse = await fetch(tokenUrl.href, {
           method: "get",
         });

         if (!tokenResponse.ok) {
           const text = await tokenResponse.text();
           res.status = 400;
           res.end(`Error from the auth server: ${text}`);
           return;
         }

         const { auth_token } = await tokenResponse.json();
         res.writeHead(204, {
           "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
         });

         res.end();
       } else {
         // Verification required, we need to render a notice to the user
         // to check their email for a verification link
         res.writeHead(200, { "Content-Type": "text/html" });
         res.end(`
           <html>
             <body>
               <p>Please check your email for a verification link.</p>
             </body>
           </html>
         `);
       }
     });
   };

   /**
    * Handles sign in with email and password.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleSignIn = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const pkce = generatePKCE();
       const { email, password, provider } = JSON.parse(body);
       if (!email || !password || !provider) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'email', 'password', and 'provider' keys, but got: ${body}`,
         );
         return;
       }

       const authenticateUrl = new URL("authenticate", EDGEDB_AUTH_BASE_URL);
       const authenticateResponse = await fetch(authenticateUrl.href, {
         method: "post",
         headers: {
           "Content-Type": "application/json",
         },
         body: JSON.stringify({
           challenge: pkce.challenge,
           email,
           password,
           provider,
         }),
       });

       if (!authenticateResponse.ok) {
         const text = await authenticateResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       const authenticateJson = await authenticateResponse.json();

       if ("code" in authenticateJson) {
         // User is verified, we can get an auth token
         const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
         tokenUrl.searchParams.set("code", authenticateJson.code);
         tokenUrl.searchParams.set("verifier", pkce.verifier);
         const tokenResponse = await fetch(tokenUrl.href, {
           method: "get",
         });

         if (!tokenResponse.ok) {
           const text = await tokenResponse.text();
           res.status = 400;
           res.end(`Error from the auth server: ${text}`);
           return;
         }

         const { auth_token } = await tokenResponse.json();
         res.writeHead(204, {
           "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
         });
         res.end();
       } else {
         // Verification required, we need to render a notice to the user
         // to check their email for a verification link
         res.writeHead(200, { "Content-Type": "text/html" });
         res.end(`
           <html>
             <body>
               <p>Please check your email for a verification link.</p>
             </body>
           </html>
         `);
       }
     });
   };

.. lint-on


Email verification
------------------

When a new user signs up, by default we require them to verify their email
address before allowing the application to get an authentication token. To
handle the verification flow, we implement an endpoint:

.. note::

   💡 If you would like to allow users to still log in, but offer limited access
   to your application, you can check the associated
   ``ext::auth::EmailPasswordFactor`` for the ``ext::auth::Identity`` to see if
   the ``verified_at`` property is some time in the past. You'll need to set
   the ``require_verification`` setting in the provider configuration to
   ``false``.

.. lint-off

.. code-block:: javascript

   /**
    * Handles the link in the email verification flow.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleVerify = async (req, res) => {
     const requestUrl = getRequestUrl(req);
     const verification_token = requestUrl.searchParams.get("verification_token");
     if (!verification_token) {
       res.status = 400;
       res.end(
         `Verify request is missing 'verification_token' search param. The verification email is malformed.`,
       );
       return;
     }

     const verifyUrl = new URL("verify", EDGEDB_AUTH_BASE_URL);
     const verifyResponse = await fetch(verifyUrl.href, {
       method: "post",
       headers: {
         "Content-Type": "application/json",
       },
       body: JSON.stringify({
         verification_token,
         provider: "builtin::local_emailpassword",
       }),
     });

     if (!verifyResponse.ok) {
       const text = await verifyResponse.text();
       res.status = 400;
       res.end(`Error from the auth server: ${text}`);
       return;
     }

     const { code } = await verifyResponse.json();

     const cookies = req.headers.cookie?.split("; ");
     const verifier = cookies
       ?.find((cookie) => cookie.startsWith("edgedb-pkce-verifier="))
       ?.split("=")[1];
     if (verifier) {
       // Email verification flow is continuing from the original
       // user agent/browser, so we can immediately get an auth token
       const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
       tokenUrl.searchParams.set("code", code);
       tokenUrl.searchParams.set("verifier", verifier);
       const tokenResponse = await fetch(tokenUrl.href, {
         method: "get",
       });

       if (!tokenResponse.ok) {
         const text = await tokenResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       const { auth_token } = await tokenResponse.json();
       res.writeHead(204, {
         "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
       });
       res.end();
       return;
     }

     // Email verification flow is continuing from a different user agent/browser,
     // so we need to render a notice to the user to sign in, which will either
     // complete the PKCE flow or start a new one
     res.status = 200;
     res.end(
       `
       <html>
         <body>
           <p>Email verified! Please sign in to continue.</p>
         </body>
       </html>`,
     );
   };

.. lint-on


Create a User object
--------------------

For some applications, you may want to create a custom ``User`` type in the
default module to attach application-specific information. You can tie this to
an ``ext::auth::Identity`` by using the ``identity_id`` returned during the
sign-up flow.

.. note::

    For this example, we'll assume you have a one-to-one relationship between
    ``User`` objects and ``ext::auth::Identity`` objects. In your own
    application, you may instead decide to have a one-to-many relationship.

Given this ``User`` type:

.. code-block:: sdl

   type User {
       email: str;
       name: str;

       required identity: ext::auth::Identity {
           constraint exclusive;
       };
   }

You can update the ``handleRegister`` function like this to create a new ``User``
object:

.. lint-off

.. code-block:: javascript-diff

     const registerJson = await registerResponse.json();

   + if ("identity_id" in registerJson) {
   +   await client.query(`
   +     with
   +       identity := <ext::auth::Identity><uuid>$identity_id,
   +       emailFactor := (
   +         select ext::auth::EmailFactor filter .identity = identity
   +       ),
   +     insert User {
   +       email := emailFactor.email,
   +       identity := identity
   +     };
   +   `, { identity_id: registerJson.identity_id });
   + }
   +
     if ("code" in registerJson) {

.. lint-on


Password reset
--------------

To allow users to reset their password, we implement three endpoints. The first
one sends the reset email. The second is the HTML form that is rendered when
the user follows the link in their email. And, the final one is the endpoint
that updates the password and logs in the user.

.. lint-off

.. code-block:: javascript

   /**
    * Request a password reset for an email.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleSendPasswordResetEmail = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const { email } = JSON.parse(body);
       const reset_url = `http://localhost:${SERVER_PORT}/auth/ui/reset-password`;
       const provider = "builtin::local_emailpassword";
       const pkce = generatePKCE();

       const sendResetUrl = new URL("send-reset-email", EDGEDB_AUTH_BASE_URL);
       const sendResetResponse = await fetch(sendResetUrl.href, {
         method: "post",
         headers: {
           "Content-Type": "application/json",
         },
         body: JSON.stringify({
           email,
           provider,
           reset_url,
           challenge: pkce.challenge,
         }),
       });

       if (!sendResetResponse.ok) {
         const text = await sendResetResponse.text();
         res.status = 400;
         res.end(`Error from auth server: ${text}`);
         return;
       }

       const { email_sent } = await sendResetResponse.json();

       res.writeHead(200, {
         "Set-Cookie": `edgedb-pkce-verifier=${pkce.verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
       });
       res.end(`Reset email sent to '${email_sent}'`);
     });
   };

   /**
    * Render a simple reset password UI
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleUiResetPassword = async (req, res) => {
     const url = new URL(req.url);
     const reset_token = url.searchParams.get("reset_token");
     res.writeHead(200, { "Content-Type": "text/html" });
     res.end(`
       <html>
         <body>
           <form method="POST" action="http://localhost:${SERVER_PORT}/auth/reset-password">
             <input type="hidden" name="reset_token" value="${reset_token}">
             <label>
               New password:
               <input type="password" name="password" required>
             </label>
             <button type="submit">Reset Password</button>
           </form>
         </body>
       </html>
     `);
   };

   /**
    * Send new password with reset token to EdgeDB Auth.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleResetPassword = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const { reset_token, password } = JSON.parse(body);
       if (!reset_token || !password) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'reset_token' and 'password' keys, but got: ${body}`
         );
         return;
       }
       const provider = "builtin::local_emailpassword";
       const cookies = req.headers.cookie.split("; ");
       const verifier = cookies
         .find((cookie) => cookie.startsWith("edgedb-pkce-verifier="))
         .split("=")[1];
       if (!verifier) {
         res.status = 400;
         res.end(
           `Could not find 'verifier' in the cookie store. Is this the same user agent/browser that started the authorization flow?`
         );
         return;
       }
       const resetUrl = new URL("reset-password", EDGEDB_AUTH_BASE_URL);
       const resetResponse = await fetch(resetUrl.href, {
         method: "post",
         headers: {
           "Content-Type": "application/json",
         },
         body: JSON.stringify({
           reset_token,
           provider,
           password,
         }),
       });
       if (!resetResponse.ok) {
         const text = await resetResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }
       const { code } = await resetResponse.json();
       const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
       tokenUrl.searchParams.set("code", code);
       tokenUrl.searchParams.set("verifier", verifier);
       const tokenResponse = await fetch(tokenUrl.href, {
         method: "get",
       });
       if (!tokenResponse.ok) {
         const text = await tokenResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }
       const { auth_token } = await tokenResponse.json();
       res.writeHead(204, {
         "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
       });
       res.end();
     });
   };

.. lint-on

:ref:`Back to the EdgeDB Auth guide <ref_guide_auth>`
