.. _ref_guide_auth_webauthn:

========
WebAuthn
========

:edb-alt-title: Integrating EdgeDB Auth's WebAuthn provider

WebAuthn, short for Web Authentication, is a web standard published by the
World Wide Web Consortium (W3C) for secure and passwordless authentication on
the web. It allows users to log in using biometrics, mobile devices, or FIDO2
security keys instead of traditional passwords. This guide will walk you
through integrating WebAuthn authentication with your application using EdgeDB
Auth.

Why choose WebAuthn?
====================

WebAuthn provides a more secure and user-friendly alternative to passwords and
SMS-based OTPs. By leveraging public key cryptography, it significantly reduces
the risk of phishing, man-in-the-middle, and replay attacks. For application
developers, integrating WebAuthn can enhance security while improving the user
experience with seamless, passwordless logins.

What is a Passkey?
==================

While WebAuthn focuses on authenticating users through cryptographic
credentials, Passkeys extend this concept by enabling users to easily access
their credentials across devices, including those they haven't used before,
without the need for a password. Passkeys are built on the WebAuthn framework
and aim to simplify the user experience further by leveraging cloud
synchronization of credentials.

Many operating systems and password managers have added support for Passkeys,
making it easier for users to manage their credentials across devices. EdgeDB
Auth's WebAuthn provider supports Passkeys, allowing users to log in to your
application using their Passkeys.

Security considerations
=======================

For maximum flexibility, EdgeDB Auth's WebAuthn provider allows multiple
WebAuthn credentials per email. This means that it's very important to verify
the email before trusting a WebAuthn credential. This can be done by setting
the ``require_verification`` option to ``true`` (which is the default) in your
WebAuthn provider configuration. Or you can check the verification status of
the factor directly.

WebAuthn flow
=============

The WebAuthn authentication flow is a sophisticated process that involves a
coordinated effort between the server and the client-side script. Unlike the
other authentication methods outlined elsewhere in this guide, WebAuthn is a
coordinated flow that involves a client-side script access web browser APIs, the
Web Authentication API specifically, to interact with the user's authenticator
device or passkey.

At a high level, the sign-up ceremony involves the following steps:

1. The user initiates the sign-up process by providing their email address.
2. The server generates a JSON object that is used to configure the WebAuthn
   registration ceremony.
3. The client takes that JSON object, and using the Web Authentication API,
   interacts with the user's authenticator device to create a new credential.
4. The client sends the credential back to the server.
5. The server verifies the credential and associates it with the user's email
   address.

The sign-in ceremony is similar, but instead of creating a new credential, the
client uses the Web Authentication API to authenticate the user with an existing
credential.

Example implementation
======================

We will demonstrate the various steps below by building a NodeJS HTTP server in
a single file that we will use to simulate a typical web application.

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

Routing
-------

Let's set up the routes we will use to handle the WebAuthn flow. We will then
detail each route handler in the following sections.

.. lint-off

.. code-block:: javascript

   const server = http.createServer(async (req, res) => {
     const requestUrl = getRequestUrl(req);

     switch (requestUrl.pathname) {
       case "/auth/webauthn/register/options": {
         await handleRegisterOptions(req, res);
         break;
       }

       case "/auth/webauthn/register": {
         await handleRegister(req, res);
         break;
       }

       case "/auth/webauthn/authenticate/options": {
         await handleAuthenticateOptions(req, res);
         break;
       }

       case "/auth/webauthn/authenticate": {
         await handleAuthenticate(req, res);
         break;
       }

       case "/auth/webauthn/verify": {
         await handleVerify(req, res);
         break;
       }

       default: {
         res.writeHead(404);
         res.end("Not found");
         break;
       }
     }
   });

.. lint-on

Handle register and authenticate options
----------------------------------------

The first step in the WebAuthn flow is to get the options for registering a new
credential or authenticating an existing credential. The server generates a
JSON object that is used to configure the WebAuthn registration or
authentication ceremony. The EdgeDB Auth extension provides these endpoints
directly, so you can either proxy the request to the Auth extension or redirect
the user to the Auth extension's URL. We'll show the proxy option here.

.. lint-off

.. code-block:: javascript

   const handleRegisterOptions = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const { email } = JSON.parse(body);
       if (!email) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'email' key, but got: ${body}`,
         );
         return;
       }

       const registerUrl = new URL("webauthn/register/options", EDGEDB_AUTH_BASE_URL);
       registerUrl.searchParams.set("email", email);

       const registerResponse = await fetch(registerUrl.href);

       if (!registerResponse.ok) {
         const text = await registerResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       const registerData = await registerResponse.json();

       res.writeHead(200, { "Content-Type": "application/json" });
       res.end(JSON.stringify(registerData));
     });
   };

   const handleAuthenticateOptions = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const { email } = JSON.parse(body);
       if (!email) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'email' key, but got: ${body}`,
         );
         return;
       }

       const authenticateUrl = new URL("webauthn/authenticate/options", EDGEDB_AUTH_BASE_URL);
       authenticateUrl.searchParams.set("email", email);

       const authenticateResponse = await fetch(authenticateUrl.href);

       if (!authenticateResponse.ok) {
         const text = await authenticateResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       const authenticateData = await authenticateResponse.json();

       res.writeHead(200, { "Content-Type": "application/json" });
       res.end(JSON.stringify(authenticateData));
     });
   };

.. lint-on

Register a new credential
-------------------------

The client script will call the Web Authentication API to create a new
credential payload and send it to this endpoint. This endpoints job will be to
forward the serialized credential payload to the EdgeDB Auth extension for
verification, and then associate the credential with the user's email address.

.. lint-off

.. code-block:: javascript

  const handleRegister = async (req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });
    req.on("end", async () => {
      const { challenge, verifier } = generatePKCE();
      const { email, provider, credentials, verify_url, user_handle } = JSON.parse(body);
      if (!email || !provider || !credentials || !verify_url || !user_handle) {
        res.status = 400;
        res.end(
          `Request body malformed. Expected JSON body with 'email', 'provider', 'credentials', 'verify_url', and 'user_handle' keys, but got: ${body}`,
        );
        return;
      }

      const registerUrl = new URL("webauthn/register", EDGEDB_AUTH_BASE_URL);

      const registerResponse = await fetch(registerUrl.href, {
        method: "post",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          provider,
          email,
          credentials,
          verify_url,
          user_handle,
          challenge,
        }),
      });

      if (!registerResponse.ok) {
        const text = await registerResponse.text();
        res.status = 400;
        res.end(`Error from the auth server: ${text}`);
        return;
      }

      const registerData = await registerResponse.json();
      if ("code" in registerData) {
        const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
        tokenUrl.searchParams.set("code", registerData.code);
        tokenUrl.searchParams.set("verifier", verifier);
        const tokenResponse = await fetch(tokenUrl.href, {
          method: "get",
        });

        if (!tokenResponse.ok) {
          const text = await authenticateResponse.text();
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
        res.writeHead(204, {
          "Set-Cookie": `edgedb-pkce-verifier=${pkce.verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
        });
        res.end();
      }
    });
  };

.. lint-on

Authenticate with an existing credential
----------------------------------------

The client script will call the Web Authentication API to authenticate with an
existing credential and send the assertion to this endpoint. This endpoint's
job will be to forward the serialized assertion to the EdgeDB Auth extension
for verification.

.. lint-off

.. code-block:: javascript

  const handleAuthenticate = async (req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });
    req.on("end", async () => {
      const { challenge, verifier } = generatePKCE();
      const { email, provider, assertion } = JSON.parse(body);
      if (!email || !provider || !assertion) {
        res.status = 400;
        res.end(
          `Request body malformed. Expected JSON body with 'email', 'provider', and 'assertion' keys, but got: ${body}`,
        );
        return;
      }

      const authenticateUrl = new URL("webauthn/authenticate", EDGEDB_AUTH_BASE_URL);

      const authenticateResponse = await fetch(authenticateUrl.href, {
        method: "post",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          provider,
          email,
          assertion,
          challenge,
        }),
      });

      if (!authenticateResponse.ok) {
        const text = await authenticateResponse.text();
        res.status = 400;
        res.end(`Error from the auth server: ${text}`);
        return;
      }

      const authenticateData = await authenticateResponse.json();
      if ("code" in authenticateData) {
        const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
        tokenUrl.searchParams.set("code", authenticateData.code);
        const tokenResponse = await fetch(tokenUrl.href, {
          method: "get",
        });

        if (!tokenResponse.ok) {
          const text = await authenticateResponse.text();
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
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Email must be verified before being able to authenticate." }));
      }
    });
  };

.. lint-on

Handle email verification
-------------------------

When a new user signs up, by default we require them to verify their email
address before allowing the application to get an authentication token. To
handle the verification flow, we implement an endpoint:

.. note::

   💡 If you would like to allow users to still log in, but offer limited access
   to your application, you can check the associated
   ``ext::auth::WebAuthnFactor`` for the ``ext::auth::Identity`` to see if the
   ``verified_at`` property is some time in the past. You'll need to set the
   ``require_verification`` setting in the provider configuration to ``false``.

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

     const cookies = req.headers.cookie?.split("; ");
     const verifier = cookies
       ?.find((cookie) => cookie.startsWith("edgedb-pkce-verifier="))
       ?.split("=")[1];
     if (!verifier) {
       res.status = 400;
       res.end(
         `Could not find 'verifier' in the cookie store. Is this the same user agent/browser that started the authorization flow?`,
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
         verifier,
         provider: "builtin::webauthn",
       }),
     });

     if (!verifyResponse.ok) {
       const text = await verifyResponse.text();
       res.status = 400;
       res.end(`Error from the auth server: ${text}`);
       return;
     }

     const { code } = await verifyResponse.json();

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
   };

.. lint-on

Client-side script
------------------

On the client-side, you will need to write a script that retrieves the options
from the EdgeDB Auth extension, calls the Web Authentication API, and sends the
resulting credential or assertion to the server. Writing out the low-level
handling of serialization and deserialization of the WebAuthn data is beyond the
scope of this guide, but we publish a WebAuthn client library that you can use
to simlify this process. The library is available on npm as part of our
``@edgedb/auth-core`` library. Here is an example of how you might set up a form
with appropriate click handlers to perform the WebAuthn sign in and sign up
ceremonies.

.. lint-off

.. code-block:: javascript

  import { WebAuthnClient } from "@edgedb/auth-core/webauthn";

  const webAuthnClient = new WebAuthnClient({
    signupOptionsUrl: "http://localhost:3000/auth/webauthn/register/options",
    signupUrl: "http://localhost:3000/auth/webauthn/register",
    signinOptionsUrl: "http://localhost:3000/auth/webauthn/authenticate/options",
    signinUrl: "http://localhost:3000/auth/webauthn/authenticate",
    verifyUrl: "http://localhost:3000/auth/webauthn/verify",
  });

  document.addEventListener("DOMContentReady", () => {
    const signUpButton = document.querySelector("button#sign-up");
    const signInButton = document.querySelector("button#sign-in");
    const emailInput = document.querySelector("input#email");

    if (signUpButton) {
      signUpButton.addEventListener("click", async (event) => {
        event.preventDefault();
        const email = emailInput.value.trim();
        if (!email) {
          throw new Error("No email provided");
        }
        try {
          await webAuthnClient.signUp(email);
          window.location = "http://localhost:3000/signup-success";
        } catch (err) {
          console.error(err);
          window.location = "http://localhost:3000/signup-error";
        }
      });
    }

    if (signInButton) {
      signInButton.addEventListener("click", async (event) => {
        event.preventDefault();
        const email = emailInput.value.trim();
        if (!email) {
          throw new Error("No email provided");
        }
        try {
          await webAuthnClient.signIn(email);
          window.location = "http://localhost:3000";
        } catch (err) {
          console.error(err);
          window.location = "http://localhost:3000/signup-error";
        }
      })
    }
  });

.. lint-on
