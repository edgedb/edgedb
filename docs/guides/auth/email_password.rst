.. _ref_guide_auth_email_password:

==================
Email and password
==================

:edb-alt-title: Integrating EdgeDB Auth's email and password provider

We secure authentication tokens and other sensitive data by using PKCE
(Proof Key of Code Exchange).

1. Your application server should create a 32-byte base64 URL encoded
   string (which will be 43-44 bytes after encoding), which we will call
   the ``verifier``. You need to store this value for the duration of
   the flow. One way to accomplish this bit of state is to use an
   HttpOnly cookie when the browser makes a request to the server for
   this value, which you can then use to retrieve it from the cookie
   store at the end of the flow.

2. You take this ``verifier`` string, and then hash it with SHA256, and
   then base64url encode the resulting string. This new string is called
   the ``challenge``.

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

3. Depending on if you’re signing up a new user or signing in as an
   existing user:

   1. If registering a new user: your application should make a request
      to the EdgeDB Auth API to attempt to register a new user,
      including the URL of the route you want to use to verify user
      emails:

      .. code-block:: tsx

         const EDGEDB_AUTH_BASE_URL = new URL(
           `db/${process.env.DB_NAME}/ext/auth`,
           process.env.EDGEDB_SERVER_URL
         );
         const registerUrl = new URL("register", EDGEDB_AUTH_BASE_URL);
         const response = await fetch(
           registerUrl.href,
           {
             method: "POST",
             headers: { "Content-Type", "application/json" },
             data: JSON.stringify({
               email,
               password,
               verify_url: new URL(process.env.EMAIL_VERIFICATION_ROUTE),
               provider,
               challenge,
             }),
           }
         );

         if (!response.ok) {
           throw new Error("Oh no!");
         }
         const { code } = await response.json();

   2. If signing in an existing user: your application should make a
      request to the EdgeDB Auth API to attempt to authenticate the
      user:

      .. code-block:: tsx

         const EDGEDB_AUTH_BASE_URL = new URL(
           `db/${process.env.DB_NAME}/ext/auth`,
           process.env.EDGEDB_SERVER_URL
         );
         const authenticateUrl = new URL("authenticate", EDGEDB_AUTH_BASE_URL);
         const response = await fetch(
           authenticateUrl.href,
           {
             method: "POST",
             headers: { "Content-Type", "application/json" },
             data: JSON.stringify({
               email,
               password,
               provider,
               challenge,
             }),
           }
         );

         if (!response.ok) {
           throw new Error("Oh no!");
         }
         const { code } = await response.json();

   3. If verifying a user’s email: The user will receive an email with a
      link back to your application’s backend. The backend should make a
      request to the EdgeDB Auth API to attempt to verify the user’s
      email:

      .. code-block:: tsx

         const EDGEDB_AUTH_BASE_URL = new URL(
           `db/${process.env.DB_NAME}/ext/auth`,
           process.env.EDGEDB_SERVER_URL
         );
         const verifyUrl = new URL("verify", EDGEDB_AUTH_BASE_URL);
         const searchParams = new URL(request.url).searchParams;
         const verificationToken = searchParams.get("verification_token");
         const provider = searchParams.get("provider");
         const email = searchParams.get("email");
         if (!verificationToken || !provider || !email) {
           throw new Error("Missing required data in request search parameters");
         }

         const response = await fetch(verifyUrl.href, {
           method: "POST",
           headers: {
             "Content-Type": "application/json",
           },
           body: JSON.stringify({
             verification_token: verificationToken,
             provider,
           }),
         });

         if (!response.ok) {
           throw new Error("Oh no!");
         }

         const { code } = await response.json();

4. Next, you take that ``code`` and the ``verifier`` you stored in step
   1, make a request to the EdgeDB Auth API to exchange the ``code`` and
   ``verifier`` for an ``auth_token`` and ``identity_id``.

   .. code-block:: tsx

      const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
      tokenUrl.searchParams.set("code", code);
      tokenUrl.searchParams.set("verifier", verifier);
      const tokenResponse = await fetch(tokenUrl.href, {
        method: "GET",
      });
      if (!tokenResponse.ok) {
        throw new Error("Could not exchange code and verifier for an auth token");
      }
      const { auth_token: authToken, identity_id: identityId } =
        await tokenResponse.json();

      const client = anonymousClient.withGlobals({
        "ext::auth::client_token": authToken,
      });
      await client.query(
        `
        with identity := assert_exists(global ext::auth::ClientTokenIdentity),
        insert User {
          name := "",
          identities := identity,
        };`,
        );
