document.addEventListener("DOMContentLoaded", () => {
  const registerForm = document.getElementById("email-factor");

  console.log({ registerForm });
  if (registerForm === null) {
    return;
  }

  registerForm.addEventListener("submit", async (event) => {
    console.log({
      submitter: event.submitter,
      submitterId: event.submitter?.id,
    });
    if (event.submitter?.id !== "webauthn-signup") {
      return;
    }
    event.preventDefault();

    const formData = new FormData(/** @type {HTMLFormElement} */ registerForm);
    const email = formData.get("email");
    const provider = "builtin::local_webauthn";
    const challenge = formData.get("challenge");
    const redirectOnFailure = formData.get("redirect_on_failure");
    const redirectTo = formData.get("redirect_to");
    const verifyUrl = formData.get("verify_url");

    if (redirectTo === null) {
      throw new Error("Missing redirect_to parameter");
    }

    try {
      const maybeCode = await register({
        email,
        provider,
        challenge,
        verifyUrl,
      });

      const redirectUrl = new URL(redirectTo);
      redirectUrl.searchParams.append("isSignup", "true");
      if (maybeCode !== null) {
        redirectUrl.searchParams.append("code", maybeCode);
      }

      window.location.href = redirectUrl.href;
    } catch (error) {
      console.error("Failed to register WebAuthn credentials:", error);
      const url = new URL(redirectOnFailure ?? redirectTo);
      url.searchParams.append("error", error.message);
      window.location.href = url.href;
    }
  });
});

const WEBAUTHN_OPTIONS_URL = new URL("../webauthn/register/options", window.location);
const WEBAUTHN_REGISTER_URL = new URL("../webauthn/register", window.location);

/**
 * Decode a base64url encoded string
 * @param {string} base64UrlString
 * @returns Uint8Array
 */
function decodeBase64Url(base64UrlString) {
  return Uint8Array.from(
    atob(base64UrlString.replace(/-/g, "+").replace(/_/g, "/")),
    (c) => c.charCodeAt(0)
  );
}

/**
 * Encode a Uint8Array to a base64url encoded string
 * @param {Uint8Array} bytes
 * @returns string
 */
function encodeBase64Url(bytes) {
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

/**
 * Register a new WebAuthn credential for the given email address
 * @param {Object} props - The properties for registration
 * @param {string} props.email - Email address to register
 * @param {string} props.provider - WebAuthn provider
 * @param {string} props.challenge - PKCE challenge
 * @param {string} props.verifyUrl - URL to verify email after registration
 * @returns {Promise<string | null>} - The PKCE code or null if the application
 *   requires email verification
 */
export async function register({ email, provider, challenge, verifyUrl }) {
  // Check if WebAuthn is supported
  if (!window.PublicKeyCredential) {
    console.error("WebAuthn is not supported in this browser.");
    return;
  }

  // Fetch WebAuthn options from the server
  const options = await getCreateOptions(email);
  console.log(JSON.stringify(options, null, 2));

  // Register the new credential
  const credentials = await navigator.credentials.create({
    publicKey: {
      ...options,
      challenge: decodeBase64Url(options.challenge),
      user: {
        ...options.user,
        id: decodeBase64Url(options.user.id),
      },
    },
  });

  // Register the credentials on the server
  const registerResult = await registerCredentials({
    email,
    credentials,
    provider,
    challenge,
    verifyUrl,
  });

  return registerResult.code ?? null;
}

/**
 * Fetch WebAuthn options from the server
 * @param {string} email - Email address to register
 * @returns {Promise<globalThis.PublicKeyCredentialCreationOptions>}
 */
async function getCreateOptions(email) {
  const url = new URL(WEBAUTHN_OPTIONS_URL);
  url.searchParams.set("email", email);

  const optionsResponse = await fetch(url, {
    method: "GET",
  });

  if (!optionsResponse.ok) {
    console.error(
      "Failed to fetch WebAuthn options:",
      optionsResponse.statusText
    );
    console.error(await optionsResponse.text());
    throw new Error("Failed to fetch WebAuthn options");
  }

  try {
    return await optionsResponse.json();
  } catch (e) {
    console.error("Failed to parse WebAuthn options:", e);
    throw new Error("Failed to parse WebAuthn options");
  }
}

/**
 * @typedef RegisterCredentialsProps
 * @property {string} email
 * @property {Object} credentials
 * @property {string} provider
 * @property {string} challenge
 * @property {string} verifyUrl
 */

/**
 * Register the credentials on the server
 * @param {RegisterCredentialsProps} props
 * @returns {Promise<Object>}
 */
async function registerCredentials(props) {
  // Credentials include raw bytes, so need to be encoded as base64url
  // for transmission
  const encodedCredentials = {
    ...props.credentials,
    rawId: encodeBase64Url(new Uint8Array(props.credentials.rawId)),
    response: {
      ...props.response,
      attestationObject: encodeBase64Url(
        new Uint8Array(props.credentials.response.attestationObject)
      ),
      clientDataJSON: encodeBase64Url(
        new Uint8Array(props.credentials.response.clientDataJSON)
      ),
    },
  };

  const registerResponse = await fetch(WEBAUTHN_REGISTER_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email: props.email,
      credentials: encodedCredentials,
      provider: props.provider,
      challenge: props.challenge,
      verify_url: props.verifyUrl,
    }),
  });

  if (!registerResponse.ok) {
    console.error(
      "Failed to register WebAuthn credentials:",
      registerResponse.statusText
    );
    console.error(await registerResponse.text());
    throw new Error("Failed to register WebAuthn credentials");
  }

  try {
    return await registerResponse.json();
  } catch (e) {
    console.error("Failed to parse WebAuthn registration result:", e);
    throw new Error("Failed to parse WebAuthn registration result");
  }
}
