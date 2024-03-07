import {
  addWebAuthnSubmitHandler,
  decodeBase64Url,
  encodeBase64Url,
} from "./utils.js";

addWebAuthnSubmitHandler(onRegisterSubmit);

let registering = false;

/**
 * Handle the form submission for WebAuthn registration
 * @param {HTMLFormElement} form
 * @returns void
 */
export async function onRegisterSubmit(form) {
  if (registering) {
    return;
  }

  registering = true;
  const registerButton = document.getElementById("webauthn-signup");
  registerButton.disabled = true;

  const formData = new FormData(form);
  const email = formData.get("email");
  const provider = "builtin::local_webauthn";
  const challenge = formData.get("challenge");
  const redirectOnFailure = formData.get("redirect_on_failure");
  const redirectTo = formData.get("redirect_to");
  const verifyUrl = formData.get("verify_url");

  try {
    const missingFields = Object.entries({
      email,
      provider,
      challenge,
      redirectTo,
      verifyUrl,
    }).filter(([k, v]) => !v);
    if (missingFields.length > 0) {
      throw new Error(
        "Missing required parameters: " +
          missingFields.map(([k]) => k).join(", ")
      );
    }

    const response = await register({
      email,
      provider,
      challenge,
      verifyUrl,
    });

    const redirectUrl = new URL(redirectTo);
    redirectUrl.searchParams.append("isSignUp", "true");
    if ("code" in response) {
      redirectUrl.searchParams.append("code", response.code);
    } else if ("verification_email_sent_at" in response) {
      redirectUrl.searchParams.append(
        "verification_email_sent_at",
        response.verification_email_sent_at
      );
    }

    window.location.href = redirectUrl.href;
  } catch (error) {
    console.error("Failed to register WebAuthn credentials:", error);
    const url = new URL(redirectOnFailure ?? redirectTo);
    url.searchParams.append("error", error.message);
    window.location.href = url.href;
  } finally {
    registering = false;
    registerButton.disabled = false;
  }
}

const WEBAUTHN_OPTIONS_URL = new URL(
  "../webauthn/register/options",
  window.location
);
const WEBAUTHN_REGISTER_URL = new URL("../webauthn/register", window.location);

/**
 * Register a new WebAuthn credential for the given email address
 * @param {Object} props - The properties for registration
 * @param {string} props.email - Email address to register
 * @param {string} props.provider - WebAuthn provider
 * @param {string} props.challenge - PKCE challenge
 * @param {string} props.verifyUrl - URL to verify email after registration
 * @returns {Promise<object>} - The server response
 */
export async function register({ email, provider, challenge, verifyUrl }) {
  // Check if WebAuthn is supported
  if (!window.PublicKeyCredential) {
    console.error("WebAuthn is not supported in this browser.");
    return;
  }

  // Fetch WebAuthn options from the server
  const options = await getCreateOptions(email);

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
  return await registerCredentials({
    email,
    credentials,
    provider,
    challenge,
    verifyUrl,
  });
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
 * Register the credentials on the server
 * @param {Object} props
 * @param {string} props.email
 * @param {Object} props.credentials
 * @param {string} props.provider
 * @param {string} props.challenge
 * @param {string} props.verifyUrl
 * @returns {Promise<Object>}
 */
async function registerCredentials(props) {
  // Credentials include raw bytes, so need to be encoded as base64url
  // for transmission
  const encodedCredentials = {
    type: props.credentials.type,
    authenticatorAttachment: props.credentials.authenticatorAttachment,
    clientExtensionResults: props.credentials.getClientExtensionResults(),
    id: props.credentials.id,
    rawId: encodeBase64Url(new Uint8Array(props.credentials.rawId)),
    response: {
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
