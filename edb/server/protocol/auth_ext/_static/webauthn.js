const WEBAUTHN_OPTIONS_URL = new URL();
const WEBAUTHN_VERIFY_URL = new URL();

/**
 * @typedef {Object} RegisterProps
 * @property {string} email - Email address to register
 * @property {string} provider - WebAuthn provider
 * @property {string} challenge - PKCE challenge
 * @property {string} verifyUrl - URL to verify email after registration
 */

/**
 * Register a new WebAuthn credential for the given email address
 * @param {RegisterProps} props
 * @returns {Promise<string | null>} - The PKCE code or null if the application
 *   requires email verification
 */
export async function register({
  email,
  provider,
  challenge,
  verifyUrl,
}) {
  // Check if WebAuthn is supported
  if (!window.PublicKeyCredential) {
    console.error("WebAuthn is not supported in this browser.");
    return;
  }

  // Fetch WebAuthn options from the server
  const options = await getCreateOptions(email);

  // Register the new credential
  const credentials = await navigator.credentials.create({
    publicKey: options,
  });

  // Verify the credentials on the server
  const verifyResult = await verifyCredentials({
    email,
    credentials,
    provider,
    challenge,
    verifyUrl,
  });

  return verifyResult.code ?? null;
}

export function authenticate(email) {
  // Check if WebAuthn is supported
  if (!window.PublicKeyCredential) {
    console.error("WebAuthn is not supported in this browser.");
    return;
  }
}

/**
 * Fetch WebAuthn options from the server
 * @param {string} email - Email address to register
 * @returns {Promise<globalThis.PublicKeyCredentialCreationOptions>}
 */
async function getCreateOptions(email) {
  const url = new URL(WEBAUTHN_OPTIONS_URL);
  url.searchParams.append("email", email);

  const optionsResponse = await fetch(WEBAUTHN_OPTIONS_URL, {
    method: "GET",
  });

  if (!optionsResponse.ok) {
    console.error(
      "Failed to fetch WebAuthn options:",
      optionsResponse.statusText
    );
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
 * @typedef VerifyProps
 * @property {string} email
 * @property {Object} credentials
 * @property {string} provider
 * @property {string} challenge
 * @property {string} verifyUrl
 */

/**
 * Verify the credentials on the server
 * @param {VerifyProps} props
 * @returns {Promise<Object>}
 */
async function verifyCredentials(props) {
  const verifyResponse = await fetch(WEBAUTHN_VERIFY_URL, {
    method: "POST",
    body: JSON.stringify({
      email: props.email,
      credentials: props.credentials,
      provider: props.provider,
      challenge: props.challenge,
      verify_url: props.verifyUrl,
    }),
  });

  if (!verifyResponse.ok) {
    console.error(
      "Failed to verify WebAuthn credentials:",
      verifyResponse.statusText
    );
    throw new Error("Failed to verify WebAuthn credentials");
  }

  try {
    return await verifyResponse.json();
  } catch (e) {
    console.error("Failed to parse WebAuthn verification result:", e);
    throw new Error("Failed to parse WebAuthn verification result");
  }
}
