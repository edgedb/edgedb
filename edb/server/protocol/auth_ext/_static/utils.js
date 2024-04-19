/**
 * @param {(form: HTMLFormElement) => void} handler
 * @returns Uint8Array
 */
export function addWebAuthnSubmitHandler(handler) {
  document.addEventListener("DOMContentLoaded", () => {
    if (!window.PublicKeyCredential) {
      console.error("WebAuthn is not supported in this browser.");

      for (const button of [
        document.getElementById("webauthn-signin"),
        document.getElementById("webauthn-signup"),
      ]) {
        if (button) {
          const newEl = document.createElement("div");
          newEl.classList.add("no-webauthn-error");
          newEl.appendChild(
            document.createTextNode(
              `Your browser does not support the WebAuthn API. ` +
                `Use another login method, or upgrade your browser.`
            )
          );
          button.parentNode.replaceChild(newEl, button);
        }
      }
      return;
    }

    const emailFactorForm = document.getElementById("email-factor");

    if (emailFactorForm === null) {
      return;
    }

    emailFactorForm.addEventListener("submit", (event) => {
      if (new URL(emailFactorForm.action).pathname == location.pathname) {
        event.preventDefault();
        handler(emailFactorForm);
      }
    });
  });
}

/**
 * Decode a base64url encoded string
 * @param {string} base64UrlString
 * @returns Uint8Array
 */
export function decodeBase64Url(base64UrlString) {
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
export function encodeBase64Url(bytes) {
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

/**
 * Parse an HTTP Response object. Allows passing in custom handlers for
 * different status codes and error.type values
 *
 * @param {Response} response
 * @param {Function[]=} handlers
 */
export async function parseResponseAsJSON(response, handlers = []) {
  const bodyText = await response.text();

  if (!response.ok) {
    let error;
    try {
      error = JSON.parse(bodyText)?.error;
    } catch (e) {
      throw new Error(
        `Failed to parse body as JSON. Status: ${response.status} ${response.statusText}. Body: ${bodyText}`
      );
    }

    for (const handler of handlers) {
      handler(response, error);
    }

    throw new Error(
      `Response was not OK. Status: ${response.status} ${response.statusText}. Body: ${bodyText}`
    );
  }

  return JSON.parse(bodyText);
}
