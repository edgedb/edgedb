import { onRegisterSubmit } from "./webauthn-register.js";
import { onAuthenticateSubmit } from "./webauthn-authenticate.js";

document.addEventListener("DOMContentLoaded", () => {
  // Check if WebAuthn is supported
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
    onRegisterSubmit(event, emailFactorForm);
    onAuthenticateSubmit(event, emailFactorForm);
  });
});
