import { onRegisterSubmit } from "./webauthn-register.js";
import { onAuthenticateSubmit } from "./webauthn-authenticate.js";

document.addEventListener("DOMContentLoaded", () => {
  // Check if WebAuthn is supported
  if (!window.PublicKeyCredential) {
    console.error("WebAuthn is not supported in this browser.");
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
