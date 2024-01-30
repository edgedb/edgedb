import { register } from "./webauthn.js";

document.addEventListener("DOMContentLoaded", () => {
  const registerForm = document.getElementById("register-form");

  if (registerForm === null) {
    return;
  }

  registerForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    const formData = new FormData(registerForm);
    const email = formData.get("email");
    const provider = formData.get("provider");
    const challenge = formData.get("challenge");
    const redirectOnFailure = formData.get("redirect_on_failure");
    const redirectTo = formData.get("redirect_to");
    const verifyUrl = formData.get("verify_url");

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
      const url = new URL(redirectOnFailure);
      url.searchParams.append("error", error.message);
      window.location.href = url.href;
    }
  });
});
