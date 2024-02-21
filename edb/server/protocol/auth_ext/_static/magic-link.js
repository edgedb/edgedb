import { parseResponseAsJSON } from "./utils.js";

const MAGIC_LINK_REGISTER_URL = new URL(
  "../magic-link/register",
  window.location.href
);
const MAGIC_LINK_EMAIL_URL = new URL(
  "../magic-link/email",
  window.location.href
);
const MAGIC_LINK_SENT_URL = new URL("./magic-link-sent", window.location.href);

document.addEventListener("DOMContentLoaded", function () {
  const emailFactorForm = document.getElementById("email-factor");

  if (emailFactorForm === null) {
    return;
  }

  emailFactorForm.addEventListener("submit", async (event) => {
    switch (event.submitter?.id) {
      case "magic-link-signup":
      case "magic-link-signin": {
        event.preventDefault();

        const formData = new FormData(emailFactorForm);
        const email = formData.get("email");
        const provider = "builtin::local_magic_link";
        const callbackUrl = formData.get("redirect_to");
        const challenge = formData.get("challenge");

        const missingFields = [email, provider, callbackUrl, challenge].filter(
          (v) => !v
        );
        if (missingFields.length > 0) {
          throw new Error(
            "Missing required parameters: " + missingFields.join(", ")
          );
        }

        try {
          if (event.submitter.id === "magic-link-signup") {
            await registerMagicLink({
              email,
              provider,
              callbackUrl,
              challenge,
            });
          } else if (event.submitter.id === "magic-link-signin") {
            await sendMagicLink({
              email,
              provider,
              callbackUrl,
              challenge,
            });
          }
          window.location = MAGIC_LINK_SENT_URL.href;
        } catch (err) {
          console.error("Magic link failed: ", err);
          const url = new URL(window.location.href);
          url.searchParams.append("error", err.message);
          window.location = url.href;
        }
      }
      default:
        return;
    }
  });
});

async function registerMagicLink({ email, provider, callbackUrl, challenge }) {
  const response = await fetch(MAGIC_LINK_REGISTER_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email,
      provider,
      callback_url: callbackUrl,
      challenge,
    }),
  });

  return await parseResponseAsJSON(response, [
    (response, error) => {
      if (response.status === 409 && error.type === "UserAlreadyRegistered") {
        throw new Error("User already registered, please sign in.");
      }
    },
    (response, error) => {
      console.error(
        "Failed to register: ",
        response.statusText,
        JSON.stringify(error)
      );
      throw new Error("Failed to register email.");
    },
  ]);
}

async function sendMagicLink({ email, provider, callbackUrl, challenge }) {
  const response = await fetch(MAGIC_LINK_EMAIL_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email,
      provider,
      callback_url: callbackUrl,
      challenge,
    }),
  });

  return await parseResponseAsJSON(response, [
    (response, error) => {
      console.error(
        "Failed to send magic link: ",
        response.statusText,
        JSON.stringify(error)
      );
      throw new Error("Failed to send magic link");
    },
  ]);
}
