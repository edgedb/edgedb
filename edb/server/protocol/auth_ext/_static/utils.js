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
