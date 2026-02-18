import { createHash } from "node:crypto";

export const normalizeWebhookUrl = (value: string) => {
  try {
    const url = new URL(String(value ?? "").trim());
    const host = url.host.toLowerCase();
    const protocol = url.protocol.toLowerCase();
    let pathname = url.pathname || "/";
    if (pathname !== "/" && pathname.endsWith("/")) {
      pathname = pathname.slice(0, -1);
    }
    return `${protocol}//${host}${pathname}`;
  } catch {
    return String(value ?? "").trim();
  }
};

export const hashKey = (value: string) =>
  createHash("sha256").update(value).digest("hex").slice(0, 16);

export const makeDestinationKey = (destinationType: string, destination: string) => {
  const type = String(destinationType ?? "").toUpperCase();
  if (type === "EMAIL") {
    return hashKey(String(destination ?? "").trim().toLowerCase());
  }
  return hashKey(normalizeWebhookUrl(String(destination ?? "")));
};
