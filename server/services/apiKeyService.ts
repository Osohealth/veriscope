import { createHash, randomBytes } from "node:crypto";

const resolvePepper = () => {
  const pepper = process.env.API_KEY_PEPPER;
  if (!pepper) {
    throw new Error("API_KEY_PEPPER environment variable is required but not set. Refusing to start with an insecure default.");
  }
  return pepper;
};

export const hashApiKey = (rawKey: string) => {
  const pepper = resolvePepper();
  return createHash("sha256").update(`${pepper}${rawKey}`).digest("hex");
};

export const generateApiKey = (prefix: string = "vs_demo") => {
  const token = randomBytes(24).toString("base64url");
  return `${prefix}_${token}`;
};
