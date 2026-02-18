import { createHash, randomBytes } from "node:crypto";

const resolveInvitePepper = () => {
  const pepper = process.env.INVITE_TOKEN_PEPPER || process.env.API_KEY_PEPPER;
  if (!pepper) {
    if (process.env.NODE_ENV === "development") {
      return "dev-pepper";
    }
    throw new Error("INVITE_TOKEN_PEPPER is required");
  }
  return pepper;
};

export const generateInviteToken = () => {
  return randomBytes(24).toString("base64url");
};

export const hashInviteToken = (rawToken: string) => {
  const pepper = resolveInvitePepper();
  return createHash("sha256").update(`${pepper}${rawToken}`).digest("hex");
};
