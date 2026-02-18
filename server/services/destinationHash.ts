import { hashKey } from "./destinationKey";

export const hashDestination = (destination: string) => {
  const raw = String(destination ?? "");
  return hashKey(raw);
};
