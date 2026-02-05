import { addMinutes, addHours } from "date-fns";

export const computeNextAttempt = (now: Date, attemptCount: number) => {
  if (attemptCount <= 1) return addMinutes(now, 5);
  if (attemptCount === 2) return addMinutes(now, 15);
  if (attemptCount === 3) return addHours(now, 1);
  if (attemptCount === 4) return addHours(now, 6);
  return addHours(now, 12);
};
