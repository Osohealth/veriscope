import { makeDestinationKey } from "./destinationKey";
import { listActiveUserContactMethods, selectContactMethod } from "./alertRoutingResolver";

export type ResolvedEscalationDestination = {
  destination_type: "EMAIL" | "WEBHOOK";
  destination: string;
  destination_key: string;
  target_user_id: string;
  target_name?: string | null;
  chosen_method_id: string;
  chosen_method_type: "EMAIL" | "WEBHOOK";
};

export type RoutingResult =
  | { ok: true; destinations: ResolvedEscalationDestination[] }
  | { ok: false; reason: "NO_USER_CONTACT_METHOD" };

export async function resolveEscalationTargets(args: {
  tenantId: string;
  userId: string;
  targetName?: string | null;
}) : Promise<RoutingResult> {
  const methods = await listActiveUserContactMethods({
    tenantId: args.tenantId,
    userId: args.userId,
  });

  if (methods.length === 0) {
    return { ok: false, reason: "NO_USER_CONTACT_METHOD" };
  }

  const chosen = selectContactMethod(methods);
  if (!chosen) {
    return { ok: false, reason: "NO_USER_CONTACT_METHOD" };
  }

  const destinationType = chosen.type as "EMAIL" | "WEBHOOK";
  const destination = chosen.value;

  return {
    ok: true,
    destinations: [{
      destination_type: destinationType,
      destination,
      destination_key: makeDestinationKey(destinationType, destination),
      target_user_id: args.userId,
      target_name: args.targetName ?? null,
      chosen_method_id: chosen.id,
      chosen_method_type: destinationType,
    }],
  };
}
