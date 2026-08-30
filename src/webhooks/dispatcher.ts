import { getSubscriptionsForContract } from "../indexer/db.js";
import { deliverWebhook } from "./deliver.js";
import type { MilestoneWebhookPayload } from "./milestone-events.js";

function subscriptionsMatchEventType(
  eventTypes: string,
  eventType: string
): boolean {
  if (eventTypes === "*") return true;
  try {
    const types = JSON.parse(eventTypes) as string[];
    return types.includes(eventType);
  } catch {
    return eventTypes === eventType;
  }
}

function reverseMapStatusToEventType(status: string): string | null {
  for (const [eventType, statusValue] of Object.entries({
    delivered: "delivered",
    approved: "approved",
    dispute_raised: "disputed",
    dispute_resolved: "resolved",
  })) {
    if (statusValue === status) return eventType;
  }
  return null;
}

export function dispatchMilestoneWebhook(payload: MilestoneWebhookPayload): void {
  const subscriptions = getSubscriptionsForContract(payload.contractId);
  if (subscriptions.length === 0) {
    return;
  }

  const eventType = reverseMapStatusToEventType(payload.newStatus);

  for (const subscription of subscriptions) {
    if (eventType && !subscriptionsMatchEventType(subscription.event_types, eventType)) {
      continue;
    }
    void deliverWebhook(subscription.webhook_url, payload);
  }
}
