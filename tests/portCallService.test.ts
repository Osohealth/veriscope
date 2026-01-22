import assert from "node:assert/strict";
import test from "node:test";
import { detectPortTransitions, derivePortStateTransition } from "../src/services/portCallService";

const port = {
  id: "port-1",
  name: "Test Port",
  lat: 0,
  lon: 0,
  geofenceRadiusKm: 2,
};

test("arrival created when crossing boundary", () => {
  const positions = [
    { lat: 0.05, lon: 0.05, timestampUtc: new Date("2024-01-01T00:00:00Z") },
    { lat: 0.0, lon: 0.0, timestampUtc: new Date("2024-01-01T00:10:00Z") },
  ];

  const transition = detectPortTransitions(positions, port);

  assert.equal(transition.arrivalAt?.toISOString(), "2024-01-01T00:10:00.000Z");
  assert.equal(transition.departureAt, undefined);
  assert.equal(transition.currentlyInside, true);
});

test("departure closes call when leaving boundary", () => {
  const positions = [
    { lat: 0.0, lon: 0.0, timestampUtc: new Date("2024-01-01T00:00:00Z") },
    { lat: 0.05, lon: 0.05, timestampUtc: new Date("2024-01-01T00:10:00Z") },
  ];

  const transition = detectPortTransitions(positions, port);

  assert.equal(transition.departureAt?.toISOString(), "2024-01-01T00:10:00.000Z");
  assert.equal(transition.arrivalAt, undefined);
  assert.equal(transition.currentlyInside, false);
});

test("repeated inside positions do not create multiple arrivals", () => {
  const positions = [
    { lat: 0.0, lon: 0.0, timestampUtc: new Date("2024-01-01T00:00:00Z") },
    { lat: 0.01, lon: 0.01, timestampUtc: new Date("2024-01-01T00:05:00Z") },
    { lat: 0.02, lon: 0.02, timestampUtc: new Date("2024-01-01T00:10:00Z") },
  ];

  const transition = detectPortTransitions(positions, port);

  assert.equal(transition.arrivalAt, undefined);
  assert.equal(transition.departureAt, undefined);
  assert.equal(transition.currentlyInside, true);
});

test("state transition opens a call only once for repeated inside positions", () => {
  const initialState = {
    vesselId: "vessel-1",
    inPort: false,
    currentPortId: null,
    currentPortCallId: null,
    lastPositionTimeUtc: null,
  };

  const first = derivePortStateTransition(
    initialState,
    true,
    "port-1",
    new Date("2024-01-01T00:00:00Z"),
  );

  assert.equal(first.action, "open");
  assert.equal(first.nextState.inPort, true);
  assert.equal(first.nextState.currentPortId, "port-1");

  const second = derivePortStateTransition(
    first.nextState,
    true,
    "port-1",
    new Date("2024-01-01T00:05:00Z"),
  );

  assert.equal(second.action, "none");
  assert.equal(second.nextState.inPort, true);
});

test("state transition closes call and clears state on departure", () => {
  const initialState = {
    vesselId: "vessel-1",
    inPort: true,
    currentPortId: "port-1",
    currentPortCallId: "call-1",
    lastPositionTimeUtc: new Date("2024-01-01T00:00:00Z"),
  };

  const transition = derivePortStateTransition(
    initialState,
    false,
    null,
    new Date("2024-01-01T00:10:00Z"),
  );

  assert.equal(transition.action, "close");
  assert.equal(transition.nextState.inPort, false);
  assert.equal(transition.nextState.currentPortId, null);
  assert.equal(transition.nextState.currentPortCallId, null);
});

test("restart-safe behavior uses persisted state to avoid duplicate arrivals", () => {
  const persistedState = {
    vesselId: "vessel-1",
    inPort: true,
    currentPortId: "port-1",
    currentPortCallId: "call-1",
    lastPositionTimeUtc: new Date("2024-01-01T00:00:00Z"),
  };

  const transition = derivePortStateTransition(
    persistedState,
    true,
    "port-1",
    new Date("2024-01-01T00:15:00Z"),
  );

  assert.equal(transition.action, "none");
  assert.equal(transition.nextState.inPort, true);
  assert.equal(transition.nextState.currentPortCallId, "call-1");
});
