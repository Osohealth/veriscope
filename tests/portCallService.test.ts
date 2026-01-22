import assert from "node:assert/strict";
import test from "node:test";
import { detectPortTransitions } from "../src/services/portCallService";

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
