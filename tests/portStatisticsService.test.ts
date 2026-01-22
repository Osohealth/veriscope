import assert from "node:assert/strict";
import test from "node:test";
import { computePortMetrics7dFromCalls } from "../src/services/portStatisticsService";

test("computes KPI counts and dwell hours for open and closed calls", () => {
  const now = new Date("2024-01-08T00:00:00Z");
  const calls = [
    {
      vesselId: "v1",
      arrivalTimeUtc: new Date("2024-01-07T00:00:00Z"),
      departureTimeUtc: new Date("2024-01-07T12:00:00Z"),
    },
    {
      vesselId: "v2",
      arrivalTimeUtc: new Date("2024-01-06T00:00:00Z"),
      departureTimeUtc: null,
    },
  ];

  const metrics = computePortMetrics7dFromCalls(calls, now);

  assert.equal(metrics.arrivals_7d, 2);
  assert.equal(metrics.departures_7d, 1);
  assert.equal(metrics.unique_vessels_7d, 2);
  assert.equal(metrics.open_calls, 1);
  assert.equal(metrics.avg_dwell_hours_7d, 30);
});

test("excludes arrivals outside the window but counts departures inside the window", () => {
  const now = new Date("2024-01-08T00:00:00Z");
  const calls = [
    {
      vesselId: "v1",
      arrivalTimeUtc: new Date("2023-12-20T00:00:00Z"),
      departureTimeUtc: new Date("2024-01-07T00:00:00Z"),
    },
    {
      vesselId: "v2",
      arrivalTimeUtc: new Date("2024-01-07T00:00:00Z"),
      departureTimeUtc: null,
    },
  ];

  const metrics = computePortMetrics7dFromCalls(calls, now);

  assert.equal(metrics.arrivals_7d, 1);
  assert.equal(metrics.departures_7d, 1);
  assert.equal(metrics.unique_vessels_7d, 1);
  assert.equal(metrics.open_calls, 1);
});
