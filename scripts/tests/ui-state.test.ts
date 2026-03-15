import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { buildQueryFromFilters, DEFAULT_FILTERS, type TerminalFilters } from "../../client/src/hooks/use-terminal-store";
import { saveView, listSavedViews, getSavedViewsError, updateView, deleteView } from "../../client/src/lib/saved-views";
import { buildAlertContextLink, buildAlertSelection } from "../../client/src/lib/alert-context";
import {
  createInvestigation,
  listInvestigations,
  updateInvestigation,
  deleteInvestigation,
  getInvestigationsError,
} from "../../client/src/lib/investigations";

const memoryStorage = () => {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
    clear: () => {
      store.clear();
    },
  };
};

test.describe("ui-state", () => {
  test.before(() => {
    const storage = memoryStorage();
    (globalThis as any).localStorage = storage;
    (globalThis as any).window = { localStorage: storage };
  });

  test.beforeEach(() => {
    (globalThis as any).localStorage.clear();
  });

  test.after(() => {
    delete (globalThis as any).localStorage;
    delete (globalThis as any).window;
  });

  test("URL sync serializes deterministically", () => {
    const filters: TerminalFilters = {
      commodity: ["LNG", "Crude"],
      mode: "cross",
      timeMode: "range",
      timeWindow: "7d",
      region: ["Europe"],
      origin: ["NLRTM"],
      destination: ["SGSIN"],
      hub: ["NLRTM"],
      riskTags: ["Weather", "Sanctions"],
    };
    const query = buildQueryFromFilters(filters, { id: "NLRTM", name: "Rotterdam", type: "port" });
    assert.equal(
      query,
      "commodity=Crude%2CLNG&time=7d&region=Europe&origin=NLRTM&destination=SGSIN&hub=NLRTM&risk=Sanctions%2CWeather&sel_id=NLRTM&sel_name=Rotterdam&sel_type=port"
    );
  });

  test("URL sync omits defaults and empty filters", () => {
    const query = buildQueryFromFilters(DEFAULT_FILTERS, null);
    assert.equal(query, "");

    const filters: TerminalFilters = {
      ...DEFAULT_FILTERS,
      origin: [],
      destination: [],
      hub: [],
      riskTags: [],
    };
    const cleaned = buildQueryFromFilters(filters, null);
    assert.equal(cleaned, "");
  });

  test("URL sync stable across list ordering", () => {
    const a: TerminalFilters = {
      ...DEFAULT_FILTERS,
      commodity: ["Crude", "LNG"],
      riskTags: ["Conflict", "Sanctions"],
      timeMode: "range",
      timeWindow: "30d",
    };
    const b: TerminalFilters = {
      ...DEFAULT_FILTERS,
      commodity: ["LNG", "Crude"],
      riskTags: ["Sanctions", "Conflict"],
      timeMode: "range",
      timeWindow: "30d",
    };
    assert.equal(buildQueryFromFilters(a, null), buildQueryFromFilters(b, null));
  });

  test("saved views restore round-trip", () => {
    const filters: TerminalFilters = {
      commodity: ["Crude"],
      mode: "sea",
      timeMode: "live",
      riskTags: ["Conflict"],
    };
    const view = saveView({
      name: "Test view",
      route: "/terminal",
      filters,
      selection: { id: "NLRTM", name: "Rotterdam", type: "port" },
    });
    const views = listSavedViews();
    assert.equal(views.length >= 1, true);
    const found = views.find((item) => item.id === view.id);
    assert.ok(found);
    assert.equal(found?.route, "/terminal");
    assert.equal(found?.selection?.entityId, "NLRTM");
  });

  test("saved views rename/delete do not corrupt store", () => {
    const view = saveView({
      name: "Rename me",
      route: "/flows",
      filters: { ...DEFAULT_FILTERS, timeMode: "range", timeWindow: "30d" },
    });
    const renamed = updateView(view.id, { name: "Renamed" });
    assert.ok(renamed);
    assert.equal(renamed?.name, "Renamed");
    deleteView(view.id);
    const views = listSavedViews();
    assert.equal(views.find((item) => item.id === view.id), undefined);
  });

  test("saved views support multiple routes", () => {
    const filters: TerminalFilters = {
      ...DEFAULT_FILTERS,
      timeMode: "range",
      timeWindow: "7d",
    };
    const routes = ["/terminal", "/flows", "/congestion", "/alerts"];
    routes.forEach((route) => {
      saveView({
        name: `View ${route}`,
        route,
        filters,
        selection: { id: "NLRTM", name: "Rotterdam", type: "port" },
      });
    });
    const views = listSavedViews();
    routes.forEach((route) => {
      assert.ok(views.find((view) => view.route === route));
    });
  });

  test("saved views handle malformed storage safely", () => {
    const key = "veriscope_saved_views";
    (globalThis as any).localStorage.setItem(key, "{not valid");
    const views = listSavedViews();
    assert.equal(views.length, 0);
    assert.ok(getSavedViewsError());
  });

  test("investigation store restores and updates", () => {
    const investigation = createInvestigation({
      title: "Test investigation",
      sourceRoute: "/terminal?commodity=Crude",
      linkedEntityId: "NLRTM",
      linkedEntityName: "Rotterdam",
      linkedEntityType: "port",
    });
    const all = listInvestigations();
    assert.ok(all.find((item) => item.id === investigation.id));
    assert.equal(all.find((item) => item.id === investigation.id)?.sourceRoute, "/terminal?commodity=Crude");
    const updated = updateInvestigation(investigation.id, { status: "closed", notes: "Resolved." });
    assert.equal(updated?.status, "closed");
    deleteInvestigation(investigation.id);
    const afterDelete = listInvestigations();
    assert.equal(afterDelete.find((item) => item.id === investigation.id), undefined);
  });

  test("investigation without source context is safe", () => {
    const investigation = createInvestigation({ title: "No source" });
    const found = listInvestigations().find((item) => item.id === investigation.id);
    assert.ok(found);
    assert.equal(found?.sourceRoute, undefined);
  });

  test("investigation deep-link from alerts is preserved", () => {
    const investigation = createInvestigation({
      title: "Alert investigation",
      sourceRoute: "/alerts?status=OPEN&severity=HIGH",
      linkedAlertId: "alert-1",
    });
    const found = listInvestigations().find((item) => item.id === investigation.id);
    assert.ok(found);
    assert.equal(found?.sourceRoute, "/alerts?status=OPEN&severity=HIGH");
    assert.equal(found?.linkedAlertId, "alert-1");
  });

  test("alert context links resolve deterministic routes", () => {
    const incident = {
      id: "incident-1",
      type: "PORT_CONGESTION",
      destination_key: "port_congestion",
      title: "NLRTM congestion",
      summary: "Queue rising at NLRTM",
    };
    const selection = buildAlertSelection(incident);
    const url = buildAlertContextLink(incident, DEFAULT_FILTERS, selection);
    assert.ok(url.startsWith("/congestion"));
    assert.ok(url.includes("sel_id="));
  });

  test("investigations handle malformed storage safely", () => {
    const key = "veriscope:investigations";
    (globalThis as any).localStorage.setItem(key, "{not valid");
    const items = listInvestigations();
    assert.equal(items.length, 0);
    assert.ok(getInvestigationsError());
  });

  test("route smoke: key dashboard routes exist in App.tsx", () => {
    const appPath = path.resolve(process.cwd(), "client", "src", "App.tsx");
    const content = fs.readFileSync(appPath, "utf8");
    ["/terminal", "/flows", "/congestion", "/alerts", "/views", "/investigations", "/command"].forEach((route) => {
      assert.ok(content.includes(`path=\"${route}\"`) || content.includes(`path='${route}'`));
    });
  });
});
