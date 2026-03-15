const fs = require("fs");
const path = require("path");

const targetPath = path.join(__dirname, "signal-engine.test.ts");
const contents = fs.readFileSync(targetPath, "utf8");
const lines = contents.split(/\r?\n/);

const forbiddenLabels = ["alerts", "incidents", "escalations", "signals"]; 
const regexes = forbiddenLabels.map((label) => ({
  label,
  regex: new RegExp(String.raw`(?:^|\s)(?:test\.)?describe\(\s*['"]${label}`, "i"),
}));

const violations = [];

lines.forEach((line, idx) => {
  for (const { label, regex } of regexes) {
    if (regex.test(line)) {
      violations.push({ line: idx + 1, label, text: line.trim() });
    }
  }
});

if (violations.length) {
  console.error("Monolith guard failed: domain tests detected in scripts/signal-engine.test.ts");
  for (const v of violations) {
    console.error(`  line ${v.line}: ${v.text}`);
  }
  console.error("Move domain tests to scripts/tests/<domain>.test.ts.");
  process.exit(1);
}

const serverRoot = path.join(__dirname, "..", "server");
const ignoreDirs = new Set(["node_modules", "dist", "build"]);
const tsGuardHits = [];

const walk = (dir) => {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!ignoreDirs.has(entry.name)) {
        walk(fullPath);
      }
      continue;
    }
    if (!entry.isFile()) continue;
    if (!entry.name.endsWith(".ts")) continue;
    const fileText = fs.readFileSync(fullPath, "utf8");
    const lines = fileText.split(/\r?\n/);
    lines.forEach((line, idx) => {
      if (line.includes("@ts-nocheck") || line.includes("@ts-ignore")) {
        tsGuardHits.push({ file: fullPath, line: idx + 1, text: line.trim() });
      }
    });
  }
};

walk(serverRoot);

if (tsGuardHits.length) {
  console.error("Monolith guard failed: @ts-nocheck/@ts-ignore found in server/**");
  tsGuardHits.forEach((hit) => {
    const rel = path.relative(path.join(__dirname, ".."), hit.file);
    console.error(`  ${rel}:${hit.line}: ${hit.text}`);
  });
  process.exit(1);
}

process.exit(0);
