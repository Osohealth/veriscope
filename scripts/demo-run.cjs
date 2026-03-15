const { spawn } = require("node:child_process");

const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";
const mode = process.argv[2] || "baseline";
const requireServer = process.env.DEMO_REQUIRE_SERVER ?? "1";
const baseUrl = process.env.DEMO_BASE_URL ?? "http://localhost:5000";

const env = {
  ...process.env,
  DEMO_MODE: mode,
  DEMO_REQUIRE_SERVER: requireServer,
  DEMO_BASE_URL: baseUrl,
};

const child = spawn(npmCmd, ["run", "demo:escalations"], {
  stdio: "inherit",
  env,
  shell: true,
});

child.on("exit", (code) => {
  process.exit(code ?? 0);
});
