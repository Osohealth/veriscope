const { spawn } = require("node:child_process");

const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";

const env = {
  ...process.env,
  DEMO_SERVER: "1",
  DEV_ROUTES_ENABLED: "true",
};

const child = spawn(npmCmd, ["run", "dev"], {
  stdio: "inherit",
  env,
  shell: true,
});

child.on("exit", (code) => {
  process.exit(code ?? 0);
});
