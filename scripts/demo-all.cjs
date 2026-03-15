const { spawn } = require("node:child_process");

const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";
const baseUrl = process.env.DEMO_BASE_URL ?? "http://localhost:5000";
const requireServer = process.env.DEMO_REQUIRE_SERVER ?? "1";

const runMode = (mode, extraEnv = {}) => new Promise((resolve, reject) => {
  const env = {
    ...process.env,
    DEMO_MODE: mode,
    DEMO_BASE_URL: baseUrl,
    DEMO_REQUIRE_SERVER: requireServer,
    ...extraEnv,
  };

  const child = spawn(npmCmd, ["run", "demo:escalations"], {
    stdio: "inherit",
    env,
    shell: true,
  });

  child.on("exit", (code) => {
    if (code && code !== 0) {
      reject(new Error(`demo:escalations (${mode}) exited with code ${code}`));
      return;
    }
    resolve();
  });
});

(async () => {
  try {
    await runMode("baseline", { DEMO_CLEANUP: "1" });
    await runMode("cooldown", { DEMO_CLEANUP: "1" });
    await runMode("dlq", { DEMO_CLEANUP: "1" });
  } catch (error) {
    console.error(String(error));
    process.exitCode = 1;
  }
})();
