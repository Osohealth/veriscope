const { spawnSync } = require("node:child_process");

const iterations = Number.parseInt(process.env.ESCALATION_STRESS_RUNS ?? "10", 10);
const npmCmd = process.env.npm_execpath ? process.execPath : (process.platform === "win32" ? "npm.cmd" : "npm");

for (let i = 1; i <= iterations; i += 1) {
  const start = Date.now();
  const args = process.env.npm_execpath
    ? [process.env.npm_execpath, "run", "test:escalations:domain"]
    : ["run", "test:escalations:domain"];
  const result = spawnSync(npmCmd, args, {
    stdio: "inherit",
    env: process.env,
  });

  if (result.error) {
    console.error(result.error);
  }
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }

  const elapsedMs = Date.now() - start;
  console.log(`TEST_STEP: escalations stress ${i}/${iterations} (${elapsedMs}ms)`);
}
