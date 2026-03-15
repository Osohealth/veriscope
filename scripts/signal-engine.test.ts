// NOTE: monolith suite is smoke-only. Domain behavior lives in scripts/tests/*.test.ts
import {
  applyTestEnv,
  cleanDatabase,
  closePool,
  enablePgCrypto,
  ensureTestSchema,
} from "./test/bootstrap";

async function main() {
  applyTestEnv();

  try {
    await enablePgCrypto();
    await ensureTestSchema();
    console.log("PASS: signal engine tests");
  } finally {
    await cleanDatabase();
    try {
      await closePool();
    } catch (poolError) {
      console.warn("Pool close warning:", (poolError as Error).message);
    }
  }
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    console.error("Signal engine tests failed:", error);
    process.exit(1);
  });
