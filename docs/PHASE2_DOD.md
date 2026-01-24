# Phase 2 Definition of Done (DoD)

## Smoke test network requirements

Production smoke tests must be run from an **unrestricted network**. If the smoke test is executed from a network that blocks outbound CONNECT tunnels (common in locked-down CI runners), it will exit with code **3** and print a network-block diagnostic. In those environments, validate against `http://localhost:*` or run the smoke test from a network without proxy restrictions.

