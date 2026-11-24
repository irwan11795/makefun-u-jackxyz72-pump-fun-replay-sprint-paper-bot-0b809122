// Steady Sniper — Pump.fun Replay Sprint entrypoint.
//
// Wires the original momentum-scalper strategy in strategy.ts to the
// house replay protocol via the vendored SDK shim (vendor/makefun-bot.ts).
// Run with: node main.ts

import { runBot } from "./vendor/makefun-bot.ts";
import { CFG, SteadySniper } from "./strategy.ts";

/** Fail fast on a misconfigured build instead of trading blind. */
function validateConfig(): void {
  if (!(CFG.takeProfit > 1 && CFG.trailTrigger > 1)) {
    throw new Error("invalid config: takeProfit and trailTrigger must exceed 1");
  }
  if (!(CFG.stopLoss > 0 && CFG.stopLoss < 1)) {
    throw new Error("invalid config: stopLoss must be between 0 and 1");
  }
  if (!(CFG.entryMinRise >= 1 && CFG.entryMaxRise > CFG.entryMinRise)) {
    throw new Error("invalid config: entry rise band is empty");
  }
  if (!(CFG.minBuySol > 0.01)) {
    throw new Error("invalid config: minBuySol must clear the 0.01 SOL round-trip floor");
  }
}

validateConfig();

const sniper = new SteadySniper();

await runBot({
  onBatch(state, batch) {
    sniper.observe(batch);
    return sniper.decide(state, batch);
  },
  onDone() {
    console.error(`steady-sniper exits: ${sniper.exitSummary()}`);
  },
});
