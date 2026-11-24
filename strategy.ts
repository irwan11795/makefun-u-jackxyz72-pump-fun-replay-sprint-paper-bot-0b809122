// Steady Sniper — original momentum scalper for the pump.fun replay sprint.
//
// Idea, from first principles: on a bonding curve almost every fresh coin
// bleeds to zero and a few run. We never predict which one runs; we buy
// SMALL, only after the tape itself confirms buying pressure (several buys,
// buy volume dominating sell volume, price already up from creation but not
// yet chased to the top, still rising over the last few trades), and we exit
// FAST on a take-profit, a stop-loss, a trailing stop, or a max hold time.
// One buy per mint, ever. No clocks, no Math.random: every decision is a
// pure function of the event stream and the batch slot, so two runs agree
// byte for byte.
//
// Fees shape everything: 125 bps per trade plus 105k lamports per order
// sent. A position must be big enough that a 25-30% move dwarfs the
// round-trip cost, but buys are capped at 1% of a mint's real SOL reserve
// per batch, so early mints only admit small tickets.

import type { Batch, BotState, OrderInput } from "./vendor/makefun-bot.ts";

export const LAMPORTS_PER_SOL = 1_000_000_000n;

// ---- tunable parameters (swept via grade-local) ----
export const CFG = {
  posSol: 0.3, // target ticket size in SOL (capped by 1% of real reserve)
  minBuySol: 0.02, // never send a buy smaller than this (fees would eat it)
  minRealSol: 2, // mint must hold at least this much real SOL to enter
  minTrades: 8, // trades observed before a mint is eligible
  minBuyTrades: 5, // of those, at least this many must be buys
  buyVolRatio: 1.5, // buy SOL volume must exceed sell SOL volume by this
  minBuyVolSol: 0, // absolute buy SOL volume required (0 = off)
  minDistinctBuyers: 0, // distinct buying wallets required (0 = off)
  entryMinRise: 1.2, // price must be >= this x the creation price
  entryMaxRise: 2.0, // ...but not above this x (do not chase the top)
  momentumTrades: 4, // momentum measured over the last N trades
  momentumMin: 1.04, // price now must be >= this x the price N trades ago
  maxAgeSlots: 6000, // only fresh mints are eligible
  takeProfit: 1.2, // sell all at +20% vs entry
  stopLoss: 0.9, // sell all at -10% vs entry
  trailTrigger: 1.08, // trailing stop arms once price is +8% vs entry
  trailDrop: 0.97, // ...then sell if price falls 3% from its peak
  maxHoldSlots: 6000, // sell whatever is left after this many slots
  buySlippageBps: 500,
  sellSlippageBps: 1000,
  maxPositions: 5,
} as const;

type MintState = "watching" | "bought" | "done";

interface MintTrack {
  mint: string;
  state: MintState;
  createdSlot: number;
  createPrice: number;
  lastPrice: number;
  // Ring of the last few trade prices (oldest first), for momentum.
  priceHist: number[];
  // Whether each of the last 3 trades was a buy. Entry needs all three
  // buys: sustained demand right now, not a dump in progress.
  lastBuys: boolean[];
  trades: number;
  buyTrades: number;
  buyVolSol: number;
  sellVolSol: number;
  lastSeenSlot: number;
  entryPrice: number;
  entrySlot: number;
  peakPrice: number;
  entrySynced: boolean; // true once entryPrice comes from the real fill cost
  // Distinct wallets that bought (bounded: we only store the first few
  // dozen). A rise driven by 1-2 wallets is usually the creator/bundlers
  // painting the tape, not organic demand.
  buyers: Set<string>;
}

const MAX_TRACKED = 4000;
const PRUNE_AGE_SLOTS = 6000;

export class SteadySniper {
  private tracks = new Map<string, MintTrack>();
  private batchesSeen = 0;
  private exitReasons = new Map<string, number>();

  /** Diagnostic summary of exits by reason (stderr only, never stdout). */
  exitSummary(): string {
    const parts: string[] = [];
    for (const [k, v] of this.exitReasons) parts.push(`${k}=${v}`);
    return parts.length > 0 ? parts.join(" ") : "no-exits";
  }

  /** Fold this batch's events into the per-mint trackers. O(events). */
  observe(batch: Batch): void {
    this.batchesSeen++;
    for (const e of batch.events) {
      if (e.event === "CreateEvent") {
        if (!this.tracks.has(e.mint)) {
          const price = priceOf(
            BigInt(e.virtual_quote_reserves),
            BigInt(e.virtual_token_reserves),
          );
          this.tracks.set(e.mint, {
            mint: e.mint,
            state: "watching",
            createdSlot: e.slot,
            createPrice: price,
            lastPrice: price,
            priceHist: [price],
            lastBuys: [],
            trades: 0,
            buyTrades: 0,
            buyVolSol: 0,
            sellVolSol: 0,
            lastSeenSlot: e.slot,
            entryPrice: 0,
            entrySlot: 0,
            peakPrice: 0,
            entrySynced: false,
            buyers: new Set<string>(),
          });
        }
      } else if (e.event === "TradeEvent") {
        let t = this.tracks.get(e.mint);
        if (t === undefined) {
          // First sight via a trade (create arrived in an earlier batch we
          // may have pruned, or before our first batch): seed from reserves.
          const price = priceOf(
            BigInt(e.virtual_quote_reserves),
            BigInt(e.virtual_token_reserves),
          );
          t = {
            mint: e.mint,
            state: "watching",
            createdSlot: e.slot,
            createPrice: price,
            lastPrice: price,
            priceHist: [price],
            lastBuys: [],
            trades: 0,
            buyTrades: 0,
            buyVolSol: 0,
            sellVolSol: 0,
            lastSeenSlot: e.slot,
            entryPrice: 0,
            entrySlot: 0,
            peakPrice: 0,
            entrySynced: false,
            buyers: new Set<string>(),
          };
          this.tracks.set(e.mint, t);
        }
        const price = priceOf(
          BigInt(e.virtual_quote_reserves),
          BigInt(e.virtual_token_reserves),
        );
        t.trades++;
        const sol = Number(BigInt(e.sol_amount)) / 1e9;
        if (e.is_buy) {
          t.buyTrades++;
          t.buyVolSol += sol;
          if (t.buyers.size < 40) t.buyers.add(e.user);
        } else {
          t.sellVolSol += sol;
        }
        t.lastPrice = price;
        t.lastBuys.push(e.is_buy);
        if (t.lastBuys.length > 3) t.lastBuys.shift();
        t.priceHist.push(price);
        if (t.priceHist.length > CFG.momentumTrades + 1) t.priceHist.shift();
        t.lastSeenSlot = e.slot;
        if (t.state === "bought" && price > t.peakPrice) t.peakPrice = price;
      } else {
        // CompleteEvent: the curve is over; forget the tracker. Any
        // position is force-closed by the harness.
        this.tracks.delete(e.mint);
      }
    }
    if (this.tracks.size > MAX_TRACKED) this.prune(batch.slot);
  }

  /** Decide this batch's orders. Exits first, then entries. */
  decide(state: BotState, batch: Batch): OrderInput[] {
    const orders: OrderInput[] = [];
    const pendingSell = new Set<string>();
    const pendingBuy = new Set<string>();
    for (const p of state.pending.values()) {
      if (p.order.side === "sell") pendingSell.add(p.order.mint);
      else pendingBuy.add(p.order.mint);
    }

    // ---- exits: every held position, checked against TP / SL / trail / age
    for (const [mint, holding] of state.positions) {
      if (pendingSell.has(mint)) continue;
      const t = this.tracks.get(mint);
      if (t !== undefined && !t.entrySynced && holding.cost_lamports > 0n && holding.token_amount > 0n) {
        // Real fill cost beats the decision-time quote: the fill lands two
        // slots later, usually at a worse price while momentum is rising.
        t.entryPrice = Number(holding.cost_lamports) / Number(holding.token_amount);
        t.peakPrice = Math.max(t.peakPrice, t.entryPrice);
        t.entrySynced = true;
      }
      const price = currentPrice(state, mint, t);
      if (price <= 0 || t === undefined || t.entryPrice <= 0) continue;
      const exit = this.exitReason(t, price, batch.slot);
      if (exit !== null) {
        this.exitReasons.set(exit, (this.exitReasons.get(exit) ?? 0) + 1);
        orders.push({
          side: "sell",
          mint,
          token_amount: holding.token_amount,
          max_slippage_bps: CFG.sellSlippageBps,
        });
        t.state = "done";
      }
    }

    // ---- entries: scan this batch's fresh mints only (cheap + timely)
    if (state.committedPositionCount() + pendingBuy.size < CFG.maxPositions) {
      const freeCash = state.freeCashLamports();
      for (const e of batch.events) {
        if (orders.length >= state.hello.caps.max_orders_per_batch) break;
        if (e.event !== "TradeEvent" && e.event !== "CreateEvent") continue;
        const mint = e.mint;
        const t = this.tracks.get(mint);
        if (t === undefined || t.state !== "watching") continue;
        if (state.positions.has(mint) || pendingBuy.has(mint)) continue;
        if (!this.entryEligible(state, t, batch.slot)) continue;
        const cap = state.maxBuyLamports(mint);
        let size = BigInt(Math.floor(CFG.posSol * 1e9));
        if (size > cap) size = (cap * 95n) / 100n;
        if (size < BigInt(Math.floor(CFG.minBuySol * 1e9))) continue;
        if (size > freeCash - state.txFeeLamports()) continue;
        if (size <= 0n) continue;
        orders.push({
          side: "buy",
          mint,
          quote_lamports: size.toString(),
          max_slippage_bps: CFG.buySlippageBps,
        });
        t.state = "bought";
        t.entryPrice = t.lastPrice;
        t.entrySlot = batch.slot;
        t.peakPrice = t.lastPrice;
        pendingBuy.add(mint);
        if (state.committedPositionCount() + pendingBuy.size >= CFG.maxPositions) break;
      }
    }
    return orders;
  }

  /**
   * Why to exit a held position now, or null to hold. Checked in order:
   * take-profit, stop-loss, trailing stop (armed only after +15%), then a
   * hard max-hold so a slow bleed never rides to the flatten.
   */
  private exitReason(t: MintTrack, price: number, slot: number): string | null {
    const ratio = price / t.entryPrice;
    if (ratio >= CFG.takeProfit) return "take-profit";
    if (ratio <= CFG.stopLoss) return "stop-loss";
    if (t.peakPrice >= CFG.trailTrigger * t.entryPrice && price <= CFG.trailDrop * t.peakPrice) {
      return "trailing-stop";
    }
    if (slot - t.entrySlot >= CFG.maxHoldSlots) return "max-hold";
    return null;
  }

  /** Entry filter: momentum confirmed, not chased, liquid enough, fresh. */
  private entryEligible(state: BotState, t: MintTrack, slot: number): boolean {
    if (t.trades < CFG.minTrades) return false;
    if (t.buyTrades < CFG.minBuyTrades) return false;
    if (t.buyVolSol < CFG.buyVolRatio * Math.max(t.sellVolSol, 1e-9)) return false;
    if (t.buyVolSol < CFG.minBuyVolSol) return false;
    if (t.lastBuys.length < 3 || !t.lastBuys[0] || !t.lastBuys[1] || !t.lastBuys[2]) return false;
    if (t.buyers.size < CFG.minDistinctBuyers) return false;
    if (slot - t.createdSlot > CFG.maxAgeSlots) return false;
    const curve = state.curves.get(t.mint);
    if (curve === undefined || curve.complete) return false;
    const realSol = Number(curve.real_quote_reserves) / 1e9;
    if (realSol < CFG.minRealSol) return false;
    if (t.createPrice <= 0 || t.lastPrice <= 0) return false;
    const rise = t.lastPrice / t.createPrice;
    if (rise < CFG.entryMinRise || rise > CFG.entryMaxRise) return false;
    if (t.priceHist.length > CFG.momentumTrades) {
      const old = t.priceHist[0];
      if (old > 0 && t.lastPrice < CFG.momentumMin * old) return false;
    }
    return true;
  }

  private prune(slot: number): void {
    for (const [mint, t] of this.tracks) {
      if (t.state === "bought") continue;
      if (slot - t.lastSeenSlot > PRUNE_AGE_SLOTS) this.tracks.delete(mint);
    }
  }
}

/** Price in SOL per token, as a float ratio of virtual reserves. */
export function priceOf(virtualQuote: bigint, virtualToken: bigint): number {
  if (virtualToken <= 0n) return 0;
  return Number(virtualQuote) / Number(virtualToken);
}

/** Latest price for a mint: live curve reserves first, tracker last. */
function currentPrice(state: BotState, mint: string, t: MintTrack | undefined): number {
  const curve = state.curves.get(mint);
  if (curve !== undefined && curve.virtual_token_reserves > 0n) {
    return priceOf(curve.virtual_quote_reserves, curve.virtual_token_reserves);
  }
  return t !== undefined ? t.lastPrice : 0;
}
