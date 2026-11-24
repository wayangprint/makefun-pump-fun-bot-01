// Surge strategy for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// A curve becomes a buy when its activity SURGES: the trade rate in the last
// short window is at least twice the rate over the longer lookback before it,
// with several buys, net SOL flowing in, and the price up a little from the
// window start. Fresh curves are skipped (minAge) and very large curves are
// skipped (maxRqSol): the sweet spot is a coin that just caught attention.
//
// Each position is one fixed buy (no pyramiding). It is sold whole on the
// first of a fixed take-profit, a fixed stop-loss, or a time limit. No
// trailing stop, no fade exit, no big-sell exit: the exits are deliberately
// plain so the entries do the work.
//
// Deterministic by construction: no clock, no randomness, iteration only
// over insertion-ordered maps fed by the batch stream.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Recent window in slots (the surge window). */
  window: number;
  /** Lookback in slots before the window (the baseline rate). */
  lookback: number;
  /** Recent trade rate must be at least this multiple of the baseline rate. */
  surge: number;
  /** Trades in the recent window. */
  minTrades: number;
  /** Buy trades in the recent window. */
  minBuys: number;
  /** Net buy SOL (buys minus sells) over the recent window. */
  minNetSol: number;
  /** Price now over price at the window start, minus 1. */
  minPriceUp: number;
  /** Curve real SOL bounds at entry. */
  minRqSol: number;
  maxRqSol: number;
  /** Slots since the CreateEvent before an entry is allowed. */
  minAge: number;
  /** SOL per position (one fixed buy). */
  stakeSol: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  /** Exits: sell whole at cost * (1 + takeProfit), cost * (1 - stopLoss), or after maxHold slots. */
  takeProfit: number;
  stopLoss: number;
  maxHold: number;
  /** Slots to wait after an exit before the same curve may be bought again. */
  cooldown: number;
  /** Most entries per curve over the run. */
  maxEntries: number;
  /** Most positions open at once. */
  maxOpen: number;
}

export const DEFAULT_PARAMS: Params = {
  window: 20,
  lookback: 100,
  surge: 3,
  minTrades: 4,
  minBuys: 3,
  minNetSol: 0.15,
  minPriceUp: 0.05,
  minRqSol: 1,
  maxRqSol: 80,
  minAge: 200,
  stakeSol: 0.05,
  minBuySol: 0.01,
  buySlipBps: 1500,
  takeProfit: 0.25,
  stopLoss: 0.1,
  maxHold: 600,
  cooldown: 150,
  maxEntries: 2,
  maxOpen: 5,
};

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125;

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  price: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  /** SOL sent in buys for this position (fees included). */
  invested: number;
  exiting: boolean;
  filled: boolean;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  const keep = p.window + p.lookback + 5;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Entry signal on a curve's tape at slot `now`. */
  const entrySignal = (t: Tape, now: number, rqSol: number): boolean => {
    if (rqSol < p.minRqSol || rqSol > p.maxRqSol) return false;
    if (t.created === null || now - t.created < p.minAge) return false;
    const from = now - p.window;
    const baseFrom = now - p.window - p.lookback;
    let trades = 0;
    let buys = 0;
    let net = 0;
    let startPrice = -1;
    let last = 0;
    let baseTrades = 0;
    for (const tr of t.trades) {
      if (tr.slot <= baseFrom) continue;
      if (tr.slot <= from) {
        baseTrades++;
        continue;
      }
      if (startPrice < 0) startPrice = tr.price;
      trades++;
      if (tr.buy) {
        buys++;
        net += tr.sol;
      } else {
        net -= tr.sol;
      }
      last = tr.price;
    }
    if (trades < p.minTrades || buys < p.minBuys) return false;
    if (net < p.minNetSol) return false;
    if (startPrice <= 0) return false;
    if (last / startPrice - 1 < p.minPriceUp) return false;
    const recentRate = trades / p.window;
    const baseRate = baseTrades / p.lookback;
    if (baseRate <= 0) return recentRate > 0;
    if (recentRate < p.surge * baseRate) return false;
    return true;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const traded = new Set<Pubkey>();
      for (const e of batch.events) {
        if (e.event === 'CreateEvent') {
          tapeOf(e.mint).created = e.slot;
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = Number(e.sol_amount) / LAMPORTS;
          t.trades.push({ slot: e.slot, buy: e.is_buy, sol, price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves) });
          traded.add(e.mint);
        }
      }
      for (const mint of traded) {
        const t = tapes.get(mint)!;
        let cut = 0;
        while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
        if (cut > 0) t.trades.splice(0, cut);
      }
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, number>();
      for (const po of state.pending.values()) pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = Number(state.txFeeLamports()) / LAMPORTS;

      // Exits first: fixed take-profit, fixed stop-loss, or time limit.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
            else t.entries--;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const mk = marks.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        let exit = h.exiting;
        if (!exit && value >= cost * (1 + p.takeProfit)) exit = true;
        if (!exit && value <= cost * (1 - p.stopLoss)) exit = true;
        if (!exit && now - h.opened > p.maxHold) exit = true;
        if (!exit) continue;
        h.exiting = true;
        if (pendingByMint.has(mint)) {
          let sellPending = false;
          for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
          if (sellPending) continue;
        }
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      }

      // Then entries: one fixed buy per new position.
      let free = Number(state.freeCashLamports()) / LAMPORTS;
      let slotsLeft = Math.min(p.maxOpen, state.hello.caps.max_open_positions) - state.committedPositionCount();
      for (const mint of traded) {
        if (orders.length >= cap) break;
        if (slotsLeft <= 0) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (held.has(mint) || pendingByMint.has(mint)) continue;
        const t = tapes.get(mint)!;
        if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
        const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
        if (!entrySignal(t, now, rqSol)) continue;
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, p.stakeSol);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        held.set(mint, { opened: now, invested: amount, exiting: false, filled: false });
        t.entries++;
        slotsLeft--;
      }
      return orders;
    },
  };
}
