// agentfun-surge: a volume-surge breakout bot for the agent.fun pump.fun replay (agent-fun/replay/v2).
// The grader runs `node /bot/main.ts`; the strategy lives in strategy.ts and the SDK shim is the
// kit's sdk/ts/makefun-bot.ts, vendored byte for byte. Tuned on the dev window: the entry needs a
// 3x activity surge with net inflow, and each position exits on a fixed take-profit, stop-loss,
// or time limit.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

await runBot(createStrategy(DEFAULT_PARAMS));
