// Thin Durable Object around routes.ts and robot.ts. One robot per Worker.
import { DurableObject } from "cloudflare:workers";
import { clockFor, Robot, type Store } from "./robot.ts";
import { handle } from "./routes.ts";
import { REAL_BASE, type Deps } from "./tz.ts";

interface Env {
  ROBOT: DurableObjectNamespace<RobotDO>;
  ROBOT_TOKEN?: string;
  ENGINE_URL?: string;
  SITE_ORIGINS?: string;
  TZ_BASE_URL?: string;
  TEST_NOW?: string;
  TEST_EPOCH?: string;
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    const id = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => { clearTimeout(id); resolve(); }, { once: true });
  });

export class RobotDO extends DurableObject<Env> {
  private robot: Robot;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const store: Store = { get: (k) => ctx.storage.get(k), put: (k, v) => ctx.storage.put(k, v) };
    const deps: Deps = { fetch: (u, i) => fetch(u, i), now: clockFor(env), sleep };
    this.robot = new Robot(store, env.TZ_BASE_URL || REAL_BASE, deps, env.ENGINE_URL || null);
  }

  async fetch(request: Request): Promise<Response> {
    const res = await handle(request, this.env, this.robot);
    // Any call may have opened an order: the next tick must be soon.
    await this.ctx.storage.setAlarm(Date.now() + 2000);
    return res;
  }

  async alarm(): Promise<void> {
    const next = await this.robot.tick();
    await this.ctx.storage.setAlarm(Date.now() + next);
  }
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.ROBOT.get(env.ROBOT.idFromName("robot")).fetch(request);
  },
} satisfies ExportedHandler<Env>;
