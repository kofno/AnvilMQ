// Aggregates per-scenario load reports from a benchmark run directory into median + min/max
// spread per point, computes the max-sustainable arrival-rate bracket, and judges reproducibility
// on completion throughput (spread max/min > 1.5x is flagged NON-REPRODUCIBLE). End-to-end p99
// tail spread is reported separately and descriptively, since it jitters run-to-run on a shared
// host even when throughput is stable. Reads only the JSON reports written by load.ts; performs
// no runs of its own. Emits summary.json and summary.md into the same directory.
import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

const dir = process.argv[2] ?? process.env.ANVILMQ_BENCH_DIR;
if (!dir) throw new Error("usage: bun summarize.ts <bench-directory>");
const SPREAD_LIMIT = Number(process.env.BENCH_SPREAD_LIMIT ?? 1.5);

type Report = {
  config: { producers: number; workers: number; targetRate: number; durationSeconds: number; warmupSeconds: number; payloadBytes: number; workMs: number };
  passed: boolean; sustained: boolean; boundedBacklog: boolean; keepingUp: boolean;
  backlogSlopePerSecond: number; finalWindowBacklog: number; completionDeficitPerSecond: number;
  enqueuePerSecond: number; completionsPerSecondDuringWindow: number; completionsPerSecondIncludingDrain: number;
  drainSeconds: number; acknowledged: number; completed: number; producerErrors: number; workerErrors: number; duplicates: number;
  latencyMs: Record<"enqueueRpc" | "submissionToHandler" | "submissionToCompletionAck", { p50: number | null; p95: number | null; p99: number | null; max: number | null }>;
  brokerDurability?: { durability?: string };
};

type Rep = { file: string; set: string; label: string; order: number; rep: number; report: Report };

const files = (await readdir(dir)).filter(f => /^(A-rate|B-workers|D-)/.test(f) && f.endsWith(".json"));
const reps: Rep[] = [];
for (const file of files) {
  const report = JSON.parse(await readFile(join(dir, file), "utf8")) as Report;
  let match: RegExpMatchArray | null;
  if ((match = file.match(/^A-rate(\d+)-rep(\d+)\.json$/))) reps.push({ file, set: "A", label: `${match[1]} jobs/s`, order: Number(match[1]), rep: Number(match[2]), report });
  else if ((match = file.match(/^B-workers(\d+)-rep(\d+)\.json$/))) reps.push({ file, set: "B", label: `${match[1]} workers`, order: Number(match[1]), rep: Number(match[2]), report });
  else if ((match = file.match(/^D-([A-Za-z]+)-rep(\d+)\.json$/))) reps.push({ file, set: "D", label: match[1].toUpperCase(), order: match[1].toUpperCase() === "NORMAL" ? 0 : 1, rep: Number(match[2]), report });
}

const median = (xs: number[]) => { const s = [...xs].sort((a, b) => a - b); const n = s.length; return n === 0 ? NaN : n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2; };
const num = (xs: (number | null)[]) => xs.filter((x): x is number => typeof x === "number" && Number.isFinite(x));
function stat(xs: (number | null)[]) {
  const v = num(xs);
  if (!v.length) return { median: null, min: null, max: null, spread: null as number | null };
  const min = Math.min(...v), max = Math.max(...v);
  return { median: median(v), min, max, spread: min > 0 ? max / min : null };
}

type PointSummary = {
  set: string; label: string; order: number; reps: number;
  enqueuePerSecond: ReturnType<typeof stat>; completionsPerSecondDuringWindow: ReturnType<typeof stat>;
  completionsPerSecondIncludingDrain: ReturnType<typeof stat>; endToEndP99Ms: ReturnType<typeof stat>;
  endToEndP50Ms: ReturnType<typeof stat>; endToEndP95Ms: ReturnType<typeof stat>;
  enqueueP50Ms: ReturnType<typeof stat>; enqueueP95Ms: ReturnType<typeof stat>; enqueueP99Ms: ReturnType<typeof stat>;
  backlogSlopePerSecond: ReturnType<typeof stat>; drainSeconds: ReturnType<typeof stat>;
  sustainedCount: number; sustainedMedian: boolean; correctnessAllPassed: boolean;
  throughputReproducible: boolean; tailLatencyReproducible: boolean;
  nonReproducible: boolean; nonReproducibleReasons: string[];
};

const groups = new Map<string, Rep[]>();
for (const r of reps) { const key = `${r.set}|${r.label}`; (groups.get(key) ?? groups.set(key, []).get(key)!).push(r); }

const points: PointSummary[] = [];
for (const [, group] of groups) {
  const rs = group.map(g => g.report);
  const completion = stat(rs.map(r => r.completionsPerSecondDuringWindow));
  const e2e = stat(rs.map(r => r.latencyMs.submissionToCompletionAck.p99));
  const sustainedCount = rs.filter(r => r.sustained).length;
  // Capacity reproducibility is judged on completion throughput (the metric the max-sustainable
  // claim rests on). End-to-end p99 tail spread is reported separately and descriptively: on a
  // shared host the tail jitters run-to-run even when throughput is rock-solid, so a wide tail
  // spread should not by itself invalidate a throughput/capacity result.
  const throughputReproducible = completion.spread === null || completion.spread <= SPREAD_LIMIT;
  const tailLatencyReproducible = e2e.spread === null || e2e.spread <= SPREAD_LIMIT;
  const reasons: string[] = [];
  if (!throughputReproducible) reasons.push(`completion/s spread ${completion.spread!.toFixed(2)}x`);
  if (!tailLatencyReproducible) reasons.push(`end-to-end p99 spread ${e2e.spread!.toFixed(2)}x (tail jitter)`);
  points.push({
    set: group[0].set, label: group[0].label, order: group[0].order, reps: group.length,
    enqueuePerSecond: stat(rs.map(r => r.enqueuePerSecond)), completionsPerSecondDuringWindow: completion,
    completionsPerSecondIncludingDrain: stat(rs.map(r => r.completionsPerSecondIncludingDrain)), endToEndP99Ms: e2e,
    endToEndP50Ms: stat(rs.map(r => r.latencyMs.submissionToCompletionAck.p50)), endToEndP95Ms: stat(rs.map(r => r.latencyMs.submissionToCompletionAck.p95)),
    enqueueP50Ms: stat(rs.map(r => r.latencyMs.enqueueRpc.p50)), enqueueP95Ms: stat(rs.map(r => r.latencyMs.enqueueRpc.p95)), enqueueP99Ms: stat(rs.map(r => r.latencyMs.enqueueRpc.p99)),
    backlogSlopePerSecond: stat(rs.map(r => r.backlogSlopePerSecond)), drainSeconds: stat(rs.map(r => r.drainSeconds)),
    sustainedCount, sustainedMedian: sustainedCount * 2 >= group.length, correctnessAllPassed: rs.every(r => r.passed),
    throughputReproducible, tailLatencyReproducible,
    nonReproducible: !throughputReproducible, nonReproducibleReasons: reasons,
  });
}
points.sort((a, b) => a.set.localeCompare(b.set) || a.order - b.order);

// Max sustainable arrival-rate bracket from Set A (fixed concurrency), by median verdict.
const setA = points.filter(p => p.set === "A").sort((a, b) => a.order - b.order);
const sustainedRates = setA.filter(p => p.sustainedMedian).map(p => p.order);
const sustainableRate = sustainedRates.length ? Math.max(...sustainedRates) : null;
const overloadRate = setA.find(p => !p.sustainedMedian && (sustainableRate === null || p.order > sustainableRate))?.order ?? null;
const maxSustainable = {
  sustainableRateJobsPerSec: sustainableRate,
  firstOverloadRateJobsPerSec: overloadRate,
  ceilingReached: sustainableRate !== null && overloadRate !== null,
  note: sustainableRate === null ? "No tested rate met the bounded-backlog criterion." : overloadRate === null ? "All tested rates stayed sustained; the ceiling was not reached." : `Max sustainable completion throughput is bracketed in (${sustainableRate}, ${overloadRate}) jobs/s: ${sustainableRate} sustained, ${overloadRate} overloaded.`,
};

const f = (x: number | null) => x === null || Number.isNaN(x) ? "n/a" : x.toFixed(2);
const cell = (s: ReturnType<typeof stat>) => `${f(s.median)} (${f(s.min)}–${f(s.max)})`;
function reproCell(p: PointSummary) {
  if (!p.throughputReproducible) return "NON-REPRODUCIBLE: " + p.nonReproducibleReasons.join("; ");
  return p.tailLatencyReproducible ? "yes" : `yes (throughput); wide tail: ${p.nonReproducibleReasons.join("; ")}`;
}
function table(title: string, rows: PointSummary[], variable: string) {
  if (!rows.length) return "";
  const header = `### ${title}\n\n| ${variable} | Enqueue/s | Completion/s (window) | End-to-end p99 ms | Backlog slope j/s | Drain s | Sustained | Reproducible |\n| --- | ---: | ---: | ---: | ---: | ---: | :---: | :---: |\n`;
  const body = rows.map(p => `| ${p.label} | ${cell(p.enqueuePerSecond)} | ${cell(p.completionsPerSecondDuringWindow)} | ${cell(p.endToEndP99Ms)} | ${cell(p.backlogSlopePerSecond)} | ${cell(p.drainSeconds)} | ${p.sustainedMedian ? "yes" : "no"} (${p.sustainedCount}/${p.reps}) | ${reproCell(p)} |`).join("\n");
  return header + body + "\n\n";
}

const summary = { generatedAt: new Date().toISOString(), directory: dir, spreadLimit: SPREAD_LIMIT, repsPerPoint: points[0]?.reps ?? 0, maxSustainable, points };
await writeFile(join(dir, "summary.json"), JSON.stringify(summary, null, 2));

const markdown = `# Benchmark summary\n\nGenerated: ${summary.generatedAt}\n\nEach cell is median (min–max) across ${summary.repsPerPoint} repetitions. The "Reproducible" verdict is judged on completion throughput (spread max/min ≤ ${SPREAD_LIMIT}x), the metric the max-sustainable claim rests on; end-to-end p99 tail spread is reported separately because it jitters run-to-run on a shared host even when throughput is stable. "Sustained" is the bounded-backlog verdict (backlog not accumulating and completion keeping up), by majority of repetitions.\n\n## Max sustainable completion throughput (Set A, fixed concurrency)\n\n${maxSustainable.note}\n\n${table("Set A — arrival-rate sweep at fixed worker concurrency", setA, "Target rate")}${table("Set B — worker-concurrency sweep at fixed arrival rate", points.filter(p => p.set === "B"), "Workers")}${table("Durability comparison at fixed operating point", points.filter(p => p.set === "D"), "Durability")}These are local exploratory measurements on shared Docker Desktop resources, not production SLOs.\n`;
await writeFile(join(dir, "summary.md"), markdown);
console.log(markdown);
