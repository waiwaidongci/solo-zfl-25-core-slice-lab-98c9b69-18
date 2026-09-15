// 验收脚本:独立端口 + 临时数据文件起私有实例,每次运行互不影响,不触碰正式数据
import { spawn } from "node:child_process";
import { rm, readFile, writeFile } from "node:fs/promises";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.TEST_PORT || 3125);
const DB = process.env.TEST_DB || `/tmp/labels-accept-${process.pid}.json`;
const BASE = `http://localhost:${PORT}`;

let passed = 0, failed = 0;
function check(name, cond, extra = "") {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.log(`  FAIL ${name} ${extra}`); }
}
async function api(path, options = {}) {
  const res = await fetch(BASE + path, options.body
    ? { ...options, headers: { "Content-Type": "application/json" } }
    : options);
  const data = await res.json();
  return { status: res.status, data };
}
const post = (path, body) => api(path, { method: "POST", body: JSON.stringify(body) });
const labelCount = async () => (await api("/api/labels")).data.length;

async function setup() {
  // 种子数据:CORE-001 / SL-001-A(研磨);再建 SL-T1 并推进到 切割
  await post("/api/samples/CORE-001/slices", { id: "SL-T1", method: "测试" });
  await post("/api/samples/CORE-001/slices/SL-T1/logs", { step: "切割", note: "测试推进" });
}

const run = async () => {
  await setup();

  console.log("== 1. 批量签发(选模板版本+工序,唯一有序) ==");
  const key1 = "REQ-TEST-" + Date.now();
  const r1 = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"] });
  check("签发 201", r1.status === 201, JSON.stringify(r1.data));
  check("响应含批次 id 和数量(页面展示所需)", r1.data.batch && r1.data.batch.id && r1.data.batch.count === 2, JSON.stringify(r1.data.batch));
  check("生成 2 枚", r1.data.labels.length === 2);
  const serials = r1.data.labels.map(l => l.serial);
  check("序号有序递增", serials[1] === serials[0] + 1, serials.join(","));
  check("编码唯一", new Set(r1.data.labels.map(l => l.code)).size === 2);
  const [L1, L2] = r1.data.labels;

  console.log("== 2. 同一请求重复签发只返回原批次 ==");
  const r2 = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"] });
  check("重复请求 200 且幂等", r2.status === 200 && r2.data.idempotent === true);
  check("返回原批次", r2.data.batch && r2.data.batch.id === r1.data.batch.id);
  check("未新增标签", (await api("/api/labels")).data.filter(l => l.batchId === r1.data.batch.id).length === 2);

  console.log("== 2b. 同幂等键不同内容 -> 冲突失败且不新增 ==");
  const before2b = await labelCount();
  const diffSlice = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A"] });
  check("换切片 409 idempotency_conflict", diffSlice.status === 409 && diffSlice.data.error === "idempotency_conflict", JSON.stringify(diffSlice.data));
  const diffStep = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 1, step: "切割", sliceIds: ["SL-001-A", "SL-T1"] });
  check("换工序 409", diffStep.status === 409 && diffStep.data.error === "idempotency_conflict");
  const diffTpl = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 2, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"] });
  check("换模板 409", diffTpl.status === 409 && diffTpl.data.error === "idempotency_conflict");
  check("冲突后标签数不变", (await labelCount()) === before2b);
  check("冲突后批次数不变", (await api("/api/label-batches")).data.length === 1);

  console.log("== 3. 并发签发同幂等键 -> 只生效一次 ==");
  const keyC = "REQ-CONC-" + Date.now();
  const conc = await Promise.all([1, 2, 3, 4].map(() =>
    post("/api/label-batches", { idempotencyKey: keyC, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] })));
  const batchIds = new Set(conc.map(r => r.data.batch && r.data.batch.id));
  check("并发签发只产生一个批次", batchIds.size === 1, [...batchIds].join(","));
  check("并发签发只产生一枚标签", (await api("/api/labels")).data.filter(l => l.batchId === [...batchIds][0]).length === 1);

  console.log("== 4. 批量过大整批失败 ==");
  const tooMany = Array.from({ length: 51 }, (_, i) => `SL-X${i}`);
  const rBig = await post("/api/label-batches", { idempotencyKey: "REQ-BIG-" + Date.now(), templateVersion: 1, step: "研磨", sliceIds: tooMany });
  check("过大返回 400 batch_too_large", rBig.status === 400 && rBig.data.error === "batch_too_large", JSON.stringify(rBig.data));

  console.log("== 5. 序号冲突整批失败(无部分写入) ==");
  const before = await labelCount();
  const rConflict = await post("/api/label-batches", { idempotencyKey: "REQ-CFL-" + Date.now(), templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"], serials: [L1.serial, L1.serial] });
  check("冲突返回 409 serial_conflict", rConflict.status === 409 && rConflict.data.error === "serial_conflict", JSON.stringify(rConflict.data));
  const rConflict2 = await post("/api/label-batches", { idempotencyKey: "REQ-CFL2-" + Date.now(), templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"], serials: [L1.serial, 999001] });
  check("与既有编码冲突也 409", rConflict2.status === 409 && rConflict2.data.error === "serial_conflict");
  check("失败后标签数不变(整批回滚)", (await labelCount()) === before);

  console.log("== 6. 扫码核销:正常匹配切片+当前工序 ==");
  const okScan = await post("/api/scan", { code: L1.code, sig: L1.sig, sliceId: "SL-001-A", step: "研磨" });
  check("核销成功", okScan.status === 200 && okScan.data.result === "成功", JSON.stringify(okScan.data));

  console.log("== 7. 并发扫码同一标签 -> 只核销一次 ==");
  const keyS = "REQ-SCAN-" + Date.now();
  const rScanBatch = await post("/api/label-batches", { idempotencyKey: keyS, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LC = rScanBatch.data.labels[0];
  const concScans = await Promise.all([1, 2, 3, 4, 5].map(() =>
    post("/api/scan", { code: LC.code, sig: LC.sig, sliceId: "SL-T1", step: "切割" })));
  const okCount = concScans.filter(r => r.status === 200).length;
  check("并发扫码仅一次成功", okCount === 1, `成功${okCount}次`);
  check("其余为重复核销失败", concScans.filter(r => r.status === 409).length === 4);
  check("标签状态为已核销", (await api("/api/labels")).data.find(l => l.code === LC.code).status === "已核销");

  console.log("== 8. 跨片扫码失败且状态不变 ==");
  const keyX = "REQ-X-" + Date.now();
  const rX = await post("/api/label-batches", { idempotencyKey: keyX, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LX = rX.data.labels[0];
  const cross = await post("/api/scan", { code: LX.code, sig: LX.sig, sliceId: "SL-001-A", step: "切割" });
  check("跨片 409", cross.status === 409 && /跨片/.test(cross.data.reason), cross.data.reason);
  check("跨片失败后标签仍有效", (await api("/api/labels")).data.find(l => l.code === LX.code).status === "有效");

  console.log("== 9. 跳步扫码失败 ==");
  const skip = await post("/api/scan", { code: L2.code, sig: L2.sig, sliceId: "SL-T1", step: "研磨" });
  check("工序已推进的标签扫码 409(跳步)", skip.status === 409 && /跳步/.test(skip.data.reason), skip.data.reason);

  console.log("== 10. 篡改校验失败 ==");
  const tamper = await post("/api/scan", { code: LX.code, sig: "deadbeefdeadbeefdeadbeef", sliceId: "SL-T1", step: "切割" });
  check("伪造签名 409", tamper.status === 409 && /篡改/.test(tamper.data.reason), tamper.data.reason);
  const tamper2 = await post("/api/scan", { code: LX.code.replace(/\d+$/, "99999"), sig: LX.sig, sliceId: "SL-T1", step: "切割" });
  check("篡改编码 404/409", [404, 409].includes(tamper2.status));

  console.log("== 11. 重印:旧码作废,新码可用,旧码扫码失败 ==");
  const rp = await post("/api/labels/reprint", { code: LX.code });
  check("重印 201", rp.status === 201, JSON.stringify(rp.data));
  check("旧码作废原因为重印", rp.data.voided.status === "已作废" && rp.data.voided.voidReason === "重印");
  const oldScan = await post("/api/scan", { code: LX.code, sig: LX.sig, sliceId: "SL-T1", step: "切割" });
  check("旧码扫码失败", oldScan.status === 409 && /已作废/.test(oldScan.data.reason), oldScan.data.reason);
  const newScan = await post("/api/scan", { code: rp.data.label.code, sig: rp.data.label.sig, sliceId: "SL-T1", step: "切割" });
  check("新码核销成功", newScan.status === 200, JSON.stringify(newScan.data));

  console.log("== 12. 并发重印同一标签 -> 只生效一次 ==");
  const keyR = "REQ-R-" + Date.now();
  const rR = await post("/api/label-batches", { idempotencyKey: keyR, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LR = rR.data.labels[0];
  const concRp = await Promise.all([1, 2, 3].map(() => post("/api/labels/reprint", { code: LR.code })));
  check("并发重印仅一次 201", concRp.filter(r => r.status === 201).length === 1, concRp.map(r => r.status).join(","));
  check("其余 409", concRp.filter(r => r.status === 409).length === 2);

  console.log("== 13. 手动作废(损坏)后扫码失败 ==");
  const keyV = "REQ-V-" + Date.now();
  const rV = await post("/api/label-batches", { idempotencyKey: keyV, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LV = rV.data.labels[0];
  const voided = await post("/api/labels/void", { code: LV.code, reason: "损坏" });
  check("作废成功", voided.status === 200 && voided.data.voidReason === "损坏");
  const voidScan = await post("/api/scan", { code: LV.code, sig: LV.sig, sliceId: "SL-T1", step: "切割" });
  check("作废标签扫码 409", voidScan.status === 409 && /已作废/.test(voidScan.data.reason));

  console.log("== 13b. 过期标签扫码自动作废 ==");
  const keyE = "REQ-E-" + Date.now();
  const rE = await post("/api/label-batches", { idempotencyKey: keyE, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LE = rE.data.labels[0];
  // 在隔离数据文件上把过期时间改到过去并重签名(模拟时间流逝)
  const db = JSON.parse(await readFile(DB, "utf8"));
  const target = db.labels.find(l => l.code === LE.code);
  target.expiresAt = new Date(Date.now() - 86400000).toISOString();
  target.sig = crypto.createHmac("sha256", db.labelSecret)
    .update([target.code, target.sliceId, target.step, target.templateVersion, target.expiresAt].join("|"))
    .digest("hex").slice(0, 24);
  await writeFile(DB, JSON.stringify(db, null, 2));
  const expScan = await post("/api/scan", { code: LE.code, sig: target.sig, sliceId: "SL-T1", step: "切割" });
  check("过期扫码 409 并提示过期", expScan.status === 409 && /过期/.test(expScan.data.reason), expScan.data.reason);
  const leAfter = (await api("/api/labels")).data.find(l => l.code === LE.code);
  check("过期自动作废且原因正确", leAfter.status === "已作废" && leAfter.voidReason === "过期");

  console.log("== 14. 模板升版只影响新标签,旧批次可查 ==");
  const tplsBefore = (await api("/api/label-templates")).data;
  const expectVersion = Math.max(...tplsBefore.map(t => t.version)) + 1;
  const tpl = await post("/api/label-templates", { name: "荧光薄片标签", prefix: "FLU", ttlDays: 60 });
  check(`升版为 v${expectVersion}`, tpl.status === 201 && tpl.data.version === expectVersion, JSON.stringify(tpl.data));
  const keyN = "REQ-NEW-" + Date.now();
  const rNew = await post("/api/label-batches", { idempotencyKey: keyN, templateVersion: tpl.data.version, step: "切割", sliceIds: ["SL-T1"] });
  check("新批次用新模板与前缀", rNew.status === 201 && rNew.data.labels[0].code.startsWith("FLU-") && rNew.data.labels[0].templateVersion === tpl.data.version, JSON.stringify(rNew.data.labels?.[0]));
  const oldBatch = await api(`/api/label-batches/${r1.data.batch.id}`);
  check("旧批次仍可查", oldBatch.status === 200 && oldBatch.data.templateVersion === 1);
  const oldLabel = (await api("/api/labels")).data.find(l => l.code === L1.code);
  check("旧标签数据未受升版影响", oldLabel && oldLabel.templateVersion === 1 && oldLabel.code.startsWith("LBL-"));

  console.log("== 15. 扫码记录已留痕 ==");
  const logs = (await api("/api/scan-logs")).data;
  check("扫码记录包含成功与失败", logs.some(l => l.result === "成功") && logs.some(l => l.result === "失败"), `共${logs.length}条`);

  console.log(`\n结果: ${passed} 通过, ${failed} 失败`);
  if (failed) process.exitCode = 1;
};

// ---- 启动隔离实例,跑完清理 ----
await rm(DB, { force: true });
const child = spawn(process.execPath, [join(__dirname, "server.js")], {
  env: { ...process.env, PORT: String(PORT), DB_PATH: DB },
  stdio: "ignore"
});
try {
  let ready = false;
  for (let i = 0; i < 60 && !ready; i++) {
    try { const r = await fetch(BASE + "/api/samples"); ready = r.ok; } catch { await new Promise(r => setTimeout(r, 200)); }
  }
  if (!ready) throw new Error("隔离实例启动失败");
  await run();
} finally {
  child.kill();
  await rm(DB, { force: true });
}
