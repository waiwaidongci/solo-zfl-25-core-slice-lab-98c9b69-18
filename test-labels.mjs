const BASE = process.env.BASE || "http://localhost:3025";
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

// 准备:两个切片,SL-001-A 当前工序为 研磨,再建一个处于 切割 的切片
async function setup() {
  const samples = (await api("/api/samples")).data;
  let s = samples.find(x => x.id === "CORE-001");
  if (!s.slices.find(x => x.id === "SL-T1")) await post(`/api/samples/CORE-001/slices`, { id: "SL-T1", method: "测试" });
  // 把 SL-T1 推进到 切割
  await post(`/api/samples/CORE-001/slices/SL-T1/logs`, { step: "切割", note: "测试推进" });
  return (await api("/api/samples")).data.find(x => x.id === "CORE-001");
}

const run = async () => {
  const sample = await setup();
  console.log("== 1. 批量签发(选模板版本+工序,唯一有序) ==");
  const key1 = "REQ-TEST-" + Date.now();
  const r1 = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"] });
  check("签发 201", r1.status === 201, JSON.stringify(r1.data));
  check("生成 2 枚", r1.data.labels.length === 2);
  const serials = r1.data.labels.map(l => l.serial);
  check("序号有序递增", serials[1] === serials[0] + 1, serials.join(","));
  check("编码唯一", new Set(r1.data.labels.map(l => l.code)).size === 2);
  const [L1, L2] = r1.data.labels; // L1=SL-001_A 研磨, L2=SL-T1 研磨

  console.log("== 2. 同一请求重复签发只返回原批次 ==");
  const r2 = await post("/api/label-batches", { idempotencyKey: key1, templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"] });
  check("重复请求 200 且幂等", r2.status === 200 && r2.data.idempotent === true);
  check("返回原批次", r2.data.id === r1.data.id);
  const labelsAfterDup = (await api("/api/labels")).data.filter(l => l.batchId === r1.data.id);
  check("未新增标签", labelsAfterDup.length === 2, String(labelsAfterDup.length));

  console.log("== 3. 并发签发同幂等键 -> 只生效一次 ==");
  const keyC = "REQ-CONC-" + Date.now();
  const conc = await Promise.all([1, 2, 3, 4].map(() =>
    post("/api/label-batches", { idempotencyKey: keyC, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] })));
  const batchIds = new Set(conc.map(r => r.data.id));
  check("并发签发只产生一个批次", batchIds.size === 1, [...batchIds].join(","));
  const concLabels = (await api("/api/labels")).data.filter(l => l.batchId === [...batchIds][0]);
  check("并发签发只产生一枚标签", concLabels.length === 1, String(concLabels.length));

  console.log("== 4. 批量过大整批失败 ==");
  const tooMany = Array.from({ length: 51 }, (_, i) => `SL-X${i}`);
  const rBig = await post("/api/label-batches", { idempotencyKey: "REQ-BIG-" + Date.now(), templateVersion: 1, step: "研磨", sliceIds: tooMany });
  check("过大返回 400 batch_too_large", rBig.status === 400 && rBig.data.error === "batch_too_large", JSON.stringify(rBig.data));

  console.log("== 5. 序号冲突整批失败(无部分写入) ==");
  const before = (await api("/api/labels")).data.length;
  const rConflict = await post("/api/label-batches", { idempotencyKey: "REQ-CFL-" + Date.now(), templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"], serials: [L1.serial, L1.serial] });
  check("冲突返回 409 serial_conflict", rConflict.status === 409 && rConflict.data.error === "serial_conflict", JSON.stringify(rConflict.data));
  const rConflict2 = await post("/api/label-batches", { idempotencyKey: "REQ-CFL2-" + Date.now(), templateVersion: 1, step: "研磨", sliceIds: ["SL-001-A", "SL-T1"], serials: [L1.serial, 999001] });
  check("与既有编码冲突也 409", rConflict2.status === 409 && rConflict2.data.error === "serial_conflict");
  const after = (await api("/api/labels")).data.length;
  check("失败后标签数不变(整批回滚)", before === after, `${before}->${after}`);

  console.log("== 6. 扫码核销:正常匹配切片+当前工序 ==");
  const okScan = await post("/api/scan", { code: L1.code, sig: L1.sig, sliceId: "SL-001-A", step: "研磨" });
  check("核销成功", okScan.status === 200 && okScan.data.result === "成功", JSON.stringify(okScan.data));

  console.log("== 7. 并发扫码同一标签 -> 只核销一次 ==");
  const scans = await Promise.all([1, 2, 3, 4, 5].map(() =>
    post("/api/scan", { code: L2.code, sig: L2.sig, sliceId: "SL-T1", step: "研磨" })));
  // SL-T1 当前工序是 切割,标签工序是 研磨 -> 全部应跳步失败;改用新标签测并发
  const keyS = "REQ-SCAN-" + Date.now();
  const rScanBatch = await post("/api/label-batches", { idempotencyKey: keyS, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LC = rScanBatch.data.labels[0];
  const concScans = await Promise.all([1, 2, 3, 4, 5].map(() =>
    post("/api/scan", { code: LC.code, sig: LC.sig, sliceId: "SL-T1", step: "切割" })));
  const okCount = concScans.filter(r => r.status === 200).length;
  check("并发扫码仅一次成功", okCount === 1, `成功${okCount}次`);
  check("其余为重复核销失败", concScans.filter(r => r.status === 409).length === 4);
  const lcAfter = (await api("/api/labels")).data.find(l => l.code === LC.code);
  check("标签状态为已核销", lcAfter.status === "已核销");

  console.log("== 8. 跨片扫码失败且状态不变 ==");
  const keyX = "REQ-X-" + Date.now();
  const rX = await post("/api/label-batches", { idempotencyKey: keyX, templateVersion: 1, step: "切割", sliceIds: ["SL-T1"] });
  const LX = rX.data.labels[0];
  const cross = await post("/api/scan", { code: LX.code, sig: LX.sig, sliceId: "SL-001-A", step: "切割" });
  check("跨片 409", cross.status === 409 && /跨片/.test(cross.data.reason), cross.data.reason);
  const lxAfter = (await api("/api/labels")).data.find(l => l.code === LX.code);
  check("跨片失败后标签仍有效", lxAfter.status === "有效");

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

  console.log("== 14. 模板升版只影响新标签,旧批次可查 ==");
  const tpl = await post("/api/label-templates", { name: "荧光薄片标签", prefix: "FLU", ttlDays: 60 });
  check("升版为 v2", tpl.status === 201 && tpl.data.version === 2, JSON.stringify(tpl.data));
  const keyN = "REQ-NEW-" + Date.now();
  const rNew = await post("/api/label-batches", { idempotencyKey: keyN, templateVersion: 2, step: "切割", sliceIds: ["SL-T1"] });
  check("新批次用 v2 模板与前缀", rNew.status === 201 && rNew.data.labels[0].code.startsWith("FLU-") && rNew.data.labels[0].templateVersion === 2, JSON.stringify(rNew.data.labels?.[0]));
  const oldBatch = await api(`/api/label-batches/${r1.data.id}`);
  check("旧批次仍可查", oldBatch.status === 200 && oldBatch.data.templateVersion === 1);
  const oldLabel = (await api("/api/labels")).data.find(l => l.code === L1.code);
  check("旧标签数据未受升版影响", oldLabel && oldLabel.templateVersion === 1 && oldLabel.code.startsWith("LBL-"));

  console.log("== 15. 扫码记录已留痕 ==");
  const logs = (await api("/api/scan-logs")).data;
  check("扫码记录包含成功与失败", logs.some(l => l.result === "成功") && logs.some(l => l.result === "失败"), `共${logs.length}条`);

  console.log(`\n结果: ${passed} 通过, ${failed} 失败`);
  process.exit(failed ? 1 : 0);
};
run().catch(e => { console.error(e); process.exit(1); });
