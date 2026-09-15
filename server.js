import http from "node:http";
import crypto from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = process.env.DB_PATH || join(__dirname, "data", "core-slices.json");
const port = Number(process.env.PORT || 3025);
const statuses = ["待切割", "制片中", "待观察", "已交付"];
const taskSteps = ["取样", "切割", "研磨", "染色", "观察"];
const MAX_LABEL_BATCH = Number(process.env.MAX_LABEL_BATCH || 50);
const MANUAL_VOID_REASONS = ["过期", "损坏"];
const VOID_REASONS = ["过期", "损坏", "重印"];

const seed = {
  samples: [
    {
      id: "CORE-001",
      project: "东岭铜矿薄片",
      borehole: "ZK-17",
      coreBox: "BX-09",
      depth: "128.4-128.8m",
      owner: "陆川",
      status: "制片中",
      delivery: "未交付",
      slices: [
        { id: "SL-001-A", method: "茜素红染色", observation: "", status: "研磨", logs: [{ at: "2026-06-12T10:00:00.000Z", step: "取样", note: "截取含矿化条带位置" }, { at: "2026-06-13T11:20:00.000Z", step: "切割", note: "完成粗切" }] }
      ]
    }
  ],
  labelTemplates: [
    { version: 1, name: "标准切片标签", prefix: "LBL", ttlDays: 30, createdAt: "2026-06-01T00:00:00.000Z" }
  ],
  labelBatches: [],
  labels: [],
  scanLogs: [],
  labelSerial: 0,
  labelSecret: crypto.randomBytes(24).toString("hex")
};

function migrate(db) {
  db.samples = db.samples || [];
  db.labelTemplates = Array.isArray(db.labelTemplates) && db.labelTemplates.length ? db.labelTemplates : seed.labelTemplates.slice();
  db.labelBatches = db.labelBatches || [];
  db.labels = db.labels || [];
  db.scanLogs = db.scanLogs || [];
  db.labelSerial = Number.isInteger(db.labelSerial) ? db.labelSerial : 0;
  db.labelSecret = db.labelSecret || crypto.randomBytes(24).toString("hex");
  return db;
}

async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await writeFile(dbPath, JSON.stringify(seed, null, 2));
  }
  return migrate(JSON.parse(await readFile(dbPath, "utf8")));
}
async function saveDb(db) { await writeFile(dbPath, JSON.stringify(db, null, 2)); }

// 所有写操作串行化,避免并发请求在 await 交错时互相覆盖或重复生效
let writeChain = Promise.resolve();
function withLock(fn) {
  const run = writeChain.then(fn);
  writeChain = run.catch(() => {});
  return run;
}

async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function updateSampleStatus(sample) {
  const sliceStatuses = sample.slices.map(slice => slice.status);
  if (sliceStatuses.length && sliceStatuses.every(step => step === "观察")) sample.status = "待观察";
  if (sample.delivery === "已交付") sample.status = "已交付";
  else if (sliceStatuses.some(step => ["取样", "切割", "研磨", "染色"].includes(step))) sample.status = "制片中";
  else sample.status = "待切割";
}

function findSlice(db, sliceId) {
  for (const sample of db.samples) {
    const slice = sample.slices.find(item => item.id === sliceId);
    if (slice) return { sample, slice };
  }
  return null;
}
function signLabel(db, label) {
  return crypto.createHmac("sha256", db.labelSecret)
    .update([label.code, label.sliceId, label.step, label.templateVersion, label.expiresAt].join("|"))
    .digest("hex").slice(0, 24);
}
function makeLabel(db, { batchId, template, step, sliceId, serial, reprintOf }) {
  const now = new Date();
  const label = {
    code: `${template.prefix}-${String(serial).padStart(5, "0")}`,
    batchId,
    templateVersion: template.version,
    sliceId,
    step,
    serial,
    status: "有效",
    voidReason: null,
    voidedAt: null,
    consumedAt: null,
    reprintOf: reprintOf || null,
    createdAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + template.ttlDays * 86400000).toISOString()
  };
  label.sig = signLabel(db, label);
  return label;
}
function publicLabel(label) { return label; }
// 批次对外视图(不暴露内部请求指纹)
function publicBatch(batch) {
  return { id: batch.id, idempotencyKey: batch.idempotencyKey, templateVersion: batch.templateVersion, step: batch.step, count: batch.count, createdAt: batch.createdAt };
}
function batchView(db, batch) {
  return { ...publicBatch(batch), labels: db.labels.filter(label => label.batchId === batch.id) };
}
// 幂等键绑定请求内容:模板版本 + 工序 + 切片集合 + 显式序号
function requestFingerprint(input, sliceIds) {
  return crypto.createHash("sha256").update(JSON.stringify({
    templateVersion: Number(input.templateVersion),
    step: String(input.step || ""),
    sliceIds,
    serials: Array.isArray(input.serials) ? input.serials.map(Number) : null
  })).digest("hex");
}

const page = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>岩芯样本切片实验室</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --stone:#73706a; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:26px; } main { display:grid; grid-template-columns:390px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; } h2 { margin:0 0 12px; font-size:18px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:68px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:10px 13px; font-weight:700; cursor:pointer; }
    .stats { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:24px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(310px,1fr)); gap:12px; } .card { display:grid; gap:8px; }
    .meta { color:var(--muted); font-size:13px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; }
    .slice { border-top:1px solid var(--line); padding-top:10px; } .toolbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
    .navlink { color:var(--accent); font-weight:700; text-decoration:none; border:1px solid var(--accent); border-radius:6px; padding:9px 12px; }
    @media (max-width:950px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} .stats{grid-template-columns:1fr 1fr;} }
  </style>
</head>
<body>
  <header><div><h1>岩芯样本切片实验室</h1><div class="meta">样本、切片任务、制片步骤和交付</div></div><div class="toolbar"><a class="navlink" href="/labels">标签签发与核销</a><button id="reload">刷新</button></div></header>
  <main>
    <form id="form">
      <h2>创建岩芯样本</h2>
      <label>项目</label><input name="project" required>
      <label>钻孔编号</label><input name="borehole" required>
      <label>岩芯箱号</label><input name="coreBox" required>
      <label>取样深度</label><input name="depth" required>
      <label>负责人</label><input name="owner" required>
      <label>初始切片编号</label><input name="sliceId" required>
      <label>染色方法</label><input name="method" required>
      <button>保存样本</button>
    </form>
    <section>
      <div class="stats" id="stats"></div>
      <div class="grid" id="samples"></div>
    </section>
  </main>
  <script>
    const statuses = ${JSON.stringify(statuses)};
    const steps = ${JSON.stringify(taskSteps)};
    const form = document.querySelector("#form");
    const stats = document.querySelector("#stats");
    const samplesEl = document.querySelector("#samples");
    let samples = [];
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ "Content-Type":"application/json" } } : options);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "请求失败");
      return data;
    }
    function render() {
      stats.innerHTML = statuses.map(s => '<div class="stat"><span>'+s+'</span><strong>'+samples.filter(item => item.status === s).length+'</strong></div>').join("");
      samplesEl.innerHTML = samples.map(sample => '<article class="card"><h3>'+sample.project+'</h3><span class="pill">'+sample.status+'</span><div class="meta">'+sample.borehole+' · '+sample.coreBox+' · '+sample.depth+' · '+sample.owner+'</div><label>新增切片</label><input data-new-slice="'+sample.id+'" placeholder="切片编号"><input data-method="'+sample.id+'" placeholder="染色方法"><button data-add="'+sample.id+'">添加切片</button>'+sample.slices.map(slice => '<div class="slice"><b>'+slice.id+'</b><div class="meta">'+slice.method+' · 当前步骤 '+slice.status+'</div><select data-step="'+sample.id+'|'+slice.id+'">'+steps.map(step => '<option>'+step+'</option>').join("")+'</select><textarea data-note="'+sample.id+'|'+slice.id+'" placeholder="步骤备注或观察结果"></textarea><button data-log="'+sample.id+'|'+slice.id+'">记录步骤</button><div class="meta">'+slice.logs.map(log => log.step+"："+log.note).join(" / ")+'</div></div>').join("")+'<button data-deliver="'+sample.id+'">标记交付</button></article>').join("");
      document.querySelectorAll("[data-step]").forEach(sel => {
        const [sampleId, sliceId] = sel.dataset.step.split("|");
        const slice = samples.find(s => s.id === sampleId).slices.find(s => s.id === sliceId);
        sel.value = slice.status;
      });
      document.querySelectorAll("[data-add]").forEach(btn => btn.onclick = async () => {
        const id = btn.dataset.add;
        await api('/api/samples/'+id+'/slices', { method:'POST', body: JSON.stringify({ id: document.querySelector('[data-new-slice="'+id+'"]').value, method: document.querySelector('[data-method="'+id+'"]').value || "未指定" }) });
        await load();
      });
      document.querySelectorAll("[data-log]").forEach(btn => btn.onclick = async () => {
        const [sampleId, sliceId] = btn.dataset.log.split("|");
        await api('/api/samples/'+sampleId+'/slices/'+sliceId+'/logs', { method:'POST', body: JSON.stringify({ step: document.querySelector('[data-step="'+sampleId+'|'+sliceId+'"]').value, note: document.querySelector('[data-note="'+sampleId+'|'+sliceId+'"]').value || "步骤完成" }) });
        await load();
      });
      document.querySelectorAll("[data-deliver]").forEach(btn => btn.onclick = async () => { await api('/api/samples/'+btn.dataset.deliver+'/deliver', { method:'POST', body: JSON.stringify({}) }); await load(); });
    }
    async function load(){ samples = await api("/api/samples"); render(); }
    document.querySelector("#reload").onclick = load;
    form.onsubmit = async event => {
      event.preventDefault();
      await api("/api/samples", { method:"POST", body: JSON.stringify(Object.fromEntries(new FormData(form).entries())) });
      form.reset(); await load();
    };
    load();
  </script>
</body>
</html>`;

const labelsPage = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>切片标签签发与核销</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --danger:#a04434; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:16px; flex-wrap:wrap; }
    h1 { margin:0; font-size:24px; } h2 { margin:0 0 12px; font-size:17px; } main { padding:22px 28px; display:grid; gap:18px; }
    .panel { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; }
    .cols { display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:18px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:9px 12px; font-weight:700; cursor:pointer; }
    button.ghost { background:#fff; color:var(--accent); border:1px solid var(--accent); } button.danger { background:var(--danger); }
    table { width:100%; border-collapse:collapse; font-size:13px; } th,td { text-align:left; padding:7px 8px; border-bottom:1px solid var(--line); vertical-align:top; } th { color:var(--muted); font-weight:600; }
    .meta { color:var(--muted); font-size:12px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 8px; font-size:12px; }
    .ok { color:var(--accent); font-weight:700; } .bad { color:var(--danger); font-weight:700; } .msg { margin-top:8px; font-size:13px; }
    .navlink { color:var(--accent); font-weight:700; text-decoration:none; border:1px solid var(--accent); border-radius:6px; padding:9px 12px; }
    .scroll { max-height:340px; overflow:auto; } .row { display:flex; gap:8px; align-items:center; } .row > * { flex:1; }
  </style>
</head>
<body>
  <header><div><h1>切片标签签发与扫码核销</h1><div class="meta">按批次签发唯一有序标签,扫码核销匹配切片与当前工序</div></div><div class="row" style="flex:0"><a class="navlink" href="/">返回样本工作台</a><button class="ghost" id="reload">刷新</button></div></header>
  <main>
    <div class="cols">
      <section class="panel">
        <h2>标签模板(升版只影响新标签)</h2>
        <div id="templates"></div>
        <label>模板名称</label><input id="tplName" placeholder="如:荧光薄片标签 v2">
        <div class="row"><div><label>编码前缀</label><input id="tplPrefix" placeholder="如 FLU"></div><div><label>有效期(天)</label><input id="tplTtl" type="number" value="30"></div></div>
        <div class="msg"><button id="createTpl">升版新模板</button></div>
        <div class="msg" id="tplMsg"></div>
      </section>
      <section class="panel">
        <h2>批量签发标签</h2>
        <label>模板版本</label><select id="issueTpl"></select>
        <label>工序</label><select id="issueStep"></select>
        <label>切片(勾选,每片一枚,有序生成)</label><div class="scroll" id="issueSlices"></div>
        <label>幂等键(同一请求重复提交只返回原批次)</label><input id="issueKey" placeholder="留空自动生成">
        <div class="msg"><button id="issueBtn">签发批次</button></div>
        <div class="msg" id="issueMsg"></div>
      </section>
      <section class="panel">
        <h2>扫码核销</h2>
        <label>标签编码</label><input id="scanCode" placeholder="如 LBL-00001">
        <label>签名(防篡改)</label><input id="scanSig" placeholder="标签上的校验码">
        <label>切片编号</label><select id="scanSlice"></select>
        <label>工序</label><select id="scanStep"></select>
        <div class="msg"><button id="scanBtn">扫码核销</button></div>
        <div class="msg" id="scanMsg"></div>
      </section>
    </div>
    <section class="panel">
      <h2>签发批次</h2>
      <div class="scroll"><table><thead><tr><th>批次</th><th>模板版本</th><th>工序</th><th>数量</th><th>幂等键</th><th>签发时间</th></tr></thead><tbody id="batches"></tbody></table></div>
    </section>
    <section class="panel">
      <h2>标签台账(有效 / 已核销 / 已作废及原因)</h2>
      <div class="scroll"><table><thead><tr><th>编码</th><th>签名</th><th>切片</th><th>工序</th><th>批次</th><th>状态</th><th>作废原因</th><th>过期时间</th><th>操作</th></tr></thead><tbody id="labels"></tbody></table></div>
    </section>
    <section class="panel">
      <h2>扫码记录</h2>
      <div class="scroll"><table><thead><tr><th>时间</th><th>编码</th><th>切片</th><th>工序</th><th>结果</th><th>说明</th></tr></thead><tbody id="scans"></tbody></table></div>
    </section>
  </main>
  <script>
    const steps = ${JSON.stringify(taskSteps)};
    let templates = [], batches = [], labels = [], scans = [], samples = [];
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ "Content-Type":"application/json" } } : options);
      const data = await res.json();
      if (!res.ok) { const err = new Error(data.error || "请求失败"); err.data = data; throw err; }
      return data;
    }
    const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;" }[c]));
    function fill() {
      document.querySelector("#issueTpl").innerHTML = templates.map(t => '<option value="'+t.version+'">v'+t.version+" · "+esc(t.name)+"("+esc(t.prefix)+")</option>").join("");
      document.querySelector("#issueStep").innerHTML = steps.map(s => "<option>"+s+"</option>").join("");
      document.querySelector("#scanStep").innerHTML = steps.map(s => "<option>"+s+"</option>").join("");
      const allSlices = samples.flatMap(sm => sm.slices.map(sl => ({ id: sl.id, status: sl.status })));
      document.querySelector("#issueSlices").innerHTML = allSlices.map(sl => '<label style="color:var(--ink)"><input type="checkbox" style="width:auto" value="'+esc(sl.id)+'"> '+esc(sl.id)+' <span class="meta">当前工序 '+esc(sl.status)+"</span></label>").join("") || '<div class="meta">暂无切片</div>';
      document.querySelector("#scanSlice").innerHTML = allSlices.map(sl => '<option value="'+esc(sl.id)+'">'+esc(sl.id)+"</option>").join("");
    }
    function render() {
      document.querySelector("#templates").innerHTML = templates.map(t => '<div class="meta">v'+t.version+" · "+esc(t.name)+" · 前缀 "+esc(t.prefix)+" · 有效 "+t.ttlDays+" 天 · "+esc(t.createdAt.slice(0,10))+"</div>").join("");
      document.querySelector("#batches").innerHTML = batches.map(b => "<tr><td>"+esc(b.id)+"</td><td>v"+b.templateVersion+"</td><td>"+esc(b.step)+"</td><td>"+b.count+"</td><td class='meta'>"+esc(b.idempotencyKey)+"</td><td class='meta'>"+esc(b.createdAt.replace("T"," ").slice(0,19))+"</td></tr>").join("");
      document.querySelector("#labels").innerHTML = labels.map(l => {
        const cls = l.status === "有效" ? "ok" : (l.status === "已作废" ? "bad" : "");
        const ops = l.status === "有效"
          ? '<button class="danger" data-void="'+esc(l.code)+'">作废</button> <button class="ghost" data-reprint="'+esc(l.code)+'">重印</button>'
          : (l.reprintOf ? '<span class="meta">重印自 '+esc(l.reprintOf)+"</span>" : "");
        return "<tr><td><b>"+esc(l.code)+"</b></td><td class='meta'>"+esc(l.sig)+"</td><td>"+esc(l.sliceId)+"</td><td>"+esc(l.step)+"</td><td class='meta'>"+esc(l.batchId)+"</td><td class='"+cls+"'>"+l.status+"</td><td>"+(l.voidReason ? esc(l.voidReason) : "")+"</td><td class='meta'>"+esc(l.expiresAt.slice(0,10))+"</td><td>"+ops+"</td></tr>";
      }).join("");
      document.querySelector("#scans").innerHTML = scans.map(s => "<tr><td class='meta'>"+esc(s.at.replace("T"," ").slice(0,19))+"</td><td>"+esc(s.code)+"</td><td>"+esc(s.sliceId)+"</td><td>"+esc(s.step)+"</td><td class='"+(s.result === "成功" ? "ok" : "bad")+"'>"+s.result+"</td><td>"+esc(s.reason || "")+"</td></tr>").join("");
      document.querySelectorAll("[data-void]").forEach(btn => btn.onclick = async () => {
        const reason = prompt("作废原因:损坏 或 过期", "损坏");
        if (!reason) return;
        try { await api("/api/labels/void", { method:"POST", body: JSON.stringify({ code: btn.dataset.void, reason }) }); await load(); }
        catch (e) { alert(e.message); }
      });
      document.querySelectorAll("[data-reprint]").forEach(btn => btn.onclick = async () => {
        try { const r = await api("/api/labels/reprint", { method:"POST", body: JSON.stringify({ code: btn.dataset.reprint }) }); alert("旧码已作废,新标签:"+r.label.code); await load(); }
        catch (e) { alert(e.message); }
      });
    }
    async function load() {
      [templates, batches, labels, scans, samples] = await Promise.all([
        api("/api/label-templates"), api("/api/label-batches"), api("/api/labels"), api("/api/scan-logs"), api("/api/samples")
      ]);
      fill(); render();
    }
    document.querySelector("#reload").onclick = load;
    document.querySelector("#createTpl").onclick = async () => {
      const msg = document.querySelector("#tplMsg");
      try {
        const t = await api("/api/label-templates", { method:"POST", body: JSON.stringify({ name: document.querySelector("#tplName").value, prefix: document.querySelector("#tplPrefix").value, ttlDays: Number(document.querySelector("#tplTtl").value) }) });
        msg.innerHTML = '<span class="ok">已升版到 v'+t.version+"</span>"; await load();
      } catch (e) { msg.innerHTML = '<span class="bad">'+esc(e.message)+"</span>"; }
    };
    document.querySelector("#issueBtn").onclick = async () => {
      const msg = document.querySelector("#issueMsg");
      const keyInput = document.querySelector("#issueKey");
      if (!keyInput.value.trim()) keyInput.value = "REQ-"+Date.now()+"-"+Math.floor(Math.random()*1e6);
      const sliceIds = [...document.querySelectorAll("#issueSlices input:checked")].map(cb => cb.value);
      try {
        const r = await api("/api/label-batches", { method:"POST", body: JSON.stringify({ idempotencyKey: keyInput.value.trim(), templateVersion: Number(document.querySelector("#issueTpl").value), step: document.querySelector("#issueStep").value, sliceIds }) });
        msg.innerHTML = '<span class="ok">批次 '+esc(r.batch.id)+" 签发 "+r.labels.length+" 枚"+(r.idempotent ? "(重复请求,返回原批次)" : "")+"</span>"; await load();
      } catch (e) { msg.innerHTML = '<span class="bad">'+esc(e.message)+"</span>"; }
    };
    document.querySelector("#scanBtn").onclick = async () => {
      const msg = document.querySelector("#scanMsg");
      try {
        const r = await api("/api/scan", { method:"POST", body: JSON.stringify({ code: document.querySelector("#scanCode").value.trim(), sig: document.querySelector("#scanSig").value.trim(), sliceId: document.querySelector("#scanSlice").value, step: document.querySelector("#scanStep").value }) });
        msg.innerHTML = '<span class="ok">核销成功:'+esc(r.label.code)+"</span>"; await load();
      } catch (e) { msg.innerHTML = '<span class="bad">'+esc(e.data && e.data.reason || e.message)+"</span>"; await load(); }
    };
    load();
  </script>
</body>
</html>`;

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "Content-Type":"text/html; charset=utf-8" });
      return res.end(page);
    }
    if (req.method === "GET" && url.pathname === "/labels") {
      res.writeHead(200, { "Content-Type":"text/html; charset=utf-8" });
      return res.end(labelsPage);
    }
    if (req.method === "GET" && url.pathname === "/api/samples") return sendJson(res, 200, (await loadDb()).samples);

    // ---------- 标签模板:升版只新增版本,旧版本与旧批次保留可查 ----------
    if (req.method === "GET" && url.pathname === "/api/label-templates") return sendJson(res, 200, (await loadDb()).labelTemplates);
    if (req.method === "POST" && url.pathname === "/api/label-templates") {
      const input = await body(req);
      if (!input.name || !input.name.trim()) return sendJson(res, 400, { error: "模板名称必填" });
      if (!input.prefix || !/^[A-Za-z][A-Za-z0-9-]{0,9}$/.test(input.prefix.trim())) return sendJson(res, 400, { error: "编码前缀需为 1-10 位字母数字" });
      const ttlDays = Number(input.ttlDays);
      if (!Number.isInteger(ttlDays) || ttlDays < 1 || ttlDays > 3650) return sendJson(res, 400, { error: "有效期需为 1-3650 天" });
      return withLock(async () => {
        const db = await loadDb();
        const version = Math.max(...db.labelTemplates.map(t => t.version)) + 1;
        const template = { version, name: input.name.trim(), prefix: input.prefix.trim().toUpperCase(), ttlDays, createdAt: new Date().toISOString() };
        db.labelTemplates.push(template);
        await saveDb(db);
        return sendJson(res, 201, template);
      });
    }

    // ---------- 批量签发:幂等 + 整批原子 ----------
    if (req.method === "GET" && url.pathname === "/api/label-batches") {
      const db = await loadDb();
      return sendJson(res, 200, db.labelBatches.map(publicBatch));
    }
    const batchMatch = url.pathname.match(/^\/api\/label-batches\/([^/]+)$/);
    if (batchMatch && req.method === "GET") {
      const db = await loadDb();
      const batch = db.labelBatches.find(item => item.id === batchMatch[1]);
      if (!batch) return sendJson(res, 404, { error: "batch_not_found" });
      return sendJson(res, 200, batchView(db, batch));
    }
    if (req.method === "POST" && url.pathname === "/api/label-batches") {
      const input = await body(req);
      const idempotencyKey = String(input.idempotencyKey || "").trim();
      if (!idempotencyKey) return sendJson(res, 400, { error: "idempotency_key_required" });
      const sliceIds = Array.isArray(input.sliceIds) ? input.sliceIds.map(s => String(s).trim()).filter(Boolean) : [];
      if (!sliceIds.length) return sendJson(res, 400, { error: "empty_batch" });
      if (sliceIds.length > MAX_LABEL_BATCH) return sendJson(res, 400, { error: "batch_too_large", max: MAX_LABEL_BATCH });
      if (new Set(sliceIds).size !== sliceIds.length) return sendJson(res, 400, { error: "duplicate_slice_in_batch" });
      return withLock(async () => {
        const db = await loadDb();
        const fingerprint = requestFingerprint(input, sliceIds);
        const existing = db.labelBatches.find(item => item.idempotencyKey === idempotencyKey);
        if (existing) {
          // 幂等键绑定请求内容:内容不一致即冲突,失败且不新增
          if (existing.requestHash && existing.requestHash !== fingerprint) {
            return sendJson(res, 409, { error: "idempotency_conflict", batchId: existing.id });
          }
          // 同一请求重复签发:只返回原批次
          return sendJson(res, 200, { idempotent: true, batch: publicBatch(existing), labels: db.labels.filter(label => label.batchId === existing.id) });
        }
        const template = db.labelTemplates.find(item => item.version === Number(input.templateVersion));
        if (!template) return sendJson(res, 400, { error: "template_not_found" });
        if (!taskSteps.includes(input.step)) return sendJson(res, 400, { error: "invalid_step" });
        for (const sliceId of sliceIds) {
          if (!findSlice(db, sliceId)) return sendJson(res, 404, { error: "slice_not_found", sliceId });
        }
        // 序号:可显式指定(用于校验冲突),否则按全局序号递增;任何冲突整批失败
        let serials;
        if (Array.isArray(input.serials)) {
          if (input.serials.length !== sliceIds.length) return sendJson(res, 400, { error: "serials_length_mismatch" });
          serials = input.serials.map(Number);
          if (serials.some(s => !Number.isInteger(s) || s < 0)) return sendJson(res, 400, { error: "invalid_serial" });
          if (new Set(serials).size !== serials.length) return sendJson(res, 409, { error: "serial_conflict" });
        } else {
          serials = sliceIds.map((_, i) => db.labelSerial + 1 + i);
        }
        const codes = serials.map(serial => `${template.prefix}-${String(serial).padStart(5, "0")}`);
        if (codes.some(code => db.labels.some(label => label.code === code))) {
          return sendJson(res, 409, { error: "serial_conflict" }); // 未做任何写入,整批失败
        }
        const batch = {
          id: `LB-${String(db.labelBatches.length + 1).padStart(4, "0")}`,
          idempotencyKey,
          requestHash: fingerprint,
          templateVersion: template.version,
          step: input.step,
          count: sliceIds.length,
          createdAt: new Date().toISOString()
        };
        const labels = sliceIds.map((sliceId, i) => makeLabel(db, { batchId: batch.id, template, step: input.step, sliceId, serial: serials[i] }));
        db.labelBatches.push(batch);
        db.labels.push(...labels);
        db.labelSerial = Math.max(db.labelSerial, ...serials);
        await saveDb(db);
        return sendJson(res, 201, { idempotent: false, batch: publicBatch(batch), labels });
      });
    }

    // ---------- 标签台账 / 作废 / 重印 ----------
    if (req.method === "GET" && url.pathname === "/api/labels") return sendJson(res, 200, (await loadDb()).labels.map(publicLabel));
    if (req.method === "POST" && url.pathname === "/api/labels/void") {
      const input = await body(req);
      if (!MANUAL_VOID_REASONS.includes(input.reason)) return sendJson(res, 400, { error: "作废原因需为:" + MANUAL_VOID_REASONS.join("/") });
      return withLock(async () => {
        const db = await loadDb();
        const label = db.labels.find(item => item.code === input.code);
        if (!label) return sendJson(res, 404, { error: "label_not_found" });
        if (label.status !== "有效") return sendJson(res, 409, { error: "label_not_active", status: label.status });
        label.status = "已作废";
        label.voidReason = input.reason;
        label.voidedAt = new Date().toISOString();
        await saveDb(db);
        return sendJson(res, 200, label);
      });
    }
    if (req.method === "POST" && url.pathname === "/api/labels/reprint") {
      const input = await body(req);
      return withLock(async () => {
        const db = await loadDb();
        const label = db.labels.find(item => item.code === input.code);
        if (!label) return sendJson(res, 404, { error: "label_not_found" });
        if (label.status !== "有效") return sendJson(res, 409, { error: "label_not_active", status: label.status });
        const template = db.labelTemplates.find(item => item.version === label.templateVersion);
        label.status = "已作废";
        label.voidReason = "重印";
        label.voidedAt = new Date().toISOString();
        const serial = db.labelSerial + 1;
        const fresh = makeLabel(db, { batchId: label.batchId, template, step: label.step, sliceId: label.sliceId, serial, reprintOf: label.code });
        db.labels.push(fresh);
        db.labelSerial = serial;
        await saveDb(db);
        return sendJson(res, 201, { voided: label, label: fresh });
      });
    }

    // ---------- 扫码核销:签名校验 + 匹配切片 + 当前工序,失败状态不变 ----------
    if (req.method === "POST" && url.pathname === "/api/scan") {
      const input = await body(req);
      return withLock(async () => {
        const db = await loadDb();
        const record = { at: new Date().toISOString(), code: String(input.code || ""), sliceId: String(input.sliceId || ""), step: String(input.step || ""), result: "失败", reason: "" };
        const fail = async (status, reason) => {
          record.reason = reason;
          db.scanLogs.unshift(record);
          await saveDb(db);
          return sendJson(res, status, { error: "scan_failed", reason });
        };
        const label = db.labels.find(item => item.code === record.code);
        if (!label) return fail(404, "标签不存在");
        if (!input.sig || input.sig !== label.sig || label.sig !== signLabel(db, label)) return fail(409, "篡改校验失败");
        if (label.status === "已作废") return fail(409, `标签已作废(${label.voidReason})`);
        if (label.status === "已核销") return fail(409, "标签已核销,不得重复");
        if (Date.now() > Date.parse(label.expiresAt)) {
          label.status = "已作废";
          label.voidReason = "过期";
          label.voidedAt = record.at;
          return fail(409, "标签已过期,已自动作废");
        }
        if (label.sliceId !== record.sliceId) return fail(409, "跨片扫码:标签不属于该切片");
        if (label.step !== record.step) return fail(409, "工序不匹配:标签工序为 " + label.step);
        const found = findSlice(db, record.sliceId);
        if (!found) return fail(404, "切片不存在");
        if (found.slice.status !== label.step) return fail(409, `跳步校验失败:切片当前工序为 ${found.slice.status}`);
        label.status = "已核销";
        label.consumedAt = record.at;
        record.result = "成功";
        record.reason = "核销成功";
        db.scanLogs.unshift(record);
        await saveDb(db);
        return sendJson(res, 200, { result: "成功", label });
      });
    }
    if (req.method === "GET" && url.pathname === "/api/scan-logs") return sendJson(res, 200, (await loadDb()).scanLogs);

    // ---------- 原有样本 / 切片 / 交付入口(保留) ----------
    if (req.method === "POST" && url.pathname === "/api/samples") {
      const input = await body(req);
      return withLock(async () => {
        const db = await loadDb();
        const sample = { id: `CORE-${Date.now()}`, project: input.project, borehole: input.borehole, coreBox: input.coreBox, depth: input.depth, owner: input.owner, status: "待切割", delivery: "未交付", slices: [{ id: input.sliceId, method: input.method, observation: "", status: "取样", logs: [{ at: new Date().toISOString(), step: "取样", note: "创建初始切片任务" }] }] };
        updateSampleStatus(sample);
        db.samples.unshift(sample);
        await saveDb(db);
        return sendJson(res, 201, sample);
      });
    }
    const addSlice = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices$/);
    if (addSlice && req.method === "POST") {
      const input = await body(req);
      return withLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === addSlice[1]);
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        sample.slices.push({ id: input.id, method: input.method || "未指定", observation: "", status: "取样", logs: [{ at: new Date().toISOString(), step: "取样", note: "新增切片任务" }] });
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 201, sample);
      });
    }
    const logMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/logs$/);
    if (logMatch && req.method === "POST") {
      const input = await body(req);
      return withLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === logMatch[1]);
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        const slice = sample.slices.find(item => item.id === logMatch[2]);
        if (!slice) return sendJson(res, 404, { error: "slice_not_found" });
        slice.status = input.step;
        if (input.step === "观察") slice.observation = input.note || slice.observation;
        slice.logs.push({ at: new Date().toISOString(), step: input.step, note: input.note || "" });
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 200, sample);
      });
    }
    const deliverMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/deliver$/);
    if (deliverMatch && req.method === "POST") {
      return withLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === deliverMatch[1]);
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        sample.delivery = "已交付";
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 200, sample);
      });
    }
    sendJson(res, 404, { error: "not_found" });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
});

server.listen(port, () => console.log(`Core slice lab app listening on http://localhost:${port}`));
