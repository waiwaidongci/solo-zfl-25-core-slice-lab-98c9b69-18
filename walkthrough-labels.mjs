// 页面走查:用 jsdom 加载 /labels 页面并执行页面自身脚本,模拟用户完成一次批量签发
// 在隔离实例上运行(独立端口 + 临时数据文件),不触碰正式数据
import { spawn } from "node:child_process";
import { rm } from "node:fs/promises";
import { JSDOM } from "jsdom";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.WALK_PORT || 3126);
const DB = process.env.WALK_DB || `/tmp/labels-walk-${process.pid}.json`;
const BASE = `http://localhost:${PORT}`;

const sleep = ms => new Promise(r => setTimeout(r, ms));
async function waitFor(fn, what, timeout = 8000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const v = await fn();
    if (v) return v;
    await sleep(100);
  }
  throw new Error("等待超时: " + what);
}

await rm(DB, { force: true });
const child = spawn(process.execPath, [join(__dirname, "server.js")], {
  env: { ...process.env, PORT: String(PORT), DB_PATH: DB },
  stdio: "ignore"
});

let failed = 0;
const check = (name, cond, extra = "") => {
  console.log(`  ${cond ? "PASS" : "FAIL"} ${name}${cond ? "" : " " + extra}`);
  if (!cond) { failed = 1; process.exitCode = 1; }
};

try {
  await waitFor(() => fetch(BASE + "/api/samples").then(r => r.ok).catch(() => false), "隔离实例就绪");

  console.log("== 页面走查:加载 /labels 并完成一次批量签发 ==");
  const html = await (await fetch(BASE + "/labels")).text();
  const dom = new JSDOM(html, {
    url: BASE + "/labels",
    runScripts: "dangerously",
    beforeParse(window) {
      // 页面里是相对路径 fetch,接到 Node 的全局 fetch 上
      window.fetch = (path, opts) => fetch(new URL(path, BASE), opts);
    }
  });
  const doc = dom.window.document;

  // 页面脚本加载后渲染出切片勾选框
  await waitFor(() => doc.querySelector("#issueSlices input[type=checkbox]"), "切片列表渲染");
  check("模板下拉已填充", doc.querySelectorAll("#issueTpl option").length >= 1);
  check("签发批次表初始为空", doc.querySelectorAll("#batches tr").length === 0);

  // 模拟用户操作:选模板 v1、工序 研磨、勾选切片、填幂等键、点签发
  doc.querySelector("#issueTpl").value = "1";
  doc.querySelector("#issueStep").value = "研磨";
  const checkbox = doc.querySelector("#issueSlices input[type=checkbox]");
  checkbox.checked = true;
  const sliceId = checkbox.value;
  doc.querySelector("#issueKey").value = "WALK-" + Date.now();
  doc.querySelector("#issueBtn").click();

  // 此前缺陷:提交后页面报 "Cannot read properties of undefined (reading 'id')"
  const msg = await waitFor(() => doc.querySelector("#issueMsg").textContent.trim(), "签发结果提示");
  check("页面显示批次和数量", /批次\s+LB-\d+\s+签发\s+1\s+枚/.test(msg), msg);
  check("无脚本错误提示", !/Cannot read|undefined/.test(msg), msg);

  await waitFor(() => doc.querySelectorAll("#batches tr").length === 1, "批次表刷新");
  check("批次表显示新批次", doc.querySelector("#batches tr").textContent.includes("LB-"));
  check("标签台账出现有效标签", [...doc.querySelectorAll("#labels tr")].some(tr => tr.textContent.includes("有效") && tr.textContent.includes(sliceId)));

  // 同一幂等键再点一次:应提示返回原批次,不新增
  // (签发成功后页面刷新会重建下拉与勾选框,需按原内容重选)
  await waitFor(() => doc.querySelector("#issueSlices input[type=checkbox]"), "切片列表重渲染");
  doc.querySelector("#issueTpl").value = "1";
  doc.querySelector("#issueStep").value = "研磨";
  doc.querySelector("#issueSlices input[type=checkbox]").checked = true;
  doc.querySelector("#issueBtn").click();
  const msg2 = await waitFor(() => {
    const t = doc.querySelector("#issueMsg").textContent;
    return t.includes("重复请求") ? t : null;
  }, "幂等提示");
  check("重复提交提示返回原批次", /重复请求,返回原批次/.test(msg2), msg2);
  await sleep(300);
  check("批次数仍为 1", doc.querySelectorAll("#batches tr").length === 1);

  dom.window.close();
} finally {
  child.kill();
  await rm(DB, { force: true });
}
console.log(failed ? "\n页面走查失败" : "\n页面走查通过");
