import { chromium } from "playwright"
import fs from "node:fs"

const baseHttp = process.env.UI_HTTP_BASE
const shotDir = process.env.UI_SHOT_DIR
const runId = process.env.RUN_ID
const username = process.env.V2_ADMIN_USER
const password = process.env.V2_ADMIN_PASSWORD
const vsApp = process.env.VS_BOOTSTRAP_APP_NAME
const vsCredential = process.env.VS_BOOTSTRAP_CREDENTIAL
fs.mkdirSync(shotDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const context = await browser.newContext({ ignoreHTTPSErrors: true })
const page = await context.newPage()
const notes = []
function note(name, ok, detail) {
  notes.push({ name, ok, detail })
  console.log(`${ok ? "PASS" : "FAIL"} ui ${name} ${detail}`)
}

async function login(url) {
  await page.goto(url, { waitUntil: "networkidle" })
  if (page.url().includes("/login") || await page.getByLabel("用户名").count()) {
    await page.locator("#username").fill(username)
    await page.locator("#password").fill(password)
    await page.getByRole("button", { name: "使用密码登录" }).click()
    await page.waitForURL((value) => !value.pathname.endsWith("/login"), { timeout: 20000 })
  }
}

try {
  await login(`${baseHttp}/login`)
  await page.goto(`${baseHttp}/data/basic/application`, { waitUntil: "networkidle" })
  const nav = page.getByRole("link", { name: "租户管理" })
  note("sidebar_has_tenant_nav", await nav.count() === 1, `count=${await nav.count()}`)
  await page.screenshot({ path: `${shotDir}/01-application-nav.png`, fullPage: true })
  await nav.click()
  await page.waitForURL(/\/data\/basic\/tenants/)
  const gate = page.getByTestId("tenant-http-gate")
  await gate.waitFor({ timeout: 15000 })
  note("http_gate_blocks_credentials", await page.getByTestId("vs-credential").count() === 0, page.url())
  await page.screenshot({ path: `${shotDir}/02-http-gate.png`, fullPage: true })
  const httpsUrl = await page.locator("p.font-mono").innerText()
  await page.goto(httpsUrl, { waitUntil: "networkidle" })
  await login(page.url())
  if (!page.url().includes("/data/basic/tenants")) {
    await page.goto(httpsUrl, { waitUntil: "networkidle" })
  }
  note("https_page", page.url().startsWith("https://") && page.url().includes("/data/basic/tenants"), page.url())
  await page.reload({ waitUntil: "networkidle" })
  note("https_refresh", await page.getByTestId("tenant-https-page").count() === 1, page.url())
  await page.getByTestId("vs-app").fill(vsApp)
  await page.getByTestId("vs-credential").fill("wrong-credential")
  await page.getByTestId("vs-enter").click()
  await page.getByTestId("tenant-error").waitFor({ timeout: 15000 })
  const authError = await page.getByTestId("tenant-error").innerText()
  note("wrong_credential_error", /HTTP 401/.test(authError), authError.slice(0, 80))
  await page.screenshot({ path: `${shotDir}/03-auth-error.png`, fullPage: true })
  await page.getByTestId("vs-credential").fill(vsCredential)
  await page.getByTestId("vs-enter").click()
  await page.getByText("已占用 / 总配额").waitFor({ timeout: 15000 })
  await page.getByTestId("field-外部租户 ID").fill(`ui-${runId}`)
  await page.getByTestId("field-逻辑名").fill(`ui-${runId}`)
  await page.getByTestId("field-字节配额").fill("1048576")
  await page.getByTestId("field-对象数配额").fill("20")
  await page.getByTestId("vs-create-tenant").click()
  await page.getByText("只显示这一次").waitFor({ timeout: 15000 })
  note("key_shown_once", await page.locator("text=vst_").count() > 0, "plaintext dialog")
  await page.getByTestId("vs-key-dismiss").click()
  await page.getByTestId("tenant-filter").fill(`ui-${runId}`)
  await page.getByText(`ui-${runId}`).first().waitFor({ timeout: 10000 })
  await page.screenshot({ path: `${shotDir}/04-tenant-list.png`, fullPage: true })
  const consolePage = page.getByTestId("tenant-https-page")
  await consolePage.getByRole("button", { name: "详情", exact: true }).click()
  await page.getByTestId("vs-rotate-key").click()
  await page.getByText("只显示这一次").waitFor({ timeout: 15000 })
  await page.getByTestId("vs-key-dismiss").click()
  const listResponse = await page.request.get(`${new URL(httpsUrl).origin}/api/virtual-storage/v1/accounts/${vsApp}/tenants`, {
    headers: { Authorization: `Bearer ${vsCredential}` },
  })
  const tenants = await listResponse.json()
  const current = tenants.find((item) => item.logical_name === `ui-${runId}`)
  await page.request.patch(`${new URL(httpsUrl).origin}/api/virtual-storage/v1/accounts/${vsApp}/tenants/${current.tenant_id}`, {
    headers: { Authorization: `Bearer ${vsCredential}`, "If-Match": String(current.revision), "Content-Type": "application/json" },
    data: { quota_bytes: current.quota_bytes_total, quota_objects: current.quota_objects_total + 1 },
  })
  await consolePage.getByRole("button", { name: "配额", exact: true }).click()
  await page.getByRole("button", { name: "保存", exact: true }).click()
  await page.getByTestId("tenant-error").waitFor({ timeout: 10000 })
  const quotaError = await page.getByTestId("tenant-error").innerText()
  note("quota_version_conflict", /412|版本|变化/.test(quotaError), quotaError.slice(0, 80))
  await page.getByRole("button", { name: "关闭", exact: true }).click()
  await consolePage.getByRole("button", { name: "冻结", exact: true }).click()
  await consolePage.getByRole("button", { name: "解冻", exact: true }).waitFor({ timeout: 10000 })
  await page.getByTestId("vs-data-plane").click()
  await page.getByTestId("tenant-error").waitFor({ timeout: 10000 })
  const frozenError = await page.getByTestId("tenant-error").innerText()
  note("frozen_data_plane_403", /HTTP 403/.test(frozenError), frozenError.slice(0, 80))
  await consolePage.getByRole("button", { name: "解冻", exact: true }).click()
  await consolePage.getByRole("button", { name: "吊销", exact: true }).first().click()
  await page.getByText("已吊销").waitFor({ timeout: 10000 })
  note("rotate_and_revoke", true, "revoked visible")
  await consolePage.getByRole("button", { name: "注销", exact: true }).click()
  await page.getByTestId("tenant-progress").waitFor({ timeout: 10000 })
  await consolePage.getByRole("button", { name: "撤销注销", exact: true }).click({ timeout: 8000 })
  await consolePage.getByRole("button", { name: "解冻", exact: true }).waitFor({ timeout: 10000 })
  note("cancel_retire", true, "returned to frozen")
  const progress = await page.getByTestId("tenant-progress").count()
  const errorBox = await page.getByTestId("tenant-error").count()
  note("lifecycle_feedback", progress + errorBox > 0 || await page.getByText("冻结").count() > 0, `progress=${progress} error=${errorBox}`)
  await page.screenshot({ path: `${shotDir}/05-lifecycle.png`, fullPage: true })
  await page.goto(`${baseHttp}/data/basic/application`, { waitUntil: "networkidle" })
  note("old_application_page", await page.getByRole("link", { name: "应用管理" }).count() === 1, page.url())
  await page.screenshot({ path: `${shotDir}/06-old-application.png`, fullPage: true })
} catch (error) {
  note("ui_exception", false, String(error).slice(0, 300))
  await page.screenshot({ path: `${shotDir}/99-failure.png`, fullPage: true }).catch(() => null)
} finally {
  await context.close()
  await browser.close()
  fs.writeFileSync(`${shotDir}/ui-result.json`, JSON.stringify({ notes }, null, 2))
  if (notes.some((item) => !item.ok)) process.exit(1)
}
