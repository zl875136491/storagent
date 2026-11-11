import { chromium } from "playwright"
import fs from "node:fs"

const baseHttp = process.env.UI_HTTP_BASE
const shotDir = process.env.UI_SHOT_DIR
const runId = process.env.RUN_ID
const username = process.env.V2_ADMIN_USER
const password = process.env.V2_ADMIN_PASSWORD
fs.mkdirSync(shotDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const context = await browser.newContext({ ignoreHTTPSErrors: true, colorScheme: "dark" })
await context.addInitScript(() => localStorage.setItem("cross_storage_theme", "dark"))
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
  note("no_second_login", await page.getByTestId("vs-credential").count() === 0 && await page.getByTestId("vs-role").innerText() === "管理员", "session role")
  await page.getByTestId("vs-account-quota").waitFor({ timeout: 15000 })
  await page.screenshot({ path: `${shotDir}/03-session-account.png`, fullPage: true })
  const tenantName = `ui-${runId}`
  await page.getByTestId("vs-create-tenant").click()
  await page.getByTestId("field-租户名").fill(tenantName)
  await page.getByTestId("vs-create-submit").click()
  await page.getByText("只显示这一次").waitFor({ timeout: 15000 })
  note("key_shown_once", await page.getByTestId("vs-issued-key").innerText().then((value) => value.startsWith("vst_")), "server generated")
  await page.getByTestId("vs-key-dismiss").click()
  await page.getByTestId("tenant-filter").fill(tenantName)
  await page.getByText(tenantName).first().waitFor({ timeout: 10000 })
  await page.screenshot({ path: `${shotDir}/04-tenant-list.png`, fullPage: true })
  await page.getByTestId("tenant-open").click()
  await page.getByTestId("tenant-detail").waitFor()
  await page.screenshot({ path: `${shotDir}/04-detail.png`, fullPage: true })
  const token = await page.evaluate(() => localStorage.getItem("cross_storage_access_token"))
  const accountId = await page.getByTestId("vs-account").inputValue()
  const origin = new URL(page.url()).origin
  const listed = await page.request.get(`${origin}/api/v1/virtual-accounts/${accountId}/tenants`, { headers: { Authorization: `Bearer ${token}` } })
  const tenants = await listed.json()
  const current = tenants.find((item) => item.logical_name === tenantName)
  await page.request.patch(`${origin}/api/v1/virtual-accounts/${accountId}/tenants/${current.tenant_id}`, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    data: { quota_bytes: current.quota_bytes_total, quota_objects: current.quota_objects_total + 1, revision: current.revision },
  })
  await page.getByTestId("vs-save-quota").click()
  await page.getByTestId("tenant-error").waitFor({ timeout: 10000 })
  const quotaError = await page.getByTestId("tenant-error").innerText()
  note("quota_version_conflict", /412|版本|变化/.test(quotaError), quotaError.slice(0, 80))
  await page.getByTestId("vs-rotate-key").click()
  await page.getByText("只显示这一次").waitFor({ timeout: 15000 })
  await page.getByTestId("vs-key-dismiss").click()
  await page.getByTestId("vs-freeze").click()
  await page.getByTestId("vs-confirm-cancel").click()
  note("freeze_cancel_sends_nothing", await page.getByTestId("vs-freeze").count() === 1, "still active")
  await page.getByTestId("vs-freeze").click()
  await page.getByTestId("vs-confirm-accept").click()
  await page.getByTestId("vs-unfreeze").waitFor({ timeout: 10000 })
  await page.getByTestId("vs-data-plane").click()
  await page.getByTestId("tenant-error").waitFor({ timeout: 10000 })
  const frozenError = await page.getByTestId("tenant-error").innerText()
  note("frozen_data_plane_403", /HTTP 403/.test(frozenError), frozenError.slice(0, 80))
  await page.getByTestId("vs-unfreeze").click()
  await page.getByTestId("vs-freeze").waitFor({ timeout: 10000 })
  await page.getByRole("button", { name: "吊销", exact: true }).first().click()
  await page.getByText("已吊销").waitFor({ timeout: 10000 })
  note("rotate_and_revoke", true, "revoked visible")
  await page.getByTestId("vs-retire").waitFor({ timeout: 10000 })
  await page.getByTestId("vs-retire").click()
  await page.getByTestId("vs-confirm-cancel").click()
  note("retire_cancel_sends_nothing", await page.getByTestId("vs-retire").count() === 1, "still present")
  await page.getByTestId("vs-retire").click()
  await page.getByTestId("vs-confirm-accept").click()
  await page.getByTestId("tenant-progress").waitFor({ timeout: 10000 })
  await page.getByTestId("vs-cancel-retire").click({ timeout: 8000 })
  await page.getByTestId("vs-unfreeze").waitFor({ timeout: 10000 })
  note("cancel_retire", true, "returned to frozen")
  const progress = await page.getByTestId("tenant-progress").count()
  const errorBox = await page.getByTestId("tenant-error").count()
  note("lifecycle_feedback", progress + errorBox > 0 || await page.getByText("冻结").count() > 0, `progress=${progress} error=${errorBox}`)
  await page.screenshot({ path: `${shotDir}/05-lifecycle.png`, fullPage: true })
  await page.goto(`${baseHttp}/data/basic/application`, { waitUntil: "networkidle" })
  note("old_application_page", await page.getByRole("link", { name: "应用管理" }).count() === 1, page.url())
  note("application_virtual_field", await page.getByText("虚拟账号管控").count() > 0, "field rendered")
  await page.screenshot({ path: `${shotDir}/06-old-application.png`, fullPage: true })
  const httpsOrigin = new URL(httpsUrl).origin
  async function expectRole(user, pass, roleText, shotName) {
    const extra = await browser.newContext({ ignoreHTTPSErrors: true, colorScheme: "dark" })
    await extra.addInitScript(() => localStorage.setItem("cross_storage_theme", "dark"))
    const other = await extra.newPage()
    await other.goto(`${httpsOrigin}/login`, { waitUntil: "networkidle" })
    await other.locator("#username").fill(user)
    await other.locator("#password").fill(pass)
    await other.getByRole("button", { name: "使用密码登录" }).click()
    await other.waitForURL((value) => !value.pathname.endsWith("/login"), { timeout: 20000 })
    await other.goto(`${httpsOrigin}/data/basic/tenants`, { waitUntil: "networkidle" })
    await other.getByTestId("vs-role").waitFor({ timeout: 15000 })
    const role = await other.getByTestId("vs-role").innerText()
    note(shotName, role === roleText && await other.getByTestId("vs-credential").count() === 0, role)
    if (roleText === "虚拟账号") {
      const token = await other.evaluate(() => localStorage.getItem("cross_storage_access_token"))
      const denied = await other.request.get(`${httpsOrigin}/api/v1/virtual-accounts/acct_not_granted/tenants`, {
        headers: { Authorization: `Bearer ${token}` },
      })
      note("virtual_cross_account_forbidden", denied.status() === 403, String(denied.status()))
    }
    if (roleText === "普通账号") {
      note("plain_empty_state", await other.getByTestId("tenant-empty-role").innerText() === "虚拟账号才可使用租户功能", "empty")
      const denied = await other.request.get(`${httpsOrigin}/api/v1/virtual-accounts/acct_65f8c543be63fc5ce010d11d/tenants`, {
        headers: { Authorization: `Bearer ${await other.evaluate(() => localStorage.getItem("cross_storage_access_token"))}` },
      })
      note("plain_api_forbidden", denied.status() === 403, String(denied.status()))
    }
    await other.screenshot({ path: `${shotDir}/${shotName}.png`, fullPage: true })
    await extra.close()
  }
  if (process.env.UI_PLAIN_USER && process.env.UI_VIRTUAL_USER) {
    await expectRole(process.env.UI_PLAIN_USER, process.env.UI_PLAIN_PASSWORD, "普通账号", "08-plain-user")
    await expectRole(process.env.UI_VIRTUAL_USER, process.env.UI_VIRTUAL_PASSWORD, "虚拟账号", "09-virtual-user")
  }
} catch (error) {
  const visibleError = await page.getByTestId("tenant-error").innerText().catch(() => "")
  note("ui_exception", false, `${String(error).split("\n")[0]} ${visibleError}`.slice(0, 300))
  await page.screenshot({ path: `${shotDir}/99-failure.png`, fullPage: true }).catch(() => null)
} finally {
  await context.close()
  await browser.close()
  fs.writeFileSync(`${shotDir}/ui-result.json`, JSON.stringify({ notes }, null, 2))
  if (notes.some((item) => !item.ok)) process.exit(1)
}
