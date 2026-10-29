// Opify simulator controller. Inject with the Claude-in-Chrome javascript tool on
// https://www.opify.in/simulator/dashboard?version=web, then call window.opifyUI.<fn>(...).
// Every function is async and returns plain JSON-able data. These are SIMULATED paper orders only.
//
//   await opifyUI.setDateTime('08-01-2026','15:00')  // CHECK .now: the picker sometimes lands 1 min late;
//                                                    // the UI at HH:MM shows the CLOSE of the HH:MM-1 bar -> step('prev',1) if needed
//   await opifyUI.setExpiry('27JAN2026')
//   opifyUI.state()                                   // datetime, spot, expiry, MTM, net delta, theta, vega, margin
//   opifyUI.chain()                                   // [{strike, ce:{ltp,iv,delta,oi}, pe:{...}}]
//   await opifyUI.order('CE','sell',25450,'27JAN2026',520)  // qty in UNITS; one leg per strike
//   await opifyUI.exit('27JAN2026',25450,'CE')        // exit a leg at LTP
//   opifyUI.positions()                               // P&L-tab rows
//   await opifyUI.step('next', 30)                    // minutes: 1,5,30,375(1 day)
//   await opifyUI.goto('09-01-2026','09:46', 12)      // step forward keeping positions; <=12 steps per call (45s tool limit)
//   await opifyUI.clearBook()                          // clear ALL legs (needed before setDateTime)
//   opifyUI.startGoto('06-08-2025','10:16'); opifyUI.job()   // long forward moves as a polled job
(function () {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const vis = e => e && e.offsetParent !== null;
  const num = s => { if (s == null) return null; const t = String(s).replace(/[₹,\s%]/g, ''); const m = t.match(/-?[\d.]+/); if (!m) return null; let v = parseFloat(m[0]); if (/L$/i.test(t)) v *= 1e5; else if (/Cr$/i.test(t)) v *= 1e7; else if (/K$/i.test(t)) v *= 1e3; return v; };
  const setv = (el, v) => {
    const s = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
    el.focus(); s.call(el, v);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    el.blur();
  };
  const text = sel => { const e = [...document.querySelectorAll(sel)].find(vis); return e ? e.textContent.replace(/\s+/g, ' ').trim() : null; };

  async function waitIdle(ms = 8000) {
    const t0 = Date.now();
    await sleep(400);
    while (Date.now() - t0 < ms) {
      const loading = [...document.querySelectorAll('*')].some(e => vis(e) && e.childNodes.length === 1 && /Data is loading|Getting .* data/i.test(e.textContent));
      if (!loading) return true;
      await sleep(300);
    }
    return false;
  }

  function datetime() {
    const el = [...document.querySelectorAll('*')].find(e => vis(e) && /^Datetime:\s*\d{2}-\d{2}-\d{4}/.test((e.textContent || '').trim()) && e.children.length <= 3);
    const m = el && el.textContent.match(/(\d{2}-\d{2}-\d{4})\s+(\d{2}:\d{2})/);
    return m ? { date: m[1], time: m[2] } : null;
  }

  async function openPicker() {
    for (let i = 0; i < 4; i++) {
      const di = [...document.querySelectorAll('input[placeholder="Select date"]')].find(vis);
      if (di) return di;
      if (i % 2 === 0 && typeof show_start_Date_calander === 'function') show_start_Date_calander('show');
      else { const b = [...document.querySelectorAll('button, span, div')].find(e => vis(e) && e.children.length === 0 && /Change Date/.test(e.textContent)); if (b) b.click(); }
      await sleep(700);
    }
    return null;
  }

  async function setDateTime(date, time) {
    if (positions().length) return { ok: false, error: 'book not empty: call clearBook() first' };
    const dt = [...document.querySelectorAll('input[placeholder="Select datetime"]')].find(vis);
    if (dt) for (const t of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) dt.dispatchEvent(new MouseEvent(t, { bubbles: true, cancelable: true, view: window }));
    const di = await openPicker();
    const ti = [...document.querySelectorAll('input[placeholder="Select time"]')].find(vis);
    if (!di || !ti) return { ok: false, error: "date picker not found: real-click 'Change Date' then the 'Select datetime' box (browser tool), then retry" };
    setv(di, date); await sleep(200); setv(ti, time); await sleep(300);
    for (const b of [...document.querySelectorAll('.el-picker-panel__footer button, .el-time-panel__btn.confirm')].filter(vis).reverse()) { b.click(); await sleep(300); }
    await waitIdle();
    await sleep(1500);
    return { ok: true, now: datetime(), expiry: expiry() };
  }

  async function allStrikes() {
    const b = [...document.querySelectorAll('button, span, div, label')].find(e => vis(e) && e.children.length === 0 && e.textContent.trim() === 'All');
    if (b) { b.click(); await sleep(1200); }
    return { ok: !!b };
  }

  function expiry() { const s = document.querySelector('select.css-ofifkflopYTDW'); return s ? s.value : null; }
  function expiries() { const s = document.querySelector('select.css-ofifkflopYTDW'); return s ? [...s.options].map(o => o.value) : []; }

  async function setExpiry(code) {
    const s = document.querySelector('select.css-ofifkflopYTDW');
    if (!s || ![...s.options].some(o => o.value === code)) return { ok: false, error: 'expiry not listed for this date', listed: expiries() };
    s.value = code; s.dispatchEvent(new Event('change', { bubbles: true }));
    await waitIdle(); await sleep(1200);
    return { ok: true, expiry: expiry() };
  }

  function chain() {
    const exp = expiry();
    const rows = [...document.querySelectorAll('tr')].filter(r => exp && r.className.includes(exp) && r.querySelector('.td_strike'));
    const seen = new Set(), out = [];
    for (const r of rows) {
      const k = num(r.className.split(/\s+/).find(c => /^\d{4,6}$/.test(c)));
      if (k == null || seen.has(k)) continue;
      const tds = [...r.children];
      const si = tds.findIndex(c => c.classList.contains('td_strike'));
      const side = arr => { const g = cls => { const c = arr.find(x => x.classList.contains(cls)); return c ? c.textContent.replace(/\s+/g, ' ').trim() : null; }; return { ltp: num((g('td_ltp') || '').split(' ')[0]), iv: num((g('td_iv') || '').split(' ')[0]), delta: num((g('td_delta') || '').split(' ')[0]), oi: num((g('td_oi') || '').split(' ')[0]) }; };
      const ce = side(tds.slice(0, si)), pe = side(tds.slice(si + 1));
      if (ce.ltp == null && pe.ltp == null) continue;
      seen.add(k); out.push({ strike: k, ce, pe });
    }
    return out.sort((a, b) => a.strike - b.strike);
  }

  function state() {
    const strip = {};
    for (const lab of ['MTM', 'NET Δ', 'Θ / DAY', 'VEGA', 'EST. MARGIN', 'ATM', 'ATM IV', 'PCR', 'MAX PAIN', 'TIME LEFT']) {
      const el = [...document.querySelectorAll('*')].find(e => vis(e) && e.children.length === 0 && e.textContent.trim() === lab);
      if (el) { const v = el.parentElement.textContent.replace(lab, '').replace(/\s+/g, ' ').trim(); strip[lab] = v; }
    }
    const head = [...document.querySelectorAll('*')].find(e => vis(e) && e.children.length <= 4 && /^NIFTY\s*[\d,]+\.?\d*/.test(e.textContent.replace(/\s+/g, ' ').trim()));
    const spot = head ? num(head.textContent.replace(/\s+/g, ' ').trim().replace(/^NIFTY\s*/, '').split(' ')[0]) : null;
    return { datetime: datetime(), expiry: expiry(), spot, mtm: num(strip['MTM']), net_delta: num(strip['NET Δ']), theta_day: num(strip['Θ / DAY']), vega: num(strip['VEGA']), est_margin: num(strip['EST. MARGIN']), raw: strip };
  }

  // Native dialogs block the renderer (and the automation): log them instead. confirm() answers NO.
  if (!window.__opifyDialogGuard) {
    window.__opifyDialogGuard = true; window.__dialogs = [];
    window.alert = m => { window.__dialogs.push('alert: ' + m); };
    window.confirm = m => { window.__dialogs.push('confirm: ' + m); return false; };
  }

  // Fire a page action WITHOUT awaiting it: Opify functions may raise blocking dialogs/popups.
  const fire = fn => setTimeout(() => { try { fn(); } catch (e) { console.log('[opifyUI]', e); } }, 5);

  function positions() {
    // rows of the P&L tab: [{expiry, strike, kind, side, qty, entry, exit, ltp, pnl}]
    const out = [];
    for (const b of document.querySelectorAll('[onclick^="exit_order("]')) {
      const m = (b.getAttribute('onclick') || '').match(/exit_order\('(\w+)-(\d+)-(CE|PE)'\)/);
      if (!m) continue;
      let row = b; for (let i = 0; i < 6 && row && row.tagName !== 'TR' && !(row.children && row.children.length >= 6); i++) row = row.parentElement;
      const cells = row ? [...row.children].map(c => c.textContent.replace(/\s+/g, ' ').trim()) : [];
      const side = cells.find(c => /^[BS]$/.test(c));
      const nums = cells.map(num).filter(v => v != null);
      out.push({ expiry: m[1], strike: +m[2], kind: m[3], side: side === 'S' ? 'sell' : side === 'B' ? 'buy' : null, cells });
    }
    return out;
  }

  async function openPnl() {
    // the qty editor only exists while the position view (P&L tab) is open
    const vp = [...document.querySelectorAll('button,a,div,span')].find(e => vis(e) && /^View Position/.test((e.textContent || '').trim()) && e.textContent.trim().length < 20);
    if (vp) { fire(() => vp.click()); await sleep(1200); }
    if (typeof NavPositionTab === 'function') { fire(() => NavPositionTab('pnl')); await sleep(800); }
  }

  async function setQty(exp, strike, kind, qty) {
    await openPnl();
    fire(() => manage_update_popup('qtn', exp, String(strike), kind));
    let inp, btn;
    for (let i = 0; i < 20; i++) {
      await sleep(250);
      inp = [...document.querySelectorAll('input')].find(x => vis(x) && (x.getAttribute('oninput') || '').includes(`'${exp}-${strike}-${kind}','qtn'`));
      btn = [...document.querySelectorAll('button')].find(b => vis(b) && b.getAttribute('onclick') === `change_order_value('qtn','${exp}','${strike}','${kind}')`);
      if (inp && btn) break;
    }
    if (!inp || !btn) return { ok: false, error: 'qty editor not found' };
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(inp, String(qty));
    inp.dispatchEvent(new Event('input', { bubbles: true }));
    fire(() => btn.click());
    await sleep(1200);
    return { ok: true, qty };
  }

  // side: 'buy' | 'sell'; qty in UNITS (lots x lot size). One leg per strike+kind (the site blocks adding to an existing leg).
  async function order(kind, side, strike, exp, qty) {
    exp = exp || expiry(); const sd = side.toLowerCase(), kd = kind.toUpperCase();
    if (positions().some(p => p.expiry === exp && p.strike === +strike && p.kind === kd))
      return { ok: false, error: 'leg already exists for this strike: change its qty instead (site raises a blocking alert on a 2nd click)' };
    const sel = `[onclick="manage_order('new','${kd}','${sd}','${strike}','${exp}')"]`;
    let b = document.querySelector(sel);
    if (!b) { await allStrikes(); b = document.querySelector(sel); }
    if (!b) return { ok: false, error: 'strike not in chain' };
    fire(() => b.click());
    await sleep(1500);
    if (!positions().some(p => p.expiry === exp && p.strike === +strike && p.kind === kd)) {
      // position table only renders when the position view is open
      const vp = [...document.querySelectorAll('button,a,div,span')].find(e => vis(e) && /^View Position/.test((e.textContent || '').trim()) && e.textContent.trim().length < 20);
      if (vp) { fire(() => vp.click()); await sleep(1200); }
    }
    let q = null;
    if (qty) q = await setQty(exp, strike, kd, qty);  // a new leg starts at 1 lot; set the real quantity
    return { ok: true, placed: `${sd} ${qty || 'lot'} ${strike}${kd} ${exp}`, qty: q, state: state() };
  }

  async function clearBook() {
    // the date cannot change while any leg (open or closed) is on the book
    for (let i = 0; i < 4; i++) {
      if (positions().length === 0) return { ok: true };
      fire(() => reset_total_position()); await sleep(1000);
      fire(() => clear_position_warning('yes')); await sleep(1800);
    }
    return { ok: positions().length === 0 };
  }

  // Long operations run as in-page jobs; poll with job() in short tool calls (tool limit 45 s).
  function startGoto(date, time) { window.__opifyJob = null; goto(date, time, 80).then(r => { window.__opifyJob = r; }); return { started: true, now: datetime() }; }
  function job() { return { done: !!window.__opifyJob, result: window.__opifyJob, now: datetime(), state: state() }; }

  async function exit(exp, strike, kind) {
    const id = `${exp}-${strike}-${kind.toUpperCase()}`;
    const b = document.querySelector(`[onclick="exit_order('${id}')"]`);
    if (!b) return { ok: false, error: 'no open leg ' + id };
    fire(() => b.click());
    await sleep(1200);
    return { ok: true, exited: id, state: state() };
  }

  async function step(dir = 'next', minutes = 1) {
    fire(() => manage_simulation(dir, minutes, 'fade', false, true));
    await waitIdle(); await sleep(900);
    return { ok: true, now: datetime(), state: state() };
  }

  function toMin(date, time) { const [d, m, y] = date.split('-').map(Number); const [h, mi] = time.split(':').map(Number); return { day: Date.UTC(y, m - 1, d) / 864e5, min: h * 60 + mi }; }

  // Move forward to (date,time) by stepping, which keeps open positions (setDateTime may restart the session).
  async function goto(date, time, maxSteps = 12) {
    // forward only, keeping positions. Whole-day jumps (1440) when they cannot overshoot, then 30/5/1-min steps.
    const tgt = toMin(date, time);
    for (let i = 0; i < maxSteps; i++) {
      const now = datetime(); if (!now) return { ok: false, error: 'no datetime' };
      const c = toMin(now.date, now.time);
      if (c.day > tgt.day || (c.day === tgt.day && c.min >= tgt.min)) return { ok: true, now, state: state() };
      let jump;
      if (c.day < tgt.day && (tgt.day - c.day >= 2 || c.min <= tgt.min)) jump = 1440;
      else if (c.day < tgt.day) jump = c.min < 15 * 60 + 29 ? Math.min(30, 15 * 60 + 29 - c.min) || 1 : 1;
      else { const gap = tgt.min - c.min; jump = gap >= 30 ? 30 : gap >= 5 ? 5 : 1; }
      const before = JSON.stringify(datetime());
      await step('next', jump);
      const after = datetime();
      if (jump === 1440 && after && (toMin(after.date, after.time).day > tgt.day ||
          (toMin(after.date, after.time).day === tgt.day && toMin(after.date, after.time).min > tgt.min))) {
        await step('prev', 1440);  // overshot (holiday gap): undo and walk instead
        return { ok: false, error: 'day jump overshot; continue with smaller steps', now: datetime() };
      }
      if (JSON.stringify(after) === before) return { ok: false, error: 'time did not advance (end of data?)', now: after };
    }
    return { ok: false, error: 'maxSteps reached - call goto again', now: datetime() };
  }

  async function reset() {
    if (typeof reset_total_position === 'function') fire(() => reset_total_position());
    await sleep(500);
    fire(() => { try { clear_position_warning('yes'); } catch (e) {} });  // only the 'clear this position' confirm
    await sleep(700);
    return { ok: true, state: state() };
  }

  window.opifyUI = { clearBook, startGoto, job, openPnl, setDateTime, allStrikes, setExpiry, expiry, expiries, chain, state, positions, order, setQty, exit, step, goto, reset, datetime, version: '1.4' };
  return 'opifyUI loaded';
})();
