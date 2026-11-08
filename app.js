(() => {
  'use strict'

  const sample = `GROUP           TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID        HOST         CLIENT-ID
orders-workers  orders            0          142              152             10   consumer-1         /10.0.0.8    worker-a
orders-workers  orders            1          98               103             -    -                  -            -
orders-workers  orders            2          204              205             1    consumer-2         /10.0.0.9    worker-b
orders-workers  payments          0          80               80              0    consumer-1         /10.0.0.8    worker-a
WARNING: The group coordinator is loading offsets
inventory       stock             0          12               15              3    consumer-3         /10.0.0.10   worker-c`

  const input = document.querySelector('#input')
  const output = document.querySelector('#output')
  const status = document.querySelector('#status')
  const analyze = document.querySelector('#analyze')
  const loadExample = document.querySelector('#load-example')
  const copy = document.querySelector('#copy')

  const formatNumber = number => new Intl.NumberFormat().format(number)
  const escapeHtml = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]))

  function bar(lag, max) {
    const width = max > 0 ? Math.round(Math.max(lag, 0) / max * 100) : 0
    return `<span class="bar-track" aria-label="${formatNumber(lag)} lag"><span class="bar-fill" style="width:${width}%"></span></span>`
  }

  function totalsTable(title, entries) {
    return `<section class="card"><h3>${title}</h3>${entries.length ? `<table><thead><tr><th scope="col">Name</th><th scope="col">Lag</th></tr></thead><tbody>${entries.map(item => `<tr><td>${escapeHtml(item.name)}</td><td>${formatNumber(item.lag)}</td></tr>`).join('')}</tbody></table>` : '<p class="muted">None found.</p>'}</section>`
  }

  function render(text) {
    const parsed = KafkaLag.parse(text)
    const summary = KafkaLag.summarize(parsed.rows)
    const max = Math.max(...summary.worstPartitions.map(row => row.lag), 0)
    output.innerHTML = `<div class="summary-grid"><div class="metric"><span>Total lag</span><strong>${formatNumber(summary.totalLag)}</strong></div><div class="metric"><span>Partitions</span><strong>${formatNumber(summary.partitionCount)}</strong></div><div class="metric"><span>Groups</span><strong>${formatNumber(summary.groupCount)}</strong></div><div class="metric"><span>Topics</span><strong>${formatNumber(summary.topicCount)}</strong></div></div>
      <div class="cards">${totalsTable('Lag by topic', summary.byTopic)}${totalsTable('Lag by group', summary.byGroup)}</div>
      <section class="card"><h3>Worst partitions</h3>${summary.worstPartitions.length ? `<div class="partition-list">${summary.worstPartitions.slice(0, 10).map(row => `<div class="partition"><div class="partition-title"><strong>${escapeHtml(row.topic)} ${row.partition}</strong><span>${escapeHtml(row.group)} · ${formatNumber(row.lag)}</span></div>${bar(row.lag, max)}</div>`).join('')}</div>` : '<p class="muted">None found.</p>'}</section>
      <section class="card"><h3>Partitions with no consumer</h3>${summary.unassigned.length ? `<ul>${summary.unassigned.map(row => `<li>${escapeHtml(row.group)} · ${escapeHtml(row.topic)} partition ${row.partition}</li>`).join('')}</ul>` : '<p class="muted">All parsed partitions have a consumer assigned.</p>'}</section>
      <section class="card advice"><h3>Advice</h3><ul>${summary.advice.map(item => `<li>${escapeHtml(item)}</li>`).join('')}</ul></section>
      ${parsed.errors.length ? `<details class="card"><summary>Ignored malformed lines (${parsed.errors.length})</summary><ul>${parsed.errors.map(item => `<li>Line ${item.line}: ${escapeHtml(item.reason)}</li>`).join('')}</ul></details>` : ''}`
    status.textContent = `Analyzed ${summary.partitionCount} valid partition${summary.partitionCount === 1 ? '' : 's'}${parsed.errors.length ? ` and ignored ${parsed.errors.length} malformed line${parsed.errors.length === 1 ? '' : 's'}` : ''}.`
  }

  analyze.addEventListener('click', () => render(input.value))
  loadExample.addEventListener('click', () => { input.value = sample; input.focus(); render(sample) })
  copy.addEventListener('click', async () => {
    const parsed = KafkaLag.parse(input.value)
    const summary = KafkaLag.summarize(parsed.rows)
    const markdown = [`# Kafka Lag Summary`, ``, `Total lag: ${summary.totalLag}`, `Partitions: ${summary.partitionCount}`, ``, `## Lag by topic`, ...summary.byTopic.map(item => `- ${item.name}: ${item.lag}`), ``, `## Lag by group`, ...summary.byGroup.map(item => `- ${item.name}: ${item.lag}`), ``, `## Advice`, ...summary.advice.map(item => `- ${item}`)].join('\n')
    try { await navigator.clipboard.writeText(markdown); status.textContent = 'Summary copied as Markdown.' }
    catch { status.textContent = 'Copy is unavailable in this browser. Select the summary from the page instead.' }
  })

  render('')
})()
