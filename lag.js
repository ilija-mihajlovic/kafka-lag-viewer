(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory()
  } else {
    root.KafkaLag = factory()
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict'

  const HEADER_WORDS = ['GROUP', 'TOPIC', 'PARTITION', 'CURRENT-OFFSET', 'LOG-END-OFFSET', 'LAG']

  function isHeader(tokens) {
    return HEADER_WORDS.every(word => tokens.includes(word))
  }

  function numberOrNull(value) {
    if (value === undefined || value === '-' || !/^\d+$/.test(value)) return null
    const number = Number(value)
    return Number.isSafeInteger(number) ? number : null
  }

  function parse(text) {
    const rows = []
    const errors = []
    let headerSeen = false

    String(text || '').split(/\r?\n/).forEach((line, index) => {
      const trimmed = line.trim()
      if (!trimmed) return
      const tokens = trimmed.split(/\s+/)
      if (isHeader(tokens)) {
        headerSeen = true
        return
      }
      if (!headerSeen || /^(warning|note|error|current assignment|consumer group)\b/i.test(trimmed)) return
      if (tokens.length < 8) {
        errors.push({ line: index + 1, text: line, reason: 'Not enough columns' })
        return
      }

      const [group, topic, partitionText, currentText, logEndText] = tokens
      const lagColumnPresent = tokens.length >= 9 || tokens[5] === '-' || /^\d+$/.test(tokens[5] || '')
      const lagText = lagColumnPresent ? tokens[5] : '-'
      const consumerId = lagColumnPresent ? tokens[6] : tokens[5]
      const host = lagColumnPresent ? tokens[7] : tokens[6]
      const clientId = lagColumnPresent ? tokens[8] : tokens[7]
      const partition = numberOrNull(partitionText)
      const currentOffset = numberOrNull(currentText)
      const logEndOffset = numberOrNull(logEndText)
      if (!group || !topic || partition === null || currentOffset === null || logEndOffset === null) {
        errors.push({ line: index + 1, text: line, reason: 'Invalid group, topic, partition, or offset' })
        return
      }
      const explicitLag = numberOrNull(lagText)
      const lag = explicitLag === null ? logEndOffset - currentOffset : explicitLag
      rows.push({
        group,
        topic,
        partition,
        currentOffset,
        logEndOffset,
        lag,
        consumerId: consumerId || '-',
        host: host || '-',
        clientId: clientId || '-',
        line: index + 1,
        lagDerived: explicitLag === null
      })
    })

    return { rows, errors, headerSeen }
  }

  function addTotal(map, key, lag) {
    map[key] = (map[key] || 0) + lag
  }

  function sortedTotals(map) {
    return Object.entries(map)
      .map(([name, lag]) => ({ name, lag }))
      .sort((a, b) => b.lag - a.lag || a.name.localeCompare(b.name))
  }

  function summarize(rows) {
    const byTopic = {}
    const byGroup = {}
    rows.forEach(row => {
      addTotal(byTopic, row.topic, row.lag)
      addTotal(byGroup, row.group, row.lag)
    })
    const totalLag = rows.reduce((sum, row) => sum + row.lag, 0)
    const worstPartitions = rows
      .slice()
      .sort((a, b) => b.lag - a.lag || a.group.localeCompare(b.group) || a.topic.localeCompare(b.topic) || a.partition - b.partition)
    const unassigned = rows.filter(row => row.consumerId === '-')
    const positiveLag = Math.max(totalLag, 0)
    const topLag = worstPartitions[0] ? Math.max(worstPartitions[0].lag, 0) : 0
    const advice = []
    if (unassigned.length) {
      advice.push(`${unassigned.length} ${unassigned.length === 1 ? 'partition has' : 'partitions have'} no consumer: the group may have fewer consumers than partitions, or a rebalance may be in progress`)
    }
    if (positiveLag > 0 && topLag >= positiveLag * 0.5 && rows.length > 1) {
      advice.push('Lag is concentrated on one partition: check for a hot key or a stuck consumer')
    }
    if (!rows.length) advice.push('No valid partition rows were found in the pasted output')
    else if (totalLag <= 0) advice.push('No positive lag was found in the parsed partitions')

    return {
      totalLag,
      partitionCount: rows.length,
      groupCount: Object.keys(byGroup).length,
      topicCount: Object.keys(byTopic).length,
      byTopic: sortedTotals(byTopic),
      byGroup: sortedTotals(byGroup),
      worstPartitions,
      unassigned,
      advice
    }
  }

  return { parse, summarize }
})
