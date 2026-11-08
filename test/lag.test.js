const test = require('node:test')
const assert = require('node:assert/strict')
const { parse, summarize } = require('../lag.js')

const header = 'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID'
const row = 'orders orders 0 10 15 5 consumer-1 /host client-1'

test('parses a standard row', () => assert.deepEqual(parse(`${header}\n${row}`).rows[0], { group: 'orders', topic: 'orders', partition: 0, currentOffset: 10, logEndOffset: 15, lag: 5, consumerId: 'consumer-1', host: '/host', clientId: 'client-1', line: 2, lagDerived: false }))
test('handles tabs and repeated headers', () => assert.equal(parse(`${header.replaceAll(' ', '\t')}\n${row.replaceAll(' ', '\t')}\n${header}\n${row}`).rows.length, 2))
test('ignores blank lines and warning lines', () => assert.equal(parse(`WARNING: coordinator\n\n${header}\n${row}`).rows.length, 1))
test('ignores status lines before and after data', () => assert.equal(parse(`Consumer group 'x' has no active members\n${header}\n${row}\nCurrent assignment`).rows.length, 1))
test('derives lag when lag is a dash', () => assert.equal(parse(`${header}\norders orders 0 10 18 - - - -`).rows[0].lag, 8))
test('derives lag when lag column is absent', () => assert.equal(parse(`${header}\norders orders 0 10 18 consumer-1 /host client-1`).rows[0].lag, 8))
test('keeps a zero lag value', () => assert.equal(parse(`${header}\norders orders 0 10 10 0 c h i`).rows[0].lag, 0))
test('marks dash consumer as unassigned', () => assert.equal(parse(`${header}\norders orders 0 10 15 5 - - -`).rows[0].consumerId, '-'))
test('rejects malformed and non numeric rows', () => { const result = parse(`${header}\nshort\norders orders x 10 15 5 c h i`); assert.equal(result.rows.length, 0); assert.equal(result.errors.length, 2) })
test('supports multiple groups and topics', () => { const result = parse(`${header}\norders a 0 0 4 4 c h i\nshipments b 1 2 9 7 c h i`); assert.equal(result.rows.length, 2); assert.equal(new Set(result.rows.map(r => r.group)).size, 2) })
test('summarizes total lag', () => assert.equal(summarize([{ group: 'g', topic: 't', partition: 0, lag: 4 }, { group: 'g', topic: 't', partition: 1, lag: 3 }]).totalLag, 7))
test('summarizes topic and group totals', () => { const result = summarize([{ group: 'g1', topic: 't', partition: 0, lag: 4 }, { group: 'g2', topic: 't', partition: 1, lag: 3 }, { group: 'g1', topic: 'u', partition: 0, lag: 2 }]); assert.deepEqual(result.byTopic, [{ name: 't', lag: 7 }, { name: 'u', lag: 2 }]); assert.deepEqual(result.byGroup, [{ name: 'g1', lag: 6 }, { name: 'g2', lag: 3 }]) })
test('ranks worst partitions with deterministic ties', () => { const result = summarize([{ group: 'b', topic: 'z', partition: 0, lag: 2 }, { group: 'a', topic: 'z', partition: 1, lag: 2 }]); assert.equal(result.worstPartitions[0].group, 'a') })
test('finds partitions without consumers', () => assert.equal(summarize([{ group: 'g', topic: 't', partition: 0, lag: 1, consumerId: '-' }]).unassigned.length, 1))
test('advises on concentrated lag', () => assert.match(summarize([{ group: 'g', topic: 't', partition: 0, lag: 8 }, { group: 'g', topic: 't', partition: 1, lag: 1 }]).advice.join(' '), /concentrated/))
test('advises on unassigned partitions', () => assert.match(summarize([{ group: 'g', topic: 't', partition: 0, lag: 0, consumerId: '-' }]).advice.join(' '), /no consumer/))
test('advises when there are no valid rows', () => assert.match(summarize([]).advice[0], /No valid/))
