# Kafka Lag Viewer

Kafka Lag Viewer is a small single page tool for inspecting pasted output from `kafka-consumer-groups.sh --describe`. It runs in the browser with no build step, dependencies, network calls, or Kafka connection.

## Use it

Open `index.html` in a browser. Paste the output of a command such as `kafka-consumer-groups.sh --bootstrap-server ... --describe --group G`, then select **Analyze**. Select **Load example** to see clearly labelled sample data.

The viewer accepts the standard column header, blank lines, warning lines, dash values for unassigned partitions, and multiple groups or topics in one paste. It shows totals by topic and group, the worst partitions, plain bars, unassigned partitions, and advice. Use **Copy summary as Markdown** to copy a short report.

## Test it

Run the tests with:

```sh
node --test
```

## Advice rules

If one partition has at least half of the total positive lag and there is more than one parsed partition, the viewer says that lag is concentrated on one partition. This is a prompt to check for a hot key or a stuck consumer.

If one or more rows have `CONSUMER-ID` equal to `-`, the viewer reports how many partitions have no consumer. This can mean the group has fewer consumers than partitions or that a rebalance is in progress.

The viewer reports when no positive lag is found. It does not diagnose the cause of lag.

## Limits

This tool reads text pasted into the page. It does not connect to Kafka, validate broker metadata, or send data anywhere. It expects whitespace separated output with numeric partition and offset columns. Malformed rows are skipped and shown as ignored lines.

## License

MIT. Copyright Ilija Mihajlovic.
