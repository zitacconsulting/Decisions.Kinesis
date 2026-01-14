# Decisions Kinesis Module

A concise Decisions module to integrate Amazon Kinesis Data Streams for real-time message processing.

## Features

- Stream management and flexible authentication (default, static, assume role)
- Message processing with checkpointing, retries, and shard lease management
- JSON payload filtering with common comparison and numeric operators
- **Enhanced Fan‑Out (EFO)** support (SubscribeToShard streaming)

## Quick Start

1. Install the module via Decisions Portal (System > Administration > Features).
2. Add `Decisions.Kinesis` to your Project modules.
3. Configure Kinesis settings (Manage > Jobs & Events > Kinesis Settings).
4. Create a Kinesis Queue (Manage > Jobs & Events > Queues) and bind a Queue Handler.

## Key Configuration (high level)

- `StreamName` — Kinesis stream name
- `InitialStreamPosition` — `Start from oldest` | `Start from latest`
- `MaxRecordsPerRequest`, `MaxRetries`, `ErrorBackoffTime`, `ShardPollInterval`, `ShardBatchWaitTime`

### Enhanced Fan‑Out (EFO)

- `UseEnhancedFanOut` (boolean) — enable streaming via SubscribeToShard
- When `UseEnhancedFanOut = true`, provide either `ConsumerArn` or `ConsumerName` (one is required)

Note: With EFO the module subscribes to shard events (push-style streaming). Without EFO it uses periodic GetRecords polling. Checkpointing and lease handling remain the same.

## JSON Payload Filters

Example filter:

```json
{
  "property": "user.type",
  "verb": "Equals",
  "value": "premium"
}
```

Supported comparisons: `Equals`, `Not Equals`, `Contains`, `Starts With`, `Ends With`, `Greater Than`, `Less Than`, etc.

## Error Handling & Retries

Exponential backoff with jitter for throughput/limit/5xx errors. Handled exceptions include `ProvisionedThroughputExceededException`, `LimitExceededException`, and 5xx server errors.

## Monitoring

Standard log levels: `ERROR`, `WARN`, `INFO`, `DEBUG`.

## Disclaimer

This module is provided "as is" without warranties of any kind. Use it at your own risk. The authors, maintainers, and contributors disclaim all liability for any direct, indirect, incidental, special, or consequential damages, including data loss or service interruption, arising from the use of this software.

## Requirements

- Decisions Platform 9+
- AWS credentials with access to target Kinesis stream

## Installation

1. Download or compile the module and upload it via Decisions Portal (System > Administration > Features).
2. Configure system and queue settings as needed, then create queues and handlers.

## Changelog

- Added: Enhanced Fan‑Out (EFO) support — new queue settings `UseEnhancedFanOut`, `ConsumerArn`/`ConsumerName` and streaming processing.
