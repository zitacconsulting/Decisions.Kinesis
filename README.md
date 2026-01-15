# Decisions Kinesis Module

> ⚠️ **Important:** Use this module at your own risk. See the **Disclaimer** section below.

## Overview

**Decisions Kinesis Module** is a comprehensive integration module for the Decisions no-code automation platform that enables real-time message processing with Amazon Kinesis Data Streams. It provides queue management capabilities with advanced features like Enhanced Fan-Out (EFO), JSON payload filtering, automatic checkpointing, and shard lease management.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration Options](#configuration-options)
- [JSON Payload Filters](#json-payload-filters)
- [Enhanced Fan-Out (EFO)](#enhanced-fan-out-efo)
- [Error Handling & Retries](#error-handling--retries)
- [Monitoring](#monitoring)
- [Building from Source](#building-from-source)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)
- [Disclaimer](#disclaimer)

## Features

This module provides comprehensive Kinesis Data Streams integration with the following capabilities:

### Stream Management
- Flexible authentication options (default, static credentials, assume role)
- Automatic shard discovery and lease management
- Configurable stream position (start from oldest/latest)
- Dynamic shard rebalancing and scaling

### Message Processing
- Automatic checkpointing for reliable message processing
- Configurable retry logic with exponential backoff
- Batch processing with configurable batch sizes
- Duplicate message detection and handling

### Advanced Capabilities
- **Enhanced Fan-Out (EFO)** support for dedicated throughput
- JSON payload filtering with multiple comparison operators
- Queue handler integration for event-driven workflows
- Comprehensive error handling and logging

### Performance & Reliability
- Configurable polling intervals and batch wait times
- Throughput optimization with max records per request
- Graceful shutdown and shard lease release
- Connection pooling and resource management

## Requirements

### Platform Requirements
- **Decisions Platform**: Version 9.0 or higher
- **.NET Runtime**: Compatible with Decisions 9.x
- **AWS Environment**: Amazon Kinesis Data Streams

### AWS Permissions
The AWS IAM user/role requires appropriate permissions for Kinesis operations:
- `kinesis:GetRecords` - Read records from streams
- `kinesis:GetShardIterator` - Initialize shard reading
- `kinesis:DescribeStream` - Get stream metadata
- `kinesis:ListShards` - Discover stream shards
- `kinesis:SubscribeToShard` - For Enhanced Fan-Out (EFO)
- `kinesis:RegisterStreamConsumer` - Register EFO consumers
- `kinesis:DescribeStreamConsumer` - Get consumer details

### Dependencies
- AWS SDK for .NET (Amazon.Kinesis)
- Decisions Platform SDK

## Installation

### Option 1: Install Pre-built Module
1. Download the compiled module (`.zip` file)
2. Log into Decisions Portal
3. Navigate to **System > Administration > Features**
4. Click **Install Module**
5. Upload the module file
6. Restart the Decisions service if prompted

### Option 2: Build and Install
See the [Building from Source](#building-from-source) section below.

## Quick Start

1. **Install the Module**
   - Upload via Decisions Portal (System > Administration > Features)

2. **Configure Global Settings (Optional)**
   - Navigate to System > Settings > Message Queue Settings > Kinesis Settings
   - Set default AWS credentials and region

3. **Add Module Dependency**
   - In your project, add `Decisions.Kinesis` as a dependency

4. **Create Kinesis Queue**
   - Go to Manage > Jobs & Events > Queues
   - Create new queue with Message Queue Type: Kinesis
   - Configure stream settings (see [Configuration Options](#configuration-options))

5. **Create Queue Handler**
   - Create a flow to process incoming messages
   - Bind the flow as a Queue Handler to your Kinesis queue

6. **Start Processing**
   - Enable the queue to begin consuming messages from the stream

## Configuration Options

### Basic Settings
- **StreamName** - Kinesis stream name (required)
- **InitialStreamPosition** - Where to start reading:
  - `Start from oldest` - Process all historical records
  - `Start from latest` - Process only new records after queue starts
- **AWS Region** - AWS region where stream is located (e.g., us-east-1)

### Performance Tuning
- **MaxRecordsPerRequest** - Maximum records to fetch per API call (default: 10,000)
- **ShardPollInterval** - Time between GetRecords calls in milliseconds (default: 1000)
- **ShardBatchWaitTime** - Time to wait for batch completion in milliseconds
- **MaxRetries** - Number of retry attempts for failed operations (default: 3)
- **ErrorBackoffTime** - Base delay for exponential backoff in milliseconds (default: 500)

### Authentication Options
Configure one of the following authentication methods:
1. **Default Credentials** - Use AWS credentials from environment/instance profile
2. **Static Credentials** - Provide explicit AWS Access Key ID and Secret Access Key
3. **Assume Role** - Use STS to assume a role for cross-account access

### Enhanced Fan-Out (EFO) Settings
- **UseEnhancedFanOut** - Enable/disable EFO (default: false)
- **ConsumerArn** - ARN of registered stream consumer (required if EFO enabled)
- **ConsumerName** - Name of stream consumer (alternative to ConsumerArn)

> **Note**: Either `ConsumerArn` or `ConsumerName` must be provided when `UseEnhancedFanOut = true`

## JSON Payload Filters

The module supports powerful JSON payload filtering to process only relevant messages. Filters are evaluated before invoking queue handlers, reducing unnecessary processing.

### Filter Structure

```json
{
  "property": "user.type",
  "verb": "Equals",
  "value": "premium"
}
```

### Supported Comparison Operators

**String Comparisons:**
- `Equals` - Exact match
- `Not Equals` - Does not match
- `Contains` - Contains substring
- `Starts With` - Begins with value
- `Ends With` - Ends with value

**Numeric Comparisons:**
- `Greater Than` - Value is greater than threshold
- `Less Than` - Value is less than threshold
- `Greater Than or Equal` - Value is greater than or equal to threshold
- `Less Than or Equal` - Value is less than or equal to threshold

### Filter Examples

**Example 1: Filter by user type**
```json
{
  "property": "user.type",
  "verb": "Equals",
  "value": "premium"
}
```

**Example 2: Filter by order amount**
```json
{
  "property": "order.amount",
  "verb": "Greater Than",
  "value": 1000
}
```

**Example 3: Filter by event name**
```json
{
  "property": "eventName",
  "verb": "Starts With",
  "value": "user."
}
```

## Enhanced Fan-Out (EFO)

Enhanced Fan-Out provides dedicated throughput for consuming Kinesis streams with lower latency compared to standard polling.

### Key Differences

**Without EFO (Standard Polling):**
- Uses periodic `GetRecords` API calls
- Shared 2 MB/sec throughput per shard across all consumers
- Polling interval introduces latency
- Lower cost for occasional processing

**With EFO (Push-based Streaming):**
- Uses `SubscribeToShard` API for push-based delivery
- Dedicated 2 MB/sec throughput per consumer per shard
- ~70ms average latency
- Higher cost but better performance for real-time requirements

### Enabling EFO

1. **Register Stream Consumer** (via AWS Console or CLI):
   ```bash
   aws kinesis register-stream-consumer \
     --stream-arn arn:aws:kinesis:us-east-1:123456789012:stream/my-stream \
     --consumer-name my-consumer
   ```

2. **Configure Kinesis Queue**:
   - Set `UseEnhancedFanOut = true`
   - Provide either:
     - `ConsumerArn`: Full ARN from registration
     - `ConsumerName`: Name used during registration

3. **Start Queue**:
   - The module will automatically subscribe to shards using EFO

> **Note**: Checkpointing and lease management work the same way regardless of EFO setting.

## Error Handling & Retries

The module implements robust error handling with exponential backoff and jitter for transient errors.

### Retryable Errors
The following errors trigger automatic retry with backoff:
- `ProvisionedThroughputExceededException` - Throttling from AWS
- `LimitExceededException` - API rate limits exceeded
- HTTP 5xx errors - AWS service errors

### Retry Strategy
1. **Initial Attempt** - Immediate execution
2. **Retry Attempts** - Up to `MaxRetries` with exponential backoff
3. **Backoff Calculation**:
   ```
   delay = ErrorBackoffTime * (2 ^ attemptNumber) + random_jitter
   ```
4. **Jitter** - Randomization prevents thundering herd

### Non-Retryable Errors
The following errors are logged and not retried:
- Authentication/authorization failures
- Invalid stream names or configurations
- Malformed requests

## Monitoring

The module provides comprehensive logging at multiple levels for monitoring and troubleshooting.

### Log Levels

- **ERROR** - Critical failures, authentication errors, unrecoverable exceptions
- **WARN** - Throttling events, retry attempts, configuration warnings
- **INFO** - Queue start/stop, shard assignments, checkpoint updates
- **DEBUG** - Detailed message processing, filter evaluations, API calls

### Key Metrics to Monitor

- **Messages Processed** - Total records consumed from stream
- **Filter Rejections** - Messages filtered out before handler invocation
- **Checkpoint Updates** - Frequency of successful checkpoint commits
- **Retry Events** - Throttling or transient error occurrences
- **Shard Assignments** - Number of active shards being processed

### Logging Configuration

Configure logging in Decisions Portal:
```
System > Settings > System Settings > Log Settings
```

Set appropriate log level for `Decisions.Kinesis` namespace.

## Building from Source

### Prerequisites
- .NET 9.0 SDK or higher
- CreateDecisionsModule Global Tool (installed automatically during build)
- AWS SDK for .NET libraries (included in project)
- Decisions Platform SDK (NuGet package)

### Build Steps

The module uses the Decisions `CreateDecisionsModule` tool to package the module according to the configuration in `Module.Build.json`.

#### On Linux/macOS:
```bash
chmod +x build_module.sh
./build_module.sh
```

#### On Windows (PowerShell):
```powershell
.\build_module.ps1
```

#### Manual Build:
If you prefer to build manually:
```bash
# 1. Publish the project
dotnet publish ./Decisions.Kinesis/Decisions.Kinesis.csproj --self-contained false --output ./Decisions.Kinesis/bin -c Debug

# 2. Install/Update CreateDecisionsModule tool
dotnet tool update --global CreateDecisionsModule-GlobalTool

# 3. Create the module package
CreateDecisionsModule -buildmodule Decisions.Kinesis -output "." -buildfile Module.Build.json
```

### Build Output
The build process creates a `Decisions.Kinesis.zip` file in the root directory containing:
- Compiled module DLL (Decisions.Kinesis.dll)
- AWS SDK dependencies:
  - AWSSDK.Core.dll
  - AWSSDK.Kinesis.dll
  - AWSSDK.SecurityToken.dll
- JSON serialization library (Newtonsoft.Json.dll)
- Module icon (kinesis.png)
- Module metadata

This ZIP file can be uploaded directly to Decisions via **System > Administration > Features**.

## Usage Examples

### Example 1: Basic Kinesis Queue for Order Processing

**Scenario**: Process orders from a Kinesis stream named `order-events`

**Configuration**:
```
Stream Name: order-events
Initial Stream Position: Start from latest
Max Records Per Request: 100
Shard Poll Interval: 1000ms
Authentication: Default Credentials
```

**Queue Handler Flow**:
1. Receive order message from Kinesis
2. Parse JSON payload
3. Validate order data
4. Insert into database
5. Send confirmation email

### Example 2: EFO with JSON Filtering for Premium Users

**Scenario**: Real-time processing of premium user events only

**Configuration**:
```
Stream Name: user-activity-stream
Initial Stream Position: Start from latest
Use Enhanced Fan-Out: true
Consumer Name: premium-user-processor
```

**JSON Filter**:
```json
{
  "property": "user.tier",
  "verb": "Equals",
  "value": "premium"
}
```

**Benefits**:
- Dedicated throughput via EFO
- Automatic filtering reduces processing overhead
- Only premium user events trigger handler

### Example 3: Multi-Region Assume Role Setup

**Scenario**: Process streams from different AWS accounts using cross-account role

**Configuration**:
```
Stream Name: cross-account-stream
AWS Region: us-west-2
Authentication: Assume Role
Role ARN: arn:aws:iam::999999999999:role/KinesisReader
```

**Use Case**: Central processing system consuming streams from multiple business units with separate AWS accounts.

## Troubleshooting

### Authentication Failures
**Problem**: "Access denied" or "Unable to authenticate"

**Solution**:
- Verify AWS credentials are configured correctly
- Check IAM permissions include required Kinesis actions
- For assume role, verify trust relationship allows assumption
- Test credentials using AWS CLI: `aws kinesis describe-stream --stream-name <name>`

### Stream Not Found
**Problem**: "ResourceNotFoundException" or stream name errors

**Solution**:
- Verify stream name spelling and case sensitivity
- Confirm stream exists in specified region
- Check AWS region configuration matches stream location
- Use AWS Console or CLI to list streams: `aws kinesis list-streams`

### Throttling Errors
**Problem**: Frequent `ProvisionedThroughputExceededException` errors

**Solution**:
- Increase `ShardPollInterval` to reduce API call frequency
- Reduce `MaxRecordsPerRequest` if processing large batches
- Consider enabling Enhanced Fan-Out for dedicated throughput
- Monitor shard count and consider splitting shards for more capacity

### EFO Consumer Not Found
**Problem**: "Consumer not found" when using EFO

**Solution**:
- Verify consumer is registered: `aws kinesis describe-stream-consumer`
- Check `ConsumerArn` or `ConsumerName` is correct
- Ensure consumer is in ACTIVE state
- Confirm IAM permissions include `kinesis:DescribeStreamConsumer`

### Messages Not Processing
**Problem**: Queue is running but no messages are processed

**Solution**:
- Check Initial Stream Position setting (latest vs oldest)
- Verify JSON filters aren't excluding all messages
- Review queue handler flow for errors
- Check Decisions logs for exceptions in handler execution
- Confirm stream has active data using AWS Kinesis console

### High Latency
**Problem**: Slow message processing

**Solution**:
- Enable Enhanced Fan-Out for lower latency
- Reduce `ShardPollInterval` for more frequent polling (without EFO)
- Optimize queue handler flow performance
- Monitor shard iterator age in CloudWatch
- Consider increasing shard count for higher throughput

## Changelog

### Version 2.0
- **Added**: Enhanced Fan-Out (EFO) support
  - New queue settings: `UseEnhancedFanOut`, `ConsumerArn`, `ConsumerName`
  - Push-based streaming with SubscribeToShard API
  - Dedicated throughput per consumer

### Version 1.0
- Initial release with core Kinesis integration
- JSON payload filtering
- Checkpointing and lease management
- Flexible authentication options
- Error handling with exponential backoff

## Disclaimer

This module is provided "as is" without warranties of any kind. Use it at your own risk. The authors, maintainers, and contributors disclaim all liability for any direct, indirect, incidental, special, or consequential damages, including data loss or service interruption, arising from the use of this software.

**Important Notes:**
- Always test in a non-production environment first
- Ensure proper monitoring and alerting for production deployments
- Review AWS and Decisions documentation for best practices
- Monitor AWS costs associated with Kinesis usage and Enhanced Fan-Out
- This module is not officially supported by AWS or Decisions
