# Decisions Kinesis Module

A module for integrating Amazon Kinesis Data Streams with Decisions automation platform. This module enables real-time data streaming and processing capabilities within Decisions.

## Features

* **Stream Management**
  * Connect to and manage Kinesis data streams
* **Flexible Authentication**
  * Default AWS credentials
  * Static credentials (Access Key/Secret)
  * IAM Role assumption
* **Message Processing**
  * Configurable batch size and polling intervals
  * Automatic checkpoint management
  * Fault-tolerant message processing
  * Support for multiple shards processing
* **JSON Payload Filtering**
  * Advanced filtering capabilities for JSON messages
  * Multiple comparison operations
  * Case-sensitive and insensitive options
* **Distributed Processing**
  * Distributed lease management for shards
  * Automatic lease renewal
  * Graceful failover handling

## Configuration Settings

### Global Settings

```markdown
└── AWS Configuration
    ├── Region Selection
    ├── Authentication Method
    │   ├── Default Credentials
    │   └── Static Credentials
    ├── Access Keys (for static auth)
    └── Role ARN (optional)
```

### Queue Settings

#### Basic Configuration
| Setting | Description |
|---------|-------------|
| Stream Name | Name of the Kinesis stream to connect to |
| Initial Position | Choose where to start reading (oldest/latest) |

#### Advanced Settings
| Setting | Range | Description |
|---------|-------|-------------|
| Max Records | 1-10000 | Records per request |
| Max Retries | ≥ 0 | Retry attempts |
| Request Timeout | ≥ 1s | AWS request timeout |

## Best Practices

### Authentication
1. **Default Credentials**
   * Preferred method for AWS authentication
   * Only works when Decisions is running in AWS
   * Automatic credential rotation
   * Enhanced security

2. **Static Credentials**
   * Regular key rotation required
   * Store securely
   * Use minimal permissions

Assume role can be used e.g. to access Kinesis streams in other AWS accounts.

## JSON Payload Filtering

### Filter Configuration
```json
{
    "property": "user.type",
    "verb": "Equals",
    "value": "premium"
}
```
Will only process records where the property user.type in the record body Equals the value premium. All other messages will be ignored.

### Supported Operations
* **Comparison**
  * `Equals`
  * `Not Equals`
  * `Contains`
  * `Starts With`
  * `Ends With`
* **Numeric**
  * `Greater Than`
  * `Less Than`
  * `Greater/Less Than or Equal`

## Error Handling

### Retry Strategy
```
Initial Delay → Exponential Backoff → Max Delay
     5s      →      10s, 20s...    →    32s
```

### Handled Exceptions
* `ProvisionedThroughputExceededException`
* `LimitExceededException`
* `5xx Server Errors`

## Monitoring

### Log Levels
```
ERROR   → Critical issues
WARN    → Potential problems
INFO    → Status updates
DEBUG   → Detailed operations
```

## Requirements

* Decisions Platform 9+
* AWS Credentials
* Kinesis Stream Access

## Installation

1. **Download Module**
   ```
   Compiled: [File](./Decisions.Kinesis.zip)
   Or pull and compile the project.
   ```

2. **Install Module**
   * Open Decisions Portal
   * Navigate to System / Administration / Features
   * Click "Upload and install Module"

3. **Configure Settings**
   * Set system wide settings in System / Settings / Message Queue Settings / Kinesis Settings (Optional)
   * Open a Project and Navigate to Manage / Configuration / Dependencies. Click Manage and choose the Modules tab. Add Decisions.Kinesis.
   * Navigate to Manage / Jobs & Events / Setting / Kinesis Settings and configure the connection settings
   * Navigate to Manage / Jobs & Events / Queues and add a new Kinesis Stream.
   * Navigate to Manage / Jobs & Event / Queue Handlers and create a new handler

4. **Control the Queue**
   Right click the newly created Queue to Start/Stop/Pause/Test
