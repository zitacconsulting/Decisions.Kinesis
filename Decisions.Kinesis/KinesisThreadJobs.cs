using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.SecurityToken;
using DecisionsFramework;
using DecisionsFramework.Utilities;
using DecisionsFramework.Data.Messaging;
using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using DecisionsFramework.ServiceLayer.Utilities;
using Decisions.MessageQueues;
using Newtonsoft.Json.Linq;

namespace Decisions.KinesisMessageQueue
{
    public class KinesisThreadJob : BaseMqThreadJob<KinesisMessageQueue>
    {
        private static readonly Log log = new Log("Kinesis");
        public override string LogCategory => "Kinesis Flow Worker";

        protected AmazonKinesisClient KinesisClient;
        protected string StreamName;
        private string threadId;
        private List<string> shardProcessingTasks;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> shardCancellationTokens = new();
        private const int LEASE_RENEWAL_INTERVAL_SECONDS = 30;
        private const int SHARD_ACQUISITION_DELAY_SECONDS = 5;

        public class BackoffRetry
        {
            private readonly int maxRetries;
            private readonly int baseDelaySeconds;
            private readonly int maxDelaySeconds;
            private readonly Random random = new Random();
            private readonly Log log;

            public BackoffRetry(int maxRetries, int baseDelaySeconds, int maxDelaySeconds, Log log)
            {
                this.maxRetries = maxRetries;
                this.baseDelaySeconds = baseDelaySeconds;
                this.maxDelaySeconds = maxDelaySeconds;
                this.log = log;
            }

            public async Task<T> ExecuteAsync<T>(Func<Task<T>> operation, string operationName, CancellationToken cancellationToken)
            {
                int attempt = 0;
                while (true)
                {
                    try
                    {
                        return await operation();
                    }
                    catch (ProvisionedThroughputExceededException ex)
                    {
                        if (attempt >= maxRetries)
                            throw;

                        await HandleRetry(attempt, ex, operationName, cancellationToken);
                    }
                    catch (LimitExceededException ex)
                    {
                        if (attempt >= maxRetries)
                            throw;

                        await HandleRetry(attempt, ex, operationName, cancellationToken);
                    }
                    catch (AmazonKinesisException ex)
                    {
                        // Only retry on 5xx errors
                        if (attempt >= maxRetries || (ex.StatusCode != System.Net.HttpStatusCode.InternalServerError &&
                                                    ex.StatusCode != System.Net.HttpStatusCode.ServiceUnavailable))
                            throw;

                        await HandleRetry(attempt, ex, operationName, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        // Don't retry on other exceptions
                        log.Error(ex, $"Non-retryable error during {operationName}");
                        throw;
                    }

                    attempt++;
                }
            }

            private async Task HandleRetry(int attempt, Exception ex, string operationName, CancellationToken cancellationToken)
            {
                // Calculate delay with exponential backoff and jitter
                int delaySeconds = CalculateDelay(attempt);

                log.Warn($"{ex.GetType().Name} during {operationName}. Attempt {attempt + 1}/{maxRetries}. " +
                         $"Retrying in {delaySeconds} seconds. Error: {ex.Message}");

                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
            }

            private int CalculateDelay(int attempt)
            {
                // Exponential backoff: baseDelay * 2^attempt
                double exponentialDelay = baseDelaySeconds * Math.Pow(2, attempt);

                // Add jitter: random value between 0 and 1 second
                double jitter = random.NextDouble();

                // Cap at maxDelay
                return (int)Math.Min(exponentialDelay + jitter, maxDelaySeconds);
            }
        }

        protected override async void SetUp()
        {
            log.Debug($"Setting up Kinesis client for stream: {queueDefinition.StreamName}");
            try
            {
                // Initialize the Kinesis client using the queue definition
                KinesisClient = KinesisUtils.GetKinesisClientForQueue(queueDefinition);
                StreamName = queueDefinition.StreamName;
                // Generate a unique thread ID for this job
                threadId = $"{Environment.MachineName}-{Guid.NewGuid()}";
                log.Info($"Kinesis client set up successfully. ThreadId: {threadId}");
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to set up Kinesis client.\n Sleeping for 30s");
                Thread.Sleep(TimeSpan.FromSeconds(30));
                throw;
            }

        }


        private async Task<List<Shard>> GetShardsAsync()
        {
            var request = new DescribeStreamRequest
            {
                StreamName = StreamName
            };

            try
            {
                var response = await KinesisClient.DescribeStreamAsync(request);
                return response.StreamDescription.Shards;
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Error getting shards for stream {StreamName}");
                throw;
            }
        }


        protected override void ReceiveMessages()
        {
            var backoffRetry = new BackoffRetry(
                maxRetries: queueDefinition.MaxRetries,
                baseDelaySeconds: queueDefinition.ErrorBackoffTime,
                maxDelaySeconds: 32,
                log: log
            );

            while (!isShuttingDown)
            {
                try
                {
                    // Get available shards
                    var getShardTask = Task.Run(async () => await GetShardsAsync());
                    var shards = getShardTask.GetAwaiter().GetResult();

                    foreach (var shard in shards)
                    {
                        if (isShuttingDown)
                            break;

                        // Skip if we're already processing this shard (Cant check only the lease for this since another task in this thread might be processing the shard)
                        if (shardProcessingTasks.Contains(shard.ShardId))
                            continue;

                        // Try to acquire lease
                        if (KinesisCheckpointer.AcquireLease(StreamName, queueDefinition.Id, shard.ShardId, threadId))
                        {
                            shardProcessingTasks.Add(shard.ShardId);
                            // Create cancellation token source for this shard
                            var shardCts = new CancellationTokenSource();
                            shardCancellationTokens.TryAdd(shard.ShardId, shardCts);

                            // Start processing the shard
                            var shardTask = Task.Run(async () =>
                            {
                                try
                                {
                                    await ProcessShardAsync(shard.ShardId, backoffRetry, shardCts.Token);
                                }
                                catch (Exception ex)
                                {
                                    log.Error(ex, $"Error processing shard {shard.ShardId}");
                                }
                                finally
                                {
                                    shardProcessingTasks.Remove(shard.ShardId);
                                    shardCancellationTokens.TryRemove(shard.ShardId, out var cts);
                                    cts?.Dispose();
                                }
                            });

                            // Wait before trying to acquire next shard to allow other nodes to pick up shards
                            Thread.Sleep(TimeSpan.FromSeconds(SHARD_ACQUISITION_DELAY_SECONDS));
                        }
                    }

                    // Wait before checking for new shards
                    Thread.Sleep(TimeSpan.FromSeconds(queueDefinition.ShardPollInterval));
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Error in ReceiveMessages loop");
                    Thread.Sleep(TimeSpan.FromSeconds(queueDefinition.ErrorBackoffTime));
                }
            }
        }



        private async Task ProcessShardAsync(string shardId, BackoffRetry backoffRetry, CancellationToken cancellationToken)
        {
            using var leaseRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            Task leaseRenewalTask = null;

            try
            {
                leaseRenewalTask = StartLeaseRenewalAsync(shardId, leaseRenewalCts.Token);

                string shardIterator;
                try
                {
                    shardIterator = await GetShardIteratorAsync(shardId);
                    if (shardIterator == null)
                    {
                        await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                        return;
                    }
                }
                catch (Exception ex)
                {
                    bool shouldReleaseCheckpoint = ex is InvalidArgumentException &&
                                                 ex.Message.Contains("StartingSequenceNumber");

                    log.Error(ex, $"Failed to get shard iterator for {shardId}. {(shouldReleaseCheckpoint ? "Checkpoint will be reset." : "Checkpoint will be preserved.")}");

                    await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                    throw;
                }

                while (!cancellationToken.IsCancellationRequested && !isShuttingDown)
                {
                    try
                    {
                        var getRecordsRequest = new GetRecordsRequest
                        {
                            ShardIterator = shardIterator,
                            Limit = queueDefinition.MaxRecordsPerRequest
                        };

                        GetRecordsResponse getRecordsResponse;
                        try
                        {
                            getRecordsResponse = await backoffRetry.ExecuteAsync(
                                async () => await KinesisClient.GetRecordsAsync(getRecordsRequest, cancellationToken),
                                $"GetRecords for shard {shardId}",
                                cancellationToken
                            );
                        }
                        // Specific handling for throughput exception
                        catch (ProvisionedThroughputExceededException ex)
                        {
                            // If we've exceeded retries in backoffRetry, we'll get here
                            log.Error(ex, $"Exceeded maximum retries for throughput exception on shard {shardId}");
                            await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                            throw;
                        }
                        // Catch other exceptions during GetRecords
                        catch (Exception ex)
                        {
                            log.Error(ex, $"Error getting records for shard {shardId}");
                            await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                            throw;
                        }

                        // Process records
                        if (getRecordsResponse.Records.Count > 0)
                        {
                            IsActive = true;
                            LastActiveTime = DateUtilities.FastNow(0L);
                            foreach (var record in getRecordsResponse.Records)
                            {
                                try
                                {
                                    await backoffRetry.ExecuteAsync(
                                        async () =>
                                        {
                                            await ProcessRecordAsync(record);
                                            return true;
                                        },
                                        $"ProcessRecord {record.SequenceNumber}",
                                        cancellationToken
                                    );
                                }
                                // Handle record processing failures
                                catch (Exception ex)
                                {
                                    log.Error(ex, $"Failed to process record {record.SequenceNumber}");
                                    await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                                    throw;
                                }
                            }

                            // Save checkpoint after processing batch
                            var lastRecord = getRecordsResponse.Records.Last();
                            KinesisCheckpointer.SaveCheckpoint(StreamName, queueDefinition.Id, shardId, lastRecord.SequenceNumber);

                        }
                        else
                        {
                            IsActive = false;
                        }

                        // Check if we've reached the end of the shard
                        if (string.IsNullOrEmpty(getRecordsResponse.NextShardIterator))
                        {
                            log.Info($"Reached end of shard {shardId}");
                            break;
                        }

                        shardIterator = getRecordsResponse.NextShardIterator;

                        if (getRecordsResponse.Records.Count < queueDefinition.MaxRecordsPerRequest)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(queueDefinition.ShardPollInterval), cancellationToken);
                        }
                        else
                        {
                            await Task.Delay(TimeSpan.FromSeconds(queueDefinition.ShardBatchWaitTime), cancellationToken);
                        }
                    }

                    catch (Exception ex)
                    {
                        log.Error(ex, $"[{ThreadDescription}] Error processing shard {shardId}");
                        await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                        throw;
                    }
                }

            }
            finally
            {
                try
                {
                    await CancelAndWaitForLeaseRenewal(leaseRenewalCts, leaseRenewalTask, shardId);
                    KinesisCheckpointer.ReleaseLease(StreamName, queueDefinition.Id, shardId, threadId);
                    log.Debug($"[{ThreadDescription}] Released lease for shard {shardId}");
                }
                catch (Exception ex)
                {
                    log.Error(ex, $"[{ThreadDescription}] Error releasing lease for shard {shardId}");
                }
            }
        }
        private async Task CancelAndWaitForLeaseRenewal(CancellationTokenSource cts, Task leaseRenewalTask, string shardId)
        {
            if (leaseRenewalTask == null) return;

            try
            {
                cts.Cancel();
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
                var completedTask = await Task.WhenAny(leaseRenewalTask, timeoutTask);
                if (completedTask == timeoutTask)
                {
                    log.Warn($"[{ThreadDescription}] Lease renewal task did not complete within timeout for shard {shardId}");
                }
            }
            catch (Exception ex)
            {
                log.Warn($"[{ThreadDescription}] Error waiting for lease renewal task to complete for shard {shardId}: {ex.Message}");
            }
        }

        private async Task StartLeaseRenewalAsync(string shardId, CancellationToken cancellationToken)
        {
            var renewalInterval = TimeSpan.FromSeconds(LEASE_RENEWAL_INTERVAL_SECONDS);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // First wait, then renew - this ensures we don't double-renew
                        // since the initial lease is fresh
                        await Task.Delay(renewalInterval, cancellationToken);

                        if (cancellationToken.IsCancellationRequested)
                            break;

                        // Use AcquireLease instead of SaveCheckpoint for renewal
                        // This will only update lease-related fields and preserve the sequence number
                        if (!KinesisCheckpointer.AcquireLease(StreamName, queueDefinition.Id, shardId, threadId))
                        {
                            log.Error($"Failed to renew lease for shard {shardId}, another thread may have taken ownership");
                            break;
                        }

                        log.Debug($"Successfully renewed lease for shard {shardId}");
                    }
                    catch (OperationCanceledException)
                    {
                        log.Debug($"Lease renewal cancelled for shard {shardId}");
                        break;
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, $"Error renewing lease for shard {shardId}, stopping lease renewal");
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Unexpected error in lease renewal task for shard {shardId}");
                throw;
            }
            finally
            {
                log.Debug($"Lease renewal task completed for shard {shardId}");
            }
        }


        private async Task<string> GetShardIteratorAsync(string shardId, bool ignoreCheckpoint = false)
        {
            try
            {
                string checkpoint = ignoreCheckpoint ? null : KinesisCheckpointer.GetCheckpoint(StreamName, queueDefinition.Id, shardId);

                var request = new GetShardIteratorRequest
                {
                    StreamName = StreamName,
                    ShardId = shardId,
                };

                if (checkpoint != null)
                {
                    request.ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
                    request.StartingSequenceNumber = checkpoint;
                    log.Debug($"Using checkpoint to start reading from sequence number: {checkpoint}");
                }
                else
                {
                    switch (queueDefinition.InitialStreamPosition)
                    {
                        case "Start from oldest record":
                            request.ShardIteratorType = ShardIteratorType.TRIM_HORIZON;
                            log.Debug("Starting from the oldest record (TRIM_HORIZON)");
                            break;
                        case "Start from latest record":
                            request.ShardIteratorType = ShardIteratorType.LATEST;
                            log.Debug("Starting from the latest record (LATEST)");
                            break;
                        default:
                            throw new ArgumentException($"Invalid InitialStreamPosition: {queueDefinition.InitialStreamPosition}");
                    }
                }

                var response = await KinesisClient.GetShardIteratorAsync(request);
                return response.ShardIterator;
            }
            catch (InvalidArgumentException ex) when (ex.Message.Contains("StartingSequenceNumber") && !ignoreCheckpoint)
            {
                // Only reset checkpoint for invalid sequence number errors
                log.Warn($"Invalid checkpoint detected for shard {shardId}, clearing checkpoint and retrying with initial position");
                KinesisCheckpointer.SaveCheckpoint(StreamName, queueDefinition.Id, shardId, null);
                log.Info($"Reset checkpoint for shard {shardId}");
                return await GetShardIteratorAsync(shardId, true);
            }
            catch (Exception ex)
            {
                // For all other errors (including ProvisionedThroughputExceededException),
                // preserve the checkpoint and throw the error
                log.Error(ex, $"Failed to get shard iterator for shard {shardId}");
                throw;
            }
        }


        private async Task ProcessRecordAsync(Record record)
        {
            try
            {
                string messageId = record.SequenceNumber;
                byte[] messageBody = record.Data.ToArray();
                string messageText = System.Text.Encoding.UTF8.GetString(messageBody);

                // Apply payload filters
                if (queueDefinition.PayloadFilters?.Length > 0)
                {
                    try
                    {
                        JObject jsonPayload = JObject.Parse(messageText);
                        if (!ApplyPayloadFilters(jsonPayload, queueDefinition.PayloadFilters))
                        {
                            log.Debug($"Record {messageId} filtered out based on payload filters");
                            return;
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, $"Error parsing JSON payload for record {messageId}");
                    }
                }

                var metadata = KinesisUtils.GetRecordMetadata(record);
                var recordData = KinesisUtils.GetRecordData(record);

                if (!ProcessMessage(messageId, messageBody, metadata, recordData, null, messageText))
                {
                    log.Warn($"Failed to process message: {messageId}");
                    throw new Exception($"Failed to process message: {messageId}");
                }

                log.Debug($"Successfully processed record: {record.SequenceNumber}");
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Error processing record {record.SequenceNumber}");
                throw;
            }
        }



        private bool ApplyPayloadFilters(JObject payload, KinesisPayloadFilter.PayloadFilter[] filters)
        {
            foreach (var filter in filters)
            {
                JToken value = payload.SelectToken(filter.Property);
                if (value == null)
                {
                    return false; // Property not found, filter fails
                }

                string stringValue = value.ToString();
                bool matches = false;

                switch (filter.Verb)
                {
                    case "Equals":
                        matches = stringValue == filter.Value;
                        break;
                    case "Equals (Case Insensitive)":
                        matches = string.Equals(stringValue, filter.Value, StringComparison.OrdinalIgnoreCase);
                        break;
                    case "Not Equals":
                        matches = stringValue != filter.Value;
                        break;
                    case "Not Equals (Case Insensitive)":
                        matches = !string.Equals(stringValue, filter.Value, StringComparison.OrdinalIgnoreCase);
                        break;
                    case "Contains":
                        matches = stringValue.Contains(filter.Value);
                        break;
                    case "Contains (Case Insensitive)":
                        matches = stringValue.IndexOf(filter.Value, StringComparison.OrdinalIgnoreCase) >= 0;
                        break;
                    case "Starts With":
                        matches = stringValue.StartsWith(filter.Value);
                        break;
                    case "Starts With (Case Insensitive)":
                        matches = stringValue.StartsWith(filter.Value, StringComparison.OrdinalIgnoreCase);
                        break;
                    case "Ends With":
                        matches = stringValue.EndsWith(filter.Value);
                        break;
                    case "Ends With (Case Insensitive)":
                        matches = stringValue.EndsWith(filter.Value, StringComparison.OrdinalIgnoreCase);
                        break;
                    case "Greater Than":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.Ordinal) > 0;
                        break;
                    case "Greater Than (Case Insensitive)":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.OrdinalIgnoreCase) > 0;
                        break;
                    case "Less Than":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.Ordinal) < 0;
                        break;
                    case "Less Than (Case Insensitive)":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.OrdinalIgnoreCase) < 0;
                        break;
                    case "Greater Than or Equal":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.Ordinal) >= 0;
                        break;
                    case "Greater Than or Equal (Case Insensitive)":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.OrdinalIgnoreCase) >= 0;
                        break;
                    case "Less Than or Equal":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.Ordinal) <= 0;
                        break;
                    case "Less Than or Equal (Case Insensitive)":
                        matches = string.Compare(stringValue, filter.Value, StringComparison.OrdinalIgnoreCase) <= 0;
                        break;
                    default:
                        log.Warn($"Unknown filter verb: {filter.Verb}");
                        return false;
                }

                if (!matches)
                {
                    return false; // If any filter doesn't match, return false
                }
            }

            return true; // All filters matched
        }
        protected override void CleanUp()
        {
            try
            {
                // Clean up any remaining cancellation tokens
                foreach (var cts in shardCancellationTokens.Values)
                {
                    try
                    {
                        cts.Dispose();
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, "Error disposing cancellation token source");
                    }
                }
                shardCancellationTokens.Clear();

                // Clean up any remaining tasks
                shardProcessingTasks.Clear();

                // Release any remaining leases
                try
                {
                    KinesisCheckpointer.ReleaseAllLeases(StreamName, queueDefinition.Id, threadId);
                }
                catch (Exception ex)
                {
                    log.Error(ex, "Error releasing all leases during cleanup");
                }
            }
            finally
            {
                base.CleanUp();
            }
        }
    }

}