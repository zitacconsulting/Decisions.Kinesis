using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.SecurityToken;
using DecisionsFramework;
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
        protected List<Amazon.Kinesis.Model.Shard> Shards;

        protected override void SetUp()
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

            // Describing Stream to Fetch Shards
            var request = new DescribeStreamRequest
            {
                StreamName = StreamName
            };
            DescribeStreamResponse response = null;

            log.Debug($"Describing stream: {StreamName}");
            try
            {
                response = Task.Run(() => KinesisClient.DescribeStreamAsync(request)).Result;
            }
            catch (AmazonSecurityTokenServiceException stse)
            {
                string errorMessage = "Failed to authenticate with AWS. Please check your credentials and permissions.\n Sleeping for 30s";
                log.Error(stse, $"{errorMessage} Details: {stse.Message}");
                Thread.Sleep(TimeSpan.FromSeconds(30));
                throw new Exception(errorMessage, stse);
            }
            catch (AmazonKinesisException ake)
            {
                string errorMessage = $"Error accessing Kinesis stream '{StreamName}'. Please check if the stream exists and you have the necessary permissions.\n Sleeping for 30s";
                log.Error(ake, $"{errorMessage} Details: {ake.Message}");
                Thread.Sleep(TimeSpan.FromSeconds(30));
                throw new Exception(errorMessage, ake);
            }
            catch (Exception ex)
            {
                string errorMessage = $"An unexpected error occurred while retrieving shards for stream '{StreamName}'.\n Sleeping for 30s";
                log.Error(ex, $"{errorMessage} Details: {ex.Message}");
                Thread.Sleep(TimeSpan.FromSeconds(30));
                throw new Exception(errorMessage, ex);
            }
            Shards = response.StreamDescription.Shards;
            log.Info($"Found {response.StreamDescription.Shards.Count} available shards");
        }

        protected override void ReceiveMessages()
        {
            foreach (var shard in Shards)
            {
                if (isShuttingDown)
                {
                    break;
                }

                // Try to lease the Shard so that multiple threads cannot process the same shard to prevent that events processed twice
                if (KinesisCheckpointer.AcquireLease(StreamName, queueDefinition.Id, shard.ShardId, threadId))
                {
                    try
                    {
                        // Process the shard if we successfully acquired the lease
                        ProcessShard(shard.ShardId);
                    }
                    catch (Exception ex)
                    {
                        log.Error(ex, $"Error processing shard {shard.ShardId}");
                    }
                    finally
                    {
                        // Always release the lease when we're done, even if an error occurred
                        KinesisCheckpointer.ReleaseLease(StreamName, queueDefinition.Id, shard.ShardId, threadId);
                    }
                }
                else
                {
                    log.Debug($"Skipping shard {shard.ShardId} as it's already leased");
                }

            }
            log.Debug($"Sleeping for {TimeSpan.FromSeconds(queueDefinition.ShardPollInterval)}s before next poll");
            Thread.Sleep(TimeSpan.FromSeconds(queueDefinition.ShardPollInterval));
        }


        private void ProcessShard(string shardId)
        {
            log.Info($"Starting to process shard: {shardId}");
            int retryCount = 0;
            string shardIterator = null;
            try
            {
                // Get the shard iterator to start reading from the shard
                shardIterator = GetShardIterator(shardId);
                if (shardIterator == null)
                {
                    log.Warn($"Shard Iterator is null for shard: {shardId}");
                    return;
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Failed fetching shard iterator for shard: {shardId}\nError: {ex.Message}");
                return;
            }

            while (!isShuttingDown)
            {
                retryCount = 0;
                while (retryCount < queueDefinition.MaxRetries && !isShuttingDown)
                {
                    string lastProcessedSequenceNumber = null;
                    GetRecordsResponse getRecordsResponse = null;

                    try
                    {
                        var getRecordsRequest = new GetRecordsRequest
                        {
                            ShardIterator = shardIterator,
                            Limit = queueDefinition.MaxRecordsPerRequest
                        };

                        log.Debug($"Fetching records for shard: {shardId}");
                        getRecordsResponse = Task.Run(() => KinesisClient.GetRecordsAsync(getRecordsRequest)).Result;

                        // Process records
                        if (getRecordsResponse.Records.Count > 0)
                        {
                            log.Info($"Retrieved {getRecordsResponse.Records.Count} records from shard: {shardId}");
                            IsActive = true;
                            foreach (var record in getRecordsResponse.Records)
                            {
                                try
                                {
                                    ProcessRecord(record);
                                    lastProcessedSequenceNumber = record.SequenceNumber;
                                }
                                catch (Exception ex)
                                {
                                    log.Error(ex, $"Error processing record: {record.SequenceNumber}");
                                    HandleUnprocessableRecord(record, ex);
                                    lastProcessedSequenceNumber = record.SequenceNumber;
                                }
                            }

                            // Save checkpoint after processing the batch
                            if (lastProcessedSequenceNumber != null)
                            {
                                KinesisCheckpointer.SaveCheckpoint(StreamName, queueDefinition.Id, shardId, lastProcessedSequenceNumber);
                            }

                            // Did we hit the limit of records?
                            if (getRecordsResponse.Records.Count >= queueDefinition.MaxRecordsPerRequest)
                            {
                                if (string.IsNullOrEmpty(getRecordsResponse.NextShardIterator))
                                {
                                    log.Debug($"No more shard iterators for shard: {shardId}. Exiting Shard");
                                    IsActive = false;
                                    return;
                                }
                                log.Debug($"Maximum records processed for shard: {shardId}. Using batch wait time. \n Sleeping for {TimeSpan.FromSeconds(queueDefinition.ShardBatchWaitTime)}s");
                                Thread.Sleep(TimeSpan.FromSeconds(queueDefinition.ShardBatchWaitTime));
                                shardIterator = getRecordsResponse.NextShardIterator;
                            }
                            else
                            {
                                log.Debug($"All records processed for shard: {shardId}. Exiting Shard.");
                                IsActive = false;
                                return;
                            }
                        }
                        else
                        {
                            log.Debug($"No records retrieved for shard: {shardId}");
                            IsActive = false;
                            return;
                        }

                    }
                    catch (AggregateException ae)
                    {
                        // Unwrap the AggregateException
                        var innerException = ae.InnerExceptions.FirstOrDefault();
                        if (innerException is ProvisionedThroughputExceededException pte)
                        {
                            retryCount++;
                            var sleeptime = GetBackoffTime(retryCount);
                            log.Error(pte, $"Throughput exceeded when fetching records from {shardId}. Retry {retryCount}/{queueDefinition.MaxRetries}.\n Sleeping for {sleeptime.TotalSeconds}s");
                            Thread.Sleep(sleeptime); // Exponential backoff
                        }
                        else
                        {
                            retryCount++;
                            var sleeptime = GetBackoffTime(retryCount);
                            log.Error(innerException, $"Error fetching records from {shardId}. Retry {retryCount}/{queueDefinition.MaxRetries}\n{innerException.Message}.\n Sleeping for {sleeptime.TotalSeconds}s");
                            Thread.Sleep(sleeptime); // Exponential backoff
                        }
                    }
                    catch (Exception ex)
                    {
                        retryCount++;
                        var sleeptime = GetBackoffTime(retryCount);
                        log.Error(ex, $"Unexpected error fetching records from {shardId}. Retry {retryCount}/{queueDefinition.MaxRetries}\n{ex.Message}.\n Sleeping for {sleeptime.TotalSeconds}s");
                        Thread.Sleep(sleeptime);  // Exponential backoff
                    }
                }

                if (retryCount == queueDefinition.MaxRetries)
                {
                    log.Error($"Failed to process shard {shardId} after {queueDefinition.MaxRetries} attempts");
                    // Consider implementing a dead-letter queue or alerting mechanism here
                    break; // Exit the outer loop if max retries are reached
                }
            }

            log.Info($"Shutting down. Stopped processing shard: {shardId}");
        }
        private TimeSpan GetBackoffTime(int retryCount)
        {
            var backoffSeconds = queueDefinition.ErrorBackoffTime * retryCount;
            return TimeSpan.FromSeconds(backoffSeconds);
        }
        private void ProcessRecord(Record record)
        {
            string messageId = record.SequenceNumber;
            byte[] messageBody = record.Data.ToArray();
            string messageText = System.Text.Encoding.UTF8.GetString(messageBody);

            // Apply payload filters
            if (queueDefinition.PayloadFilters != null && queueDefinition.PayloadFilters.Length > 0)
            {
                try
                {
                    JObject jsonPayload = JObject.Parse(messageText);
                    if (!ApplyPayloadFilters(jsonPayload, queueDefinition.PayloadFilters))
                    {
                        log.Debug($"Record {messageId} filtered out based on payload filters");
                        return; // Skip this record if it doesn't match the filters
                    }
                }
                catch (Exception ex)
                {
                    log.Error(ex, $"Error parsing JSON payload for record {messageId}. Skipping filter application.");
                }
            }


            var metadata = KinesisUtils.GetRecordMetadata(record);
            var recordData = KinesisUtils.GetRecordData(record);

            // Use the BaseMqThreadJob's ProcessMessage method to handle the record
            if (!ProcessMessage(messageId, messageBody, metadata, recordData, null, messageText))
            {
                log.Warn($"Failed to process message: {messageId}");
                throw new Exception($"Failed to process message: {messageId}");
            }
            log.Debug($"Successfully processed record: {record.SequenceNumber}");
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

        private void HandleUnprocessableRecord(Record record, Exception ex)
        {
            log.Error(ex, $"Unprocessable record: SequenceNumber={record.SequenceNumber}, PartitionKey={record.PartitionKey}");
            // TODO: Implement a dead-letter queue mechanism
        }

        private string GetShardIterator(string shardId)
        {
            try
            {
                log.Debug($"Getting shard iterator for shard: {shardId}");
                // Retrieve the last checkpoint for this shard
                string checkpoint = KinesisCheckpointer.GetCheckpoint(StreamName, queueDefinition.Id, shardId);

                var request = new GetShardIteratorRequest
                {
                    StreamName = StreamName,
                    ShardId = shardId,
                };

                if (checkpoint != null)
                {
                    // If we have a checkpoint, start after that sequence number
                    request.ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
                    request.StartingSequenceNumber = checkpoint;
                    log.Debug($"Using checkpoint to start reading from sequence number: {checkpoint}");
                }
                else
                {
                    // If no checkpoint exists, use the initial stream position setting
                    switch (queueDefinition.InitialStreamPosition)
                    {
                        case "Start from oldest record":
                            request.ShardIteratorType = ShardIteratorType.TRIM_HORIZON;
                            log.Debug("No checkpoint found. Starting from the oldest record (TRIM_HORIZON)");
                            break;
                        case "Start from latest record":
                            request.ShardIteratorType = ShardIteratorType.LATEST;
                            log.Debug("No checkpoint found. Starting from the latest record (LATEST)");
                            break;
                        default:
                            throw new ArgumentException($"Invalid InitialStreamPosition: {queueDefinition.InitialStreamPosition}");
                    }
                }

                var response = Task.Run(() => KinesisClient.GetShardIteratorAsync(request)).Result;
                log.Debug($"Successfully retrieved shard iterator for shard: {shardId}");
                return response.ShardIterator;
            }
            catch (AggregateException ae)
            {
                // Unwrap the AggregateException to get the actual exception
                var innerException = ae.InnerExceptions[0];
                if (innerException is AmazonKinesisException akex)
                {
                    log.Error(akex, $"Failed to get shard iterator for shard {shardId}");
                    return null;
                }
                else
                {
                    log.Error(innerException, $"Failed to get shard iterator for shard {shardId}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Failed to get shard iterator for shard {shardId}");
                return null;
            }
        }

        protected override void CleanUp()
        {
            log.Info($"Cleaning up KinesisThreadJob. ThreadId: {threadId}");
            try
            {
                // Release all leases held by this thread
                KinesisCheckpointer.ReleaseAllLeases(StreamName, queueDefinition.Id, threadId);
                KinesisClient?.Dispose();
                log.Info("KinesisThreadJob cleaned up successfully");
            }
            catch (Exception ex)
            {
                log.Error(ex, "Error during KinesisThreadJob cleanup");
            }
        }

    }
}