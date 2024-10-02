using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using DecisionsFramework;
using DecisionsFramework.Data.Messaging;
using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using DecisionsFramework.ServiceLayer.Utilities;
using Decisions.MessageQueues;

namespace Decisions.KinesisMessageQueue
{
    public class KinesisThreadJob : BaseMqThreadJob<KinesisMessageQueue>
    {
        private static readonly Log log = new Log("KinesisThreadJob");
        public override string LogCategory => "Kinesis Flow Worker";

        // Time interval between polling for available shards
        private static readonly int SHARD_POLL_INTERVAL = 5000; // 5 seconds

        protected AmazonKinesisClient KinesisClient;
        protected string StreamName;
        private string threadId;

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
                log.Error(ex, "Failed to set up Kinesis client");
                throw;
            }
        }

        protected override void ReceiveMessages()
        {
            try
            {
                log.Debug("Starting to process available shards");
                ProcessAvailableShards();
            }
            catch (Exception ex)
            {
                log.Error(ex, "Error in ReceiveMessages");
                // Consider implementing an alerting mechanism here
            }
            log.Debug($"Sleeping for {SHARD_POLL_INTERVAL}ms before next poll");
            Thread.Sleep(SHARD_POLL_INTERVAL);
        }

        private void ProcessAvailableShards()
        {
            // Get all available (unleasedshards for this stream
            try
            {
                var shards = KinesisUtils.GetAvailableShards(KinesisClient, queueDefinition.Id, StreamName);

                log.Info($"Found {shards.Count} available shards");

                foreach (var shard in shards)
                {
                    log.Debug($"Attempting to acquire lease for shard: {shard.ShardId}");
                    if (KinesisCheckpointer.AcquireLease(StreamName, queueDefinition.Id, shard.ShardId, threadId))
                    {
                        log.Info($"Acquired lease for shard: {shard.ShardId}");
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
                            log.Debug($"Releasing lease for shard: {shard.ShardId}");
                            KinesisCheckpointer.ReleaseLease(StreamName, queueDefinition.Id, shard.ShardId, threadId);
                        }
                    }
                    else
                    {
                        log.Debug($"Failed to acquire lease for shard: {shard.ShardId}");
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Failure occured while processing {StreamName}");
            }
        }

        private void ProcessShard(string shardId)
        {
            log.Info($"Starting to process shard: {shardId}");
            int retryCount = 0;
            while (retryCount < queueDefinition.MaxRetries)
            {
                try
                {
                    // Get the shard iterator to start reading from the shard
                    string shardIterator = GetShardIterator(shardId);
                    if (shardIterator == null)
                    {
                        log.Warn($"Failed to get shard iterator for shard: {shardId}");
                        break;
                    }

                    string lastProcessedSequenceNumber = null;

                    while (shardIterator != null)
                    {
                        var getRecordsRequest = new GetRecordsRequest
                        {
                            ShardIterator = shardIterator,
                            Limit = queueDefinition.MaxRecordsPerRequest
                        };

                        log.Debug($"Fetching records for shard: {shardId}");
                        var getRecordsResponse = Task.Run(() => KinesisClient.GetRecordsAsync(getRecordsRequest)).Result;

                        if (getRecordsResponse.Records.Count > 0)
                        {
                            log.Info($"Retrieved {getRecordsResponse.Records.Count} records from shard: {shardId}");
                            IsActive = true;
                            foreach (var record in getRecordsResponse.Records)
                            {
                                try
                                {
                                    // Process each record
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
                                log.Debug($"Saving checkpoint for shard: {shardId}, sequence number: {lastProcessedSequenceNumber}");
                                KinesisCheckpointer.SaveCheckpoint(StreamName, queueDefinition.Id, shardId, lastProcessedSequenceNumber);
                            }
                        }
                        else
                        {
                            log.Debug($"No records retrieved for shard: {shardId}");
                            IsActive = false;
                        }
                        // Get the next shard iterator for the next read
                        shardIterator = getRecordsResponse.NextShardIterator;
                    }

                    log.Info($"Finished processing shard: {shardId}");
                    break;
                }

                catch (AggregateException ae)
                {
                    // Unwrap the AggregateException to get the actual exception
                    var innerException = ae.InnerExceptions[0];
                    if (innerException is AmazonKinesisException akex)
                    {
                        retryCount++;
                        log.Error(akex, $"Error processing shard {shardId}. Retry {retryCount}/{queueDefinition.MaxRetries}");
                        Thread.Sleep(1000 * retryCount); // Exponential backoff
                    }
                    else
                    {
                        retryCount++;
                        log.Error(innerException, $"Error processing shard {shardId}. Retry {retryCount}/{queueDefinition.MaxRetries}");
                        Thread.Sleep(1000 * retryCount); // Exponential backoff
                    }
                }
                catch (Exception ex)
                {
                    retryCount++;
                    log.Error(ex, $"Error processing shard {shardId}. Retry {retryCount}/{queueDefinition.MaxRetries}");
                    Thread.Sleep(1000 * retryCount); // Exponential backoff
                }
            }

            if (retryCount == queueDefinition.MaxRetries)
            {
                log.Error($"Failed to process shard {shardId} after {queueDefinition.MaxRetries} attempts");
                // Consider implementing a dead-letter queue or alerting mechanism here
            }
        }

        private void ProcessRecord(Record record)
        {
            log.Debug($"Processing record: {record.SequenceNumber}");
            string messageId = record.SequenceNumber;
            byte[] messageBody = record.Data.ToArray();
            string messageText = System.Text.Encoding.UTF8.GetString(messageBody);

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

        ~KinesisThreadJob()
        {
            log.Debug("KinesisThreadJob finalized");
            GC.SuppressFinalize(this);
        }
    }
}