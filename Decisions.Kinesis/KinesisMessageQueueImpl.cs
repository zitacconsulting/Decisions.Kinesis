using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using DecisionsFramework;
using DecisionsFramework.Data.Messaging;
using DecisionsFramework.Data.Messaging.Implementations;
using DecisionsFramework.ServiceLayer;
using Decisions.MessageQueues;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Decisions.KinesisMessageQueue
{
    public class KinesisMessageQueueImpl : SimpleMessageQueueImpl<KinesisMessageQueue>
    {
        private static readonly Log Log = new Log("KinesisQueueImpl");

        protected override IMqThreadManager CreateThreadManager()
            => new BaseMqThreadManager<KinesisThreadJob, KinesisMessageQueue>("Kinesis", QueueDefinition);

        protected override string QueueTypeName => "Kinesis";

        private AmazonKinesisClient client;

        public KinesisMessageQueueImpl(KinesisMessageQueue queueDef) : base(queueDef)
        {
            try
            {
                Log.Debug($"Initializing KinesisMessageQueueImpl for stream: {queueDef.StreamName}");
                client = KinesisUtils.GetKinesisClientForQueue(queueDef);
                Log.Info($"Successfully initialized KinesisMessageQueueImpl for stream: {queueDef.StreamName}");
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Failed to initialize KinesisMessageQueueImpl for stream: {queueDef.StreamName}");
                throw new InvalidOperationException($"Failed to initialize Kinesis client: {ex.Message}", ex);
            }
        }

        public override MessageQueueMessage GetMessage(string handlerId, MessageFetchType messageFetchType, bool blockForMessage)
        {
            Log.Warn($"GetMessage operation is not supported for Kinesis streams. HandlerId: {handlerId}, FetchType: {messageFetchType}, BlockForMessage: {blockForMessage}");
            return null;
        }

        public override long? GetMessageCount()
        {
            Log.Warn("GetMessageCount operation is not supported for Kinesis streams");
            return null;
        }

        public override long MessageCount => throw new NotImplementedException("Message count is not directly available for Kinesis streams.");

        public override string GetAlternativeTestQueueResult()
        {
            Log.Debug($"Testing connection to Kinesis stream: {QueueDefinition.StreamName}");
            try
            {
                var describeStreamRequest = new DescribeStreamRequest
                {
                    StreamName = QueueDefinition.StreamName
                };

                var describeStreamResponse = Task.Run(() => client.DescribeStreamAsync(describeStreamRequest)).Result;

                string result = $"Successfully connected to Kinesis stream '{QueueDefinition.StreamName}'. " +
                                $"Stream status: {describeStreamResponse.StreamDescription.StreamStatus}, " +
                                $"Shard count: {describeStreamResponse.StreamDescription.Shards.Count}";
                Log.Info(result);
                return result;
            }
            catch (AggregateException ae)
            {
                // Unwrap the AggregateException to get the actual exception
                var innerException = ae.InnerExceptions[0];
                if (innerException is AmazonKinesisException akex)
                {
                    string errorMessage = $"Amazon Kinesis error while connecting to stream '{QueueDefinition.StreamName}': {akex.Message}";
                    Log.Error(akex, errorMessage);
                    return errorMessage;
                }
                else
                {
                    string errorMessage = $"Unexpected error while connecting to Kinesis stream '{QueueDefinition.StreamName}': {innerException.Message}";
                    Log.Error(innerException, errorMessage);
                    return errorMessage;
                }
            }
            catch (Exception ex)
            {
                string errorMessage = $"Unexpected error while connecting to Kinesis stream '{QueueDefinition.StreamName}': {ex.Message}";
                Log.Error(ex, errorMessage);
                return errorMessage;
            }
        }

        public override void PushMessage(string id, byte[] message)
        {
            Log.Debug($"Attempting to push message with ID {id} to stream: {QueueDefinition.StreamName}");
            var putRecordRequest = new PutRecordRequest
            {
                StreamName = QueueDefinition.StreamName,
                Data = new MemoryStream(message),
                PartitionKey = id // Using the id as the partition key. Adjust if needed.
            };

            try
            {
                var putRecordResponse = Task.Run(() => client.PutRecordAsync(putRecordRequest)).Result;
                Log.Info($"Successfully put record to Kinesis. Stream: {QueueDefinition.StreamName}, Shard ID: {putRecordResponse.ShardId}, Sequence number: {putRecordResponse.SequenceNumber}");
            }
            catch (AmazonKinesisException akex)
            {
                string errorMessage = $"Amazon Kinesis error while putting record to stream '{QueueDefinition.StreamName}': {akex.Message}";
                Log.Error(akex, errorMessage);
                throw new InvalidOperationException(errorMessage, akex);
            }
            catch (Exception ex)
            {
                string errorMessage = $"Unexpected error while putting record to Kinesis stream '{QueueDefinition.StreamName}': {ex.Message}";
                Log.Error(ex, errorMessage);
                throw new InvalidOperationException(errorMessage, ex);
            }
        }

        public override void Dispose()
        {
            Log.Debug($"Disposing KinesisMessageQueueImpl for stream: {QueueDefinition.StreamName}");
            try
            {
                client?.Dispose();
                Log.Info($"Successfully disposed KinesisMessageQueueImpl for stream: {QueueDefinition.StreamName}");
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error while disposing KinesisMessageQueueImpl for stream: {QueueDefinition.StreamName}");
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}