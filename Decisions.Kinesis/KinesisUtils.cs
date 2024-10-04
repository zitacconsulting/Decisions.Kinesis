using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using DecisionsFramework;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using DecisionsFramework.ServiceLayer.Services.Projects.Settings;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Decisions.KinesisMessageQueue
{
    public static class KinesisUtils
    {
        private static readonly Log Log = new Log("KinesisUtils");

        internal static KinesisSettings GetSettings(string projectId)
        {
            Log.Debug($"Retrieving Kinesis settings for project ID: {projectId}");
            try
            {
                if (string.IsNullOrEmpty(projectId))
                {
                    Log.Debug("Project ID is null or empty, returning instance settings");
                    return ModuleSettingsAccessor<KinesisSettings>.Instance;
                }
                else
                {
                    Log.Debug($"Retrieving project-specific settings for project ID: {projectId}");
                    return ProjectSettingsAccessor<KinesisSettings>.GetSettings(projectId);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error retrieving Kinesis settings for project ID: {projectId}");
                throw new Exception($"Failed to retrieve Kinesis settings: {ex.Message}", ex);
            }
        }


        public static List<Shard> GetAvailableShards(AmazonKinesisClient client, string queueId, string streamName)
        {
            Log.Debug($"Getting available shards for stream: {streamName}, queue ID: {queueId}");
            List<Shard> availableShards = new List<Shard>();
            string exclusiveStartShardId = null;
            bool hasMoreShards = true;

            try
            {
                // Continue fetching shards until we've retrieved all of them
                while (hasMoreShards)
                {
                    var request = new DescribeStreamRequest
                    {
                        StreamName = streamName,
                        ExclusiveStartShardId = exclusiveStartShardId
                    };

                    Log.Debug($"Describing stream: {streamName}, starting from shard: {exclusiveStartShardId ?? "beginning"}");
                    var response = Task.Run(() => client.DescribeStreamAsync(request)).Result;

                    foreach (var shard in response.StreamDescription.Shards)
                    {
                        // Only add shards that are not currently leased
                        if (!KinesisCheckpointer.IsShardLeased(streamName, queueId, shard.ShardId))
                        {
                            Log.Debug($"Found available shard: {shard.ShardId}");
                            availableShards.Add(shard);
                        }
                    }

                    // Check if there are more shards to retrieve
                    if (response.StreamDescription.HasMoreShards)
                    {
                        exclusiveStartShardId = response.StreamDescription.Shards.Last().ShardId;
                        Log.Debug($"More shards available, continuing from shard: {exclusiveStartShardId}");
                    }
                    else
                    {
                        hasMoreShards = false;
                        Log.Debug("No more shards available");
                    }
                }

                Log.Info($"Retrieved {availableShards.Count} available shards for stream: {streamName}");
                return availableShards;
            }
            catch (AmazonSecurityTokenServiceException stse)
            {
                string errorMessage = "Failed to authenticate with AWS. Please check your credentials and permissions.";
                Log.Error(stse, $"{errorMessage} Details: {stse.Message}");
                throw new Exception(errorMessage, stse);
            }
            catch (AmazonKinesisException ake)
            {
                string errorMessage = $"Error accessing Kinesis stream '{streamName}'. Please check if the stream exists and you have the necessary permissions.";
                Log.Error(ake, $"{errorMessage} Details: {ake.Message}");
                throw new Exception(errorMessage, ake);
            }
            catch (Exception ex)
            {
                string errorMessage = $"An unexpected error occurred while retrieving shards for stream '{streamName}'.";
                Log.Error(ex, $"{errorMessage} Details: {ex.Message}");
                throw new Exception(errorMessage, ex);
            }
        }


        /// Creates and configures an AmazonKinesisClient for a given queue definition.
        public static AmazonKinesisClient GetKinesisClientForQueue(KinesisMessageQueue queueDefinition)
        {
            Log.Debug($"Creating Kinesis client for queue: {queueDefinition.DisplayName}");
            try
            {
                var settings = GetSettings(queueDefinition.GetProjectId());
                Log.Debug($"Retrieved settings for project: {queueDefinition.GetProjectId()}");

                var clientConfig = new AmazonKinesisConfig
                {
                    RegionEndpoint = RegionEndpoint.GetBySystemName(queueDefinition.OverrideSettings ? queueDefinition.Region : settings.AwsRegion),
                    Timeout = TimeSpan.FromSeconds(queueDefinition.RequestTimeout),
                    MaxErrorRetry = queueDefinition.MaxRetries
                };
                Log.Debug($"Configured Kinesis client with Region: {clientConfig.RegionEndpoint}, Timeout: {clientConfig.Timeout}, MaxErrorRetry: {clientConfig.MaxErrorRetry}");

                AWSCredentials credentials = GetCredentials(queueDefinition, settings);
                Log.Debug($"Retrieved AWS credentials using authentication method: {(queueDefinition.OverrideSettings ? queueDefinition.AuthenticationMethod : settings.AuthenticationMethod)}");

                var client = new AmazonKinesisClient(credentials, clientConfig);
                Log.Info($"Successfully created Kinesis client for queue: {queueDefinition.DisplayName}");
                return client;
            }
            catch (AmazonKinesisException akex)
            {
                Log.Error(akex, $"Amazon Kinesis specific error occurred while creating client for queue: {queueDefinition.DisplayName}");
                throw new Exception($"Failed to create Kinesis client due to Amazon Kinesis error: {akex.Message}", akex);
            }
            catch (AmazonServiceException asex)
            {
                Log.Error(asex, $"Amazon service error occurred while creating client for queue: {queueDefinition.DisplayName}");
                throw new Exception($"Failed to create Kinesis client due to Amazon service error: {asex.Message}", asex);
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Unexpected error occurred while creating Kinesis client for queue: {queueDefinition.DisplayName}");
                throw new Exception($"Failed to create Kinesis client due to unexpected error: {ex.Message}", ex);
            }
        }

        /// Retrieves the appropriate AWS credentials based on the queue definition and settings.
        private static AWSCredentials GetCredentials(KinesisMessageQueue queueDefinition, KinesisSettings settings)
        {
            string authMethod = queueDefinition.OverrideSettings ? queueDefinition.AuthenticationMethod : settings.AuthenticationMethod;
            Log.Debug($"Getting credentials using authentication method: {authMethod}");

            try
            {
                switch (authMethod)
                {
                    case "DefaultCredentials":
                        Log.Debug("Using DefaultCredentials (InstanceProfileAWSCredentials)");
                        return new InstanceProfileAWSCredentials();
                    case "StaticCredentials":
                        Log.Debug("Using StaticCredentials (BasicAWSCredentials)");
                        string accessKeyId = queueDefinition.OverrideSettings ? queueDefinition.AccessKeyId : settings.AccessKeyId;
                        string secretAccessKey = queueDefinition.OverrideSettings ? queueDefinition.SecretAccessKey : settings.SecretAccessKey;
                        return new BasicAWSCredentials(accessKeyId, secretAccessKey);
                    case "RoleARN":
                        return GetEnvironmentAwareRoleCredentials(queueDefinition, settings);
                    default:
                        throw new ArgumentException($"Invalid authentication method: {authMethod}");
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error occurred while getting AWS credentials with method {authMethod}");
                throw new Exception($"Failed to get AWS credentials: {ex.Message}", ex);
            }
        }


        private static AWSCredentials GetEnvironmentAwareRoleCredentials(KinesisMessageQueue queueDefinition, KinesisSettings settings)
        {
            string environment = DetectEnvironment();
            string roleArn = queueDefinition.OverrideSettings ? queueDefinition.RoleArn : settings.RoleArn;

            Log.Debug($"Detected environment: {environment}");

            switch (environment)
            {
                case "ECS":
                    Log.Debug("Using ECS Task Credentials for role assumption.");
                    return new AssumeRoleAWSCredentials(new ECSTaskCredentials(), roleArn, "DecisionsKinesisSession");

                case "EC2":
                    Log.Debug("Using EC2 Instance Profile Credentials for role assumption.");
                    return new AssumeRoleAWSCredentials(new InstanceProfileAWSCredentials(), roleArn, "DecisionsKinesisSession");

                case "Container":
                    Log.Debug("Running in a non-ECS container. Using environment variables for credentials.");
                    return new AssumeRoleAWSCredentials(new EnvironmentVariablesAWSCredentials(), roleArn, "DecisionsKinesisSession");

                case "WindowsServer":
                    Log.Debug("Running on Windows Server. Using AWS SDK's default credential search order.");
                    return new AssumeRoleAWSCredentials(FallbackCredentialsFactory.GetCredentials(), roleArn, "DecisionsKinesisSession");

                default:
                    Log.Warn("Unable to detect specific environment. Falling back to default credential provider chain.");
                    return new AssumeRoleAWSCredentials(FallbackCredentialsFactory.GetCredentials(), roleArn, "DecisionsKinesisSession");
            }
        }

        private static string DetectEnvironment()
        {
            // Check for ECS (container environment)
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ECS_CONTAINER_METADATA_URI")))
            {
                return "ECS";
            }

            // Check for EC2 (could be container or Windows server)
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("EC2_INSTANCE_ID")))
            {
                return "EC2";
            }

            // Check if running in a container
            if (File.Exists("/.dockerenv"))
            {
                return "Container";
            }

            // If none of the above, assume it's a Windows server
            return "WindowsServer";
        }


        /// Extracts metadata from a Kinesis record.
        internal static List<DataPair> GetRecordMetadata(Record record)
        {
            Log.Debug($"Getting metadata for record with sequence number: {record.SequenceNumber}");
            try
            {
                return new List<DataPair>
                {
                    new DataPair("PartitionKey", record.PartitionKey),
                    new DataPair("SequenceNumber", record.SequenceNumber),
                    new DataPair("ApproximateArrivalTimestamp", record.ApproximateArrivalTimestamp.ToString("o"))
                };
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error getting metadata for record with sequence number: {record.SequenceNumber}");
                throw new Exception($"Failed to get record metadata: {ex.Message}", ex);
            }
        }

        /// Extracts all data from a Kinesis record, including its content.
        internal static List<DataPair> GetRecordData(Record record)
        {
            Log.Debug($"Getting data for record with sequence number: {record.SequenceNumber}");
            try
            {
                List<DataPair> data = new List<DataPair>
                {
                    new DataPair("PartitionKey", record.PartitionKey),
                    new DataPair("SequenceNumber", record.SequenceNumber),
                    new DataPair("ApproximateArrivalTimestamp", record.ApproximateArrivalTimestamp.ToString("o")),
                    new DataPair("EncryptionType", record.EncryptionType?.Value ?? "None")
                };

                // Convert the record's data to a string if it exists
                if (record.Data != null && record.Data.Length > 0)
                {
                    string dataAsString = Encoding.UTF8.GetString(record.Data.ToArray());
                    data.Add(new DataPair("Data", dataAsString));
                    Log.Debug($"Record data length: {dataAsString.Length} characters");
                }
                else
                {
                    Log.Debug("Record data is null or empty");
                }

                return data;
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error getting data for record with sequence number: {record.SequenceNumber}");
                throw new Exception($"Failed to get record data: {ex.Message}", ex);
            }
        }
    }
}