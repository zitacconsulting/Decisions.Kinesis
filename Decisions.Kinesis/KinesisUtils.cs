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
        private static readonly Log Log = new Log("Kinesis");

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
            bool useRoleArn = queueDefinition.OverrideSettings ? queueDefinition.UseRoleArn : settings.UseRoleArn;
            Log.Debug($"Getting credentials using authentication method: {authMethod}");

            try
            {
                AWSCredentials baseCredentials;

                switch (authMethod)
                {
                    case "DefaultCredentials":
                        Log.Info("Using FallbackCredentialsFactory for default credentials");
                        baseCredentials = FallbackCredentialsFactory.GetCredentials();
                        break;

                    case "StaticCredentials":
                        Log.Info("Using StaticCredentials for authentication");
                        string accessKeyId = queueDefinition.OverrideSettings ? queueDefinition.AccessKeyId : settings.AccessKeyId;
                        string secretAccessKey = queueDefinition.OverrideSettings ? queueDefinition.SecretAccessKey : settings.SecretAccessKey;
                        if (string.IsNullOrEmpty(accessKeyId) || string.IsNullOrEmpty(secretAccessKey))
                        {
                            throw new ArgumentException("AccessKeyId and SecretAccessKey must be provided for StaticCredentials");
                        }
                        baseCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
                        break;

                    default:
                        throw new ArgumentException($"Invalid authentication method: {authMethod}");
                }

                if (useRoleArn)
                {
                    Log.Info("Using Role ARN for authentication");
                    string roleArn = queueDefinition.OverrideSettings ? queueDefinition.RoleArn : settings.RoleArn;
                    if (string.IsNullOrEmpty(roleArn))
                    {
                        throw new ArgumentException("RoleARN must be provided when UseRoleArn is true");
                    }

                    return new AssumeRoleAWSCredentials(baseCredentials, roleArn, "DecisionsKinesisSession");
                }
                else
                {
                    return baseCredentials;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error getting credentials for authentication method: {authMethod}\n{ex.Message}");
                throw;
            }
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
                    new DataPair("ApproximateArrivalTimestamp", record.ApproximateArrivalTimestamp.ToString())
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
                    new DataPair("EncryptionType", record.EncryptionType?.Value ?? "None")
                };

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
