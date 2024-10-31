using DecisionsFramework;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Data.Messaging;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.ConfigurationStorage;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.Properties.Attributes;
using DecisionsFramework.ServiceLayer.Utilities;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Decisions.MessageQueues;


namespace Decisions.KinesisMessageQueue
{
    public class KinesisMessageQueue : BaseMqDefinition
    {
        public override string LogCategory => "Kinesis";
        protected override IMessageQueue CreateNewQueueImpl() => new KinesisMessageQueueImpl(this);
        protected override BaseMqClusterNotification CreateNewClusterNotification() => new KinesisClusterNotification();

        private ORMOneToManyRelationship<KinesisPayloadFilter.PayloadFilter> payloadFilters;

        [ORMField]
        [WritableValue]
        private string streamName;

        [DataMember]
        [PropertyClassification(1, "Stream Name", "1 Definition")]
        public string StreamName
        {
            get { return streamName; }
            set
            {
                streamName = value;
                OnPropertyChanged();
            }
        }

        [ReadonlyEditor]
        [PropertyClassification(2, "Stream Position", new string[] { "1 Definition" })]
        public string InputTypeANote
        {
            get => "Determines where to start reading from the stream when no checkpoint exists. This setting only applies when the stream is processed for the first time.";
            set
            {
            }
        }

        [ORMField]
        [WritableValue]
        private string initialStreamPosition;

        [DataMember]
        [PropertyClassification(3, "Initial Stream Position", "1 Definition")]
        [SelectStringEditor("InitialStreamPositionOptions")]
        public string InitialStreamPosition
        {
            get { return initialStreamPosition; }
            set
            {
                initialStreamPosition = value;
                OnPropertyChanged();
            }
        }

        [DataMember]
        [PropertyClassification(4, "JSON Payload Filters", "1 Definition")]
        public KinesisPayloadFilter.PayloadFilter[] PayloadFilters
        {
            get { return payloadFilters.Items; }
            set { payloadFilters.Items = value; }
        }

        [ORMField]
        [WritableValue]
        private int maxRecordsPerRequest = 100;

        [DataMember]
        [PropertyClassification(11, "Max Records Per Request", "2 Advanced")]
        public int MaxRecordsPerRequest
        {
            get { return maxRecordsPerRequest; }
            set
            {
                maxRecordsPerRequest = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        private int maxRetries = 3;

        [DataMember]
        [PropertyClassification(12, "Max Retries", "2 Advanced")]
        public int MaxRetries
        {
            get { return maxRetries; }
            set
            {
                maxRetries = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        private int requestTimeout = 10;

        [DataMember]
        [PropertyClassification(13, "Request Timeout (seconds)", "2 Advanced")]
        public int RequestTimeout
        {
            get { return requestTimeout; }
            set
            {
                requestTimeout = value;
                OnPropertyChanged();
            }
        }

        [ReadonlyEditor]
        [PropertyClassification(2, "Polling Settings", new string[] { "2 Advanced" })]
        public string InputTypeANote2
        {
            get => "'Shard Poll Interval' defines how long to wait before checking for new messages in the shard. 'Shard Wait Between Batches Interval' defines how long to wait between batches if there are more messages than 'Max Records Per Requests' in the shard";
            set
            {
            }
        }

        [ORMField]
        [WritableValue]
        private int shardPollInterval = 30;

        [DataMember]
        [PropertyClassification(15, "Shard Poll Interval (seconds)", "2 Advanced")]
        public int ShardPollInterval
        {
            get { return shardPollInterval; }
            set
            {
                shardPollInterval = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        private int shardBatchWaitTime = 1;

        [DataMember]
        [PropertyClassification(16, "Shard Wait Between Batches Interval (seconds)", "2 Advanced")]
        public int ShardBatchWaitTime
        {
            get { return shardBatchWaitTime; }
            set
            {
                shardBatchWaitTime = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        private int errorBackoffTime = 5;

        [DataMember]
        [PropertyClassification(17, "Error Backoff Time (seconds)", "2 Advanced")]
        public int ErrorBackoffTime
        {
            get { return errorBackoffTime; }
            set
            {
                errorBackoffTime = value;
                OnPropertyChanged();
            }
        }


        [ORMField]
        [WritableValue]
        private bool overrideSettings;

        [DataMember]
        [PropertyClassification(1, "Override Default Settings", "3 Connection")]
        public bool OverrideSettings
        {
            get { return overrideSettings; }
            set
            {
                overrideSettings = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        private string region;

        [DataMember]
        [PropertyClassification(2, "AWS Region", "3 Connection")]
        [SelectStringEditor("RegionOptions")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public string Region
        {
            get { return region; }
            set
            {
                region = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        private string authenticationMethod;

        [DataMember]
        [PropertyClassification(3, "Authentication Method", "3 Connection")]
        [SelectStringEditor("AuthenticationMethods")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public string AuthenticationMethod
        {
            get { return authenticationMethod; }
            set
            {
                authenticationMethod = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [PropertyClassification(4, "Access Key ID", "3 Connection")]
        [WritableValue]
        [PropertyHiddenByValue(nameof(AuthenticationMethod), "StaticCredentials", false)]
        public string AccessKeyId { get; set; }

        [ORMField(4000, typeof(FixedLengthStringFieldConverter))]
        [PropertyClassification(5, "Secret Access Key", "3 Connection")]
        [PasswordText]
        [WritableValue]
        [PropertyHiddenByValue(nameof(AuthenticationMethod), "StaticCredentials", false)]
        public string SecretAccessKey { get; set; }

        [ORMField]
        [WritableValue]
        private bool useRoleArn;

        [DataMember]
        [PropertyClassification(5, "Assume Role", "3 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public bool UseRoleArn
        {
            get { return useRoleArn; }
            set
            {
                useRoleArn = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        [DataMember]
        [PropertyClassification(7, "Role ARN", "3 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        [BooleanPropertyHidden(nameof(UseRoleArn), false)]
        public string RoleArn { get; set; }

        [PropertyHidden]
        public string[] InitialStreamPositionOptions
        {
            get
            {
                return new string[]
                {
                "Start from oldest record",
                "Start from latest record"
                };
            }
        }

        [PropertyHidden]
        public string[] RegionOptions
        {
            get
            {
                return new string[]
                {
                    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                    "eu-west-1", "eu-west-2", "eu-central-1",
                    "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
                    "sa-east-1"
                };
            }
        }

        [PropertyHidden]
        public string[] AuthenticationMethods
        {
            get
            {
                return new string[]
                {
                    "DefaultCredentials",
                    "StaticCredentials"
                };
            }
        }

        public override void Read(ReadStream stream)
        {
            base.Read(stream);
            if (stream.ContainsValue("filters_Count"))
            {
                List<KinesisPayloadFilter.PayloadFilter> filterList = new List<KinesisPayloadFilter.PayloadFilter>();
                for (int index = 0; index < stream.GetValue<int>("filters_Count"); ++index)
                {
                    if (stream.ContainsValue($"filters_{index + 1}") && stream.GetValue<byte[]>($"filters_{index + 1}") != null)
                        filterList.Add(ObjectGraphSerializer.Deserialize(stream.GetValue<byte[]>($"filters_{index + 1}"), DataGraph.ReferenceUpdatePolicy.Inherit) as KinesisPayloadFilter.PayloadFilter);
                }
                this.payloadFilters.Items = filterList.ToArray();
            }
        }

        public override void Save(WriteStream stream)
        {
            base.Save(stream);
            if (this.payloadFilters != null && this.payloadFilters.Items.Any())
            {
                stream.Add("filters_Count", this.payloadFilters.Items.Length);
                for (int index = 0; index < this.payloadFilters.Items.Length; ++index)
                    stream.Add($"filters_{index + 1}", ObjectGraphSerializer.Serialize(this.payloadFilters.Items[index]));
            }
        }
        public override ValidationIssue[] GetAdditionalValidationIssues()
        {
            List<ValidationIssue> issues = new List<ValidationIssue>();

            if (string.IsNullOrEmpty(StreamName))
                issues.Add(new ValidationIssue(this, "Stream name must be supplied", "", BreakLevel.Fatal));

            if (string.IsNullOrEmpty(InitialStreamPosition))
                issues.Add(new ValidationIssue(this, "Initial Stream Position must be selected", "", BreakLevel.Fatal));

            if (MaxRecordsPerRequest < 1 || MaxRecordsPerRequest > 10000)
                issues.Add(new ValidationIssue(this, "Max Records Per Request must be between 1 and 10000", "", BreakLevel.Fatal));

            if (MaxRetries < 0)
                issues.Add(new ValidationIssue(this, "Max Retries must be a non-negative integer", "", BreakLevel.Fatal));

            if (RequestTimeout < 1)
                issues.Add(new ValidationIssue(this, "Request Timeout must be at least 1 second", "", BreakLevel.Fatal));

            if (OverrideSettings)
            {
                if (string.IsNullOrEmpty(Region))
                    issues.Add(new ValidationIssue(this, "Region must be supplied if settings are overridden", "", BreakLevel.Fatal));

                if (string.IsNullOrEmpty(AuthenticationMethod))
                    issues.Add(new ValidationIssue(this, "Authentication method must be selected if settings are overridden", "", BreakLevel.Fatal));

                if (UseRoleArn && string.IsNullOrEmpty(RoleArn))
                    issues.Add(new ValidationIssue(this, "Role ARN must be supplied when using Role ARN authentication", "", BreakLevel.Fatal));

                if (AuthenticationMethod == "StaticCredentials" && (string.IsNullOrEmpty(AccessKeyId) || string.IsNullOrEmpty(SecretAccessKey)))
                    issues.Add(new ValidationIssue(this, "Access Key ID and Secret Access Key must be supplied when using Static Credentials", "", BreakLevel.Fatal));
            }
            if (PayloadFilters != null)
            {
                foreach (var filter in PayloadFilters)
                {
                    if (string.IsNullOrEmpty(filter.Property))
                        issues.Add(new ValidationIssue(this, "Filter property cannot be empty", "", BreakLevel.Fatal));
                    if (string.IsNullOrEmpty(filter.Verb))
                        issues.Add(new ValidationIssue(this, "Filter verb cannot be empty", "", BreakLevel.Fatal));
                    if (string.IsNullOrEmpty(filter.Value))
                        issues.Add(new ValidationIssue(this, "Filter value cannot be empty", "", BreakLevel.Fatal));
                }
            }
            return issues.ToArray();
        }
        public KinesisMessageQueue()
        {
            ORMOneToManyRelationship<KinesisPayloadFilter.PayloadFilter> filterRelationship = new ORMOneToManyRelationship<KinesisPayloadFilter.PayloadFilter>("kinesis_message_queue_id", false);
            filterRelationship.DeleteOrphans = true;
            this.payloadFilters = filterRelationship;
        }
    }
}