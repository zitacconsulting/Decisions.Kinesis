using System;
using System.Runtime.Serialization;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Actions;
using DecisionsFramework.ServiceLayer.Utilities;
using DecisionsFramework.Utilities;

namespace Decisions.KinesisMessageQueue
{
    [ORMEntity("kinesis_checkpoint")]
    public class KinesisCheckpoint : AbstractEntity
    {
        [ORMPrimaryKeyField]
        [PropertyHidden]
        [DataMember]
        public string Id { get; set; }

        [ORMField("stream_name")]
        [DataMember]
        [WritableValue]
        public string StreamName { get; set; }

        [ORMField("shard_id")]
        [DataMember]
        [WritableValue]
        public string ShardId { get; set; }

        [ORMField("queue_id")]
        [DataMember]
        [WritableValue]
        public string QueueId { get; set; }

        [ORMField("sequence_number")]
        [DataMember]
        [WritableValue]
        public string SequenceNumber { get; set; }

        [ORMField("last_processed_timestamp")]
        [DataMember]
        [WritableValue]
        public DateTime LastProcessedTimestamp { get; set; }

        [ORMField("lease_owner")]
        [DataMember]
        [WritableValue]
        public string LeaseOwner { get; set; }

        [ORMField("lease_expiration_time")]
        [DataMember]
        [WritableValue]
        public DateTime LeaseExpirationTime { get; set; }

        [PropertyHidden]
        [DataMember]
        public override string EntityName
        {
            get { return $"{StreamName}-{ShardId}"; }
            set {}
        }

        [PropertyHidden]
        [DataMember]
        public override string EntityDescription
        {
            get { return $"Checkpoint for Stream: {StreamName}, Shard: {ShardId}"; }
            set {}
        }

        public KinesisCheckpoint()
        {
            Id = IDUtility.GetNewIdString();
        }

        public KinesisCheckpoint(string streamName, string queueId, string shardId, string sequenceNumber)
        {
            Id = IDUtility.GetNewIdString();
            StreamName = streamName;
            QueueId = queueId;
            ShardId = shardId;
            SequenceNumber = sequenceNumber;
            LastProcessedTimestamp = DateTime.UtcNow;
        }

        public override BaseActionType[] GetActions(AbstractUserContext userContext, EntityActionType[] types)
        {
            return new BaseActionType[0];
        }
    }
}