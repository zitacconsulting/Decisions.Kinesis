using System;
using DecisionsFramework.Data.ORMapper;
using Decisions.MessageQueues;

namespace Decisions.KinesisMessageQueue
{
    public class KinesisClusterNotification : BaseMqClusterNotification
    {
        public override BaseMqDefinition GetQueueDefinition(string queueEntityId)
        {
            return new ORM<KinesisMessageQueue>().Fetch(queueEntityId);
        }
    }
}