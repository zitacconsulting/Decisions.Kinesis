using System;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework;

namespace Decisions.KinesisMessageQueue
{
    public static class KinesisCheckpointer
    {
        private static readonly Log Log = new Log("KinesisCheckpointer");
        private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(1);

        public static void SaveCheckpoint(string streamName, string queueId, string shardId, string sequenceNumber)
        {
            Log.Debug($"Saving checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}, sequence: {sequenceNumber}");
            try
            {
                var orm = new ORM<KinesisCheckpoint>();
                var checkpoint = orm.Fetch(new WhereCondition[] {
                    new FieldWhereCondition("stream_name", QueryMatchType.Equals, streamName),
                    new FieldWhereCondition("queue_id", QueryMatchType.Equals, queueId),
                    new FieldWhereCondition("shard_id", QueryMatchType.Equals, shardId)
                }).FirstOrDefault();

                if (checkpoint == null)
                {
                    checkpoint = new KinesisCheckpoint(streamName, queueId, shardId, sequenceNumber);
                    orm.Store(checkpoint);
                    Log.Info($"Created new checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                }
                else
                {
                    checkpoint.SequenceNumber = sequenceNumber;
                    checkpoint.LastProcessedTimestamp = DateTime.UtcNow;
                    orm.Store(checkpoint, false, false, "sequence_number", "last_processed_timestamp");
                    Log.Info($"Updated existing checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Failed to save checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                throw new InvalidOperationException($"Failed to save checkpoint: {ex.Message}", ex);
            }
        }

        public static string GetCheckpoint(string streamName, string queueId, string shardId)
        {
            Log.Debug($"Getting checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}");
            try
            {
                var orm = new ORM<KinesisCheckpoint>();
                var checkpoint = orm.Fetch(new WhereCondition[] {
                    new FieldWhereCondition("stream_name", QueryMatchType.Equals, streamName),
                    new FieldWhereCondition("queue_id", QueryMatchType.Equals, queueId),
                    new FieldWhereCondition("shard_id", QueryMatchType.Equals, shardId)
                }).FirstOrDefault();
                if (checkpoint != null)
                {
                    Log.Info($"Retrieved checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}. Sequence: {checkpoint.SequenceNumber}");
                    return checkpoint.SequenceNumber;
                }
                else
                {
                    Log.Warn($"No checkpoint found for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Failed to get checkpoint for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                throw new InvalidOperationException($"Failed to get checkpoint: {ex.Message}", ex);
            }
        }

        public static bool AcquireLease(string streamName, string queueId, string shardId, string threadId)
        {
            Log.Debug($"Attempting to acquire lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
            try
            {
                var orm = new ORM<KinesisCheckpoint>();
                var checkpoint = orm.Fetch(new WhereCondition[] {
                    new FieldWhereCondition("stream_name", QueryMatchType.Equals, streamName),
                    new FieldWhereCondition("queue_id", QueryMatchType.Equals, queueId),
                    new FieldWhereCondition("shard_id", QueryMatchType.Equals, shardId)
                }).FirstOrDefault();

                if (checkpoint == null)
                {
                    checkpoint = new KinesisCheckpoint(streamName, queueId, shardId, null)
                    {
                        LeaseOwner = threadId,
                        LeaseExpirationTime = DateTime.UtcNow.Add(LeaseDuration)
                    };
                    orm.Store(checkpoint);
                    Log.Info($"Acquired new lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
                    return true;
                }

                if (checkpoint.LeaseExpirationTime < DateTime.UtcNow)
                {
                    checkpoint.LeaseOwner = threadId;
                    checkpoint.LeaseExpirationTime = DateTime.UtcNow.Add(LeaseDuration);
                    orm.Store(checkpoint, false, false, "lease_owner", "lease_expiration_time");
                    Log.Info($"Acquired expired lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
                    return true;
                }

                Log.Warn($"Failed to acquire lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}. Lease is currently held by: {checkpoint.LeaseOwner}");
                return false;
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error acquiring lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
                throw new InvalidOperationException($"Failed to acquire lease: {ex.Message}", ex);
            }
        }

        public static void ReleaseLease(string streamName, string queueId, string shardId, string threadId)
        {
            Log.Debug($"Releasing lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
            try
            {
                var orm = new ORM<KinesisCheckpoint>();
                var checkpoint = orm.Fetch(new WhereCondition[] {
                    new FieldWhereCondition("stream_name", QueryMatchType.Equals, streamName),
                    new FieldWhereCondition("queue_id", QueryMatchType.Equals, queueId),
                    new FieldWhereCondition("shard_id", QueryMatchType.Equals, shardId),
                    new FieldWhereCondition("lease_owner", QueryMatchType.Equals, threadId)
                }).FirstOrDefault();

                if (checkpoint != null)
                {
                    checkpoint.LeaseOwner = null;
                    checkpoint.LeaseExpirationTime = DateTime.UtcNow;
                    orm.Store(checkpoint, false, false, "lease_owner", "lease_expiration_time");
                    Log.Info($"Released lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
                }
                else
                {
                    Log.Warn($"No lease found to release for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error releasing lease for stream: {streamName}, queue: {queueId}, shard: {shardId}, thread: {threadId}");
                throw new InvalidOperationException($"Failed to release lease: {ex.Message}", ex);
            }
        }

        public static void ReleaseAllLeases(string streamName, string queueId, string threadId)
        {
            Log.Debug($"Releasing all leases for stream: {streamName}, queue: {queueId}, thread: {threadId}");
            try
            {
                var orm = new ORM<KinesisCheckpoint>();
                var checkpoints = orm.Fetch(new WhereCondition[] {
                    new FieldWhereCondition("stream_name", QueryMatchType.Equals, streamName),
                    new FieldWhereCondition("queue_id", QueryMatchType.Equals, queueId),
                    new FieldWhereCondition("lease_owner", QueryMatchType.Equals, threadId)
                });
                
                foreach (var checkpoint in checkpoints)
                {
                    checkpoint.LeaseOwner = null;
                    checkpoint.LeaseExpirationTime = DateTime.UtcNow;
                    orm.Store(checkpoint, false, false, "lease_owner", "lease_expiration_time");
                    Log.Info($"Released lease for stream: {streamName}, queue: {queueId}, shard: {checkpoint.ShardId}, thread: {threadId}");
                }

                Log.Info($"Released {checkpoints.Length} leases for stream: {streamName}, queue: {queueId}, thread: {threadId}");
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error releasing all leases for stream: {streamName}, queue: {queueId}, thread: {threadId}");
                throw new InvalidOperationException($"Failed to release all leases: {ex.Message}", ex);
            }
        }

        public static bool IsShardLeased(string streamName, string queueId, string shardId)
        {
            Log.Debug($"Checking if shard is leased for stream: {streamName}, queue: {queueId}, shard: {shardId}");
            try
            {
                var orm = new ORM<KinesisCheckpoint>();
                var checkpoint = orm.Fetch(new WhereCondition[] {
                    new FieldWhereCondition("stream_name", QueryMatchType.Equals, streamName),
                    new FieldWhereCondition("queue_id", QueryMatchType.Equals, queueId),
                    new FieldWhereCondition("shard_id", QueryMatchType.Equals, shardId)
                }).FirstOrDefault();

                if (checkpoint == null)
                {
                    Log.Info($"No lease found for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                    return false;
                }

                bool isLeased = !string.IsNullOrEmpty(checkpoint.LeaseOwner) && checkpoint.LeaseExpirationTime > DateTime.UtcNow;
                Log.Info($"Shard lease status for stream: {streamName}, queue: {queueId}, shard: {shardId} - IsLeased: {isLeased}");
                return isLeased;
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error checking shard lease for stream: {streamName}, queue: {queueId}, shard: {shardId}");
                throw new InvalidOperationException($"Failed to check shard lease: {ex.Message}", ex);
            }
        }
    }
}