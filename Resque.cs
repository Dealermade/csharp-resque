using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;

namespace Resque
{
    public class NoQueueError : Exception { }

    public class NoClassError : Exception { }

    public class Resque
    {
        private const string RESQUE_QUEUES_KEY = "resque:queues";
        private const string RESQUE_QUEUE_KEY_PREFIX = "resque:queue:";

        public const double Version = 1.0;
        public static Dictionary<string, Type> RegisteredJobs = new Dictionary<string, Type>();
        public static ConnectionMultiplexer redisClient;
        public static IDatabase db;

        public static void SetRedis(string hostname = "localhost", int port = 6379, int database = 0)
        {
            redisClient = ConnectionMultiplexer.Connect(String.Format("{0}:{1}", hostname, port));
            db = redisClient.GetDatabase(database);
        }

        public static void Push(string queue, JObject item)
        {
            db.SetAdd(RESQUE_QUEUES_KEY, queue);
            db.ListRightPush(RESQUE_QUEUE_KEY_PREFIX + queue, item.ToString());
        }

        public static JObject Pop(string queue)
        {
            var data = db.ListRightPop(RESQUE_QUEUE_KEY_PREFIX + queue);
            if (data.IsNullOrEmpty) return null;
            return JsonConvert.DeserializeObject<JObject>(data);
        }

        public static long Size(string queue)
        {
            return db.ListLength(RESQUE_QUEUE_KEY_PREFIX + queue);
        }

        public static bool Enqueue(string queue, string className, JObject arguments, bool trackStatus = false)
        {
            var argumentsArray = new JArray
                                     {
                                         arguments
                                     };
            var result = Job.Create(queue, className, argumentsArray, trackStatus);

            if (result)
            {
                Event.OnAfterEnqueue(className, arguments, queue, EventArgs.Empty);
            }

            return result;
        }

        public static Job Reserve(string queue)
        {
            return Job.Reserve(queue);
        }

        public static void AddJob(string className, Type type)
        {
            RegisteredJobs.Add(className, type);
        }

        public static RedisValue[] Queues()
        { 
            return db.ListRange(RESQUE_QUEUES_KEY);
        }
    }
}
