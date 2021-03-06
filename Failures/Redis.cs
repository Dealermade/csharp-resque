﻿using System;
using Newtonsoft.Json.Linq;

namespace Resque.Failures
{
    public class Redis : Backend
    {
        public Redis(JObject payload, Exception exception, Worker worker, String queue)
        {
            Exception = exception;
            Worker = worker;
            Queue = queue;
            Payload = payload;

            var data = new JObject
                           {
                               new JProperty("failed_at", DateTime.Now),
                               new JProperty("payload", Payload),
                               new JProperty("error", Exception.Message),
                               new JProperty("backtrace", Exception.ToString()),
                               new JProperty("worker", Worker.ToString()),
                               new JProperty("queue", Queue)
                           };

            Resque.db.ListRightPush("resque:failed", data.ToString());
        }
    }
}
