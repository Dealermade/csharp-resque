using System;

namespace Resque
{
    public class Stat 
    {
        public static int Get(String stat)
        {
            return Int32.Parse(Resque.db.StringGet("resque:stat:" + stat));   
        }

        public static void Increment(String stat, int amt)
        {
            Resque.db.StringIncrement("resque:stat:" + stat, amt);
        }

        public static void Increment(String stat)
        {
            Resque.db.StringIncrement("resque:stat:" + stat, 1);
        }

        public static void Decrement(String stat)
        {
             Resque.db.StringDecrement("resque:stat:" + stat, 1);
        }

        public static void Clear(String stat)
        {
            Resque.db.KeyDelete("resque:stat:" + stat);
        }
    }
}
