using System;
using System.Collections.Generic;
using System.Text;

namespace Common
{
    public class Q
    {
        public string topic { get; }
        public string content { get; }

        public Q(string Topic, string Content)
        {
            topic = Topic;
            content = Content;
        }
    }
}
