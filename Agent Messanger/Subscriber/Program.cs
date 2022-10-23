using Common;
using System;

namespace Subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsoleKey myKey = ConsoleKey.Enter;
            while (myKey != ConsoleKey.Escape) { 
                Console.WriteLine("\n~~~Subcscriber~~~\n");

                string topic;

                Console.Write("Topic: ");
                topic = Console.ReadLine().ToLower();

                var subscriberSocket = new SubscriberSocket(topic);

                subscriberSocket.Connect(Settings.BROKER_IP, Settings.BROKER_PORT);

                Console.WriteLine("Esc to exit...");
                myKey = Console.ReadKey().Key;
            }
        } 
    }
}
