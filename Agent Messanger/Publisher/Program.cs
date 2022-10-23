using Common;
using Newtonsoft.Json;
using System;
using System.Text;

namespace Publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("\n~~~Publisher~~~\n");

            var publicherSocket = new PublisherSocket();
            publicherSocket.Connect(Settings.BROKER_IP, Settings.BROKER_PORT);

            if (publicherSocket.IsConnected)
            {
                while (true)
                {
                    var payload = new Payload();

                    Console.Write("\nTopic: ");
                    payload.Topic = Console.ReadLine().ToLower();
                   
                    Console.Write("\nMessage: ");
                    payload.Message = Console.ReadLine();

                    var payloadString = JsonConvert.SerializeObject(payload);
                    byte[] data = Encoding.UTF8.GetBytes(payloadString);

                    publicherSocket.Send(data);
                }
            }

            Console.ReadLine();
        }
    }
}
