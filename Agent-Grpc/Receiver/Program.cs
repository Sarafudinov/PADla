using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Grpc.Net.Client;
using GrpcAgeny;
using Microsoft.AspNetCore.Hosting.Server.Features;

namespace Receiver
{
    public class Program
    {
        public static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            var host = WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .UseUrls(EndpointsConstants.SubscribersAddress)
                .Build();

            host.Start();

            Subscribe(host);

            Console.WriteLine("Enter for exit...");
            Console.ReadLine();
        }
        private static void Subscribe(IWebHost host)
        {
            var channel = GrpcChannel.ForAddress(EndpointsConstants.BrokerAddress);
            var client = new Subscriber.SubscriberClient(channel);
            var address = host.ServerFeatures.Get<IServerAddressesFeature>().Addresses.First();
            Console.WriteLine($"Subscriber from address: {address}");

            Console.Write("Topic: ");
            var topic = Console.ReadLine().ToLower();

            var request = new SubscribeRequest() { Topic = topic, Address = address };

            try
            {
                var reply = client.Subscribe(request);
                Console.WriteLine($"Subscription response: {reply.IsSuccess}");

            }
            catch (Exception e)
            {
                Console.WriteLine($"Subscription error: {e.Message}");
            }
        }
    }
}
