using Broker.Models;
using Broker.Services.Interfaces;
using Grpc.Core;
using GrpcAgeny;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;

namespace Broker.Services
{
    public class SubscriberService : Subscriber.SubscriberBase
    {
        private readonly IConnectionStorageService _connectionStorage;
        public SubscriberService(IConnectionStorageService connectionStorage)
        {
            _connectionStorage = connectionStorage;
        }
        public override Task<SubscribeReply> Subscribe(SubscribeRequest request, ServerCallContext context)
        {

            Console.WriteLine($"The new subscriber tries to connect: {request.Address} {request.Topic}");
            try
            {
                var connection = new Connection(request.Address, request.Topic);
                _connectionStorage.Add(connection);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Connection not added: {request.Address} {request.Topic} {e.Message}");
            }

            return Task.FromResult(new SubscribeReply()
            {
                IsSuccess = true
            }); 
        }
    }
}
