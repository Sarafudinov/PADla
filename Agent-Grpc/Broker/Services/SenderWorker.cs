using Broker.Services.Interfaces;
using Grpc.Core;
using GrpcAgeny;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Broker.Services
{
    public class SenderWorker : IHostedService
    {
        
        private Timer _timer;
        private const int TimeToWait = 2000;
        private readonly IMessageStorageService _messageStorage;
        private readonly IConnectionStorageService _connectionStorage;

        public SenderWorker(IServiceScopeFactory serviceScopeFactory)
        {
            using (var scope = serviceScopeFactory.CreateScope())
            {
                _messageStorage = scope.ServiceProvider.GetRequiredService<IMessageStorageService>();
                _connectionStorage = scope.ServiceProvider.GetRequiredService<IConnectionStorageService>();
            }
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _timer = new Timer(DoSendWork, null, 0, TimeToWait);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _timer?.Change(Timeout.Infinite, 0);
            return Task.CompletedTask;

        }
        private void DoSendWork(object state)
        {
            while (!_messageStorage.IsEmpty())
            {
                var message = _messageStorage.GetNext();

                if (message != null)
                {
                    Console.WriteLine($"To the Topic \" {message.Topic} \" subscribed " +
                        $"{_connectionStorage.GetConnectionsByTopic(message.Topic).Count} people");
                    int i = 0;
                    while (i==0)
                    {
                        if (_connectionStorage.GetConnectionsByTopic(message.Topic).Count == 0) break;
                        
                        Console.WriteLine($"To the Topic {message.Topic} subscribed " +
                                                    $"{_connectionStorage.GetConnectionsByTopic(message.Topic).Count} people");
                        var connections = _connectionStorage.GetConnectionsByTopic(message.Topic);
                        foreach (var connection in connections)
                        {
                            var client = new Notifier.NotifierClient(connection.Channel);
                            var request = new NotifyRequest() { Content = message.Content };

                            try
                            {
                                var reply = client.Notify(request);
                            }
                            catch (RpcException rpcException)
                            {
                                if (rpcException.StatusCode == StatusCode.Internal)
                                {
                                    _connectionStorage.Remove(connection.Address);
                                }
                                Console.WriteLine($"Subscriber error {connection.Address}. {rpcException.Message}");
                            }
                            catch (Exception exception)
                            {
                                Console.WriteLine($"Subscriber notification error {connection.Address}. {exception.Message}");
                            }
                        i++;
                        break;
                        }
                    }
                }
            }
        }
    }
}
