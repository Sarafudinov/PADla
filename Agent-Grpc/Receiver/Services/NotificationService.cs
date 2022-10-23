using Grpc.Core;
using GrpcAgeny;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Receiver.Services
{
    public class NotificationService: Notifier.NotifierBase
    {
        public override Task<NotifyReply> Notify(NotifyRequest request, ServerCallContext context)
        {
            Console.WriteLine($"Received: {request.Content}");
            return Task.FromResult(new NotifyReply() { IsSuccess = true });
        }
    }
}
