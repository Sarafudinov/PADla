using Broker.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Broker.Services.Interfaces
{
    public interface IMessageStorageService
    {
        void Add(Message message);
        Message GetNext();
        bool IsEmpty();

    }
}
