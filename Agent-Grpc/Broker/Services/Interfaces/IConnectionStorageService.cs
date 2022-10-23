using Broker.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Broker.Services.Interfaces
{
    public interface IConnectionStorageService
    {
        void Add(Connection connection);
        void Remove(string address);

        IList<Connection> GetConnectionsByTopic(string topic);

    }
}
