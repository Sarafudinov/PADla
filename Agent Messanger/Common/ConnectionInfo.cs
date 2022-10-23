
using System.Net.Sockets;


namespace Common
{
  public class ConnectionInfo
    {

        public const int BUFF_SIZE = 1024;

        public byte[] Data { get; set; }
        public Socket Socket { get; set; }

        public string Address { get; set; }

        public string Topic { get; set; }

        public ConnectionInfo()
        {
            Data = new byte[BUFF_SIZE];
        }
    }
}
