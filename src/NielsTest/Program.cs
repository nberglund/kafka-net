using KafkaNet;
using KafkaNet.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NielsTest
{
    
    class Program
    {
        static KafkaSqlClrProducer _producer;
        static Uri[] _uris;

        static void Main(string[] args)
        {

            _uris = new[] { new Uri("http://rhel1:9092") };

            var nCount = 10;
            for (int x = 0; x < nCount; x++)
            {
                var msg = $"Hello World: {x}";
                SendUseKafkaProducer(msg);
                Thread.Sleep(100);
            }

            Console.ReadLine();
        }

        static async void SendUseKafkaNet()
        {
            var options = new KafkaOptions(new Uri("http://rhel1:9092"));

            

            var router = new BrokerRouter(options);
            var client = new Producer(router);
           

            var msg = new KafkaNet.Protocol.Message("hello world");
            msg.Key = Encoding.UTF8.GetBytes(999999.ToString());
            //await client.SendMessageAsync("testing", new[] { msg }).ConfigureAwait(false);

            var resps = await client.SendMessageAsync("testing2", new[] { msg }).ConfigureAwait(false);
            foreach(var resp in resps)
            {
                Console.WriteLine($"Offset: {resp.Offset}");
            }
            Console.WriteLine("Message Sent");

        }

        static async void SendUseKafkaProducer(string msg)
        {
            if(_producer == null)
            {
                _producer = new KafkaSqlClrProducer(_uris);
            }

            await _producer.SendAsync("testing", msg).ConfigureAwait(false);
        }
    }
}
