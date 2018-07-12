using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NielsTest
{
    public class KafkaSqlClrProducer
    {
        KafkaOptions _options;
        BrokerRouter _router;
        Producer _client;

        public KafkaSqlClrProducer(Uri[] brokerList)
        {
            _options = new KafkaOptions(brokerList);
            _router = new BrokerRouter(_options);
            _client = new Producer(_router);
        }

        public async Task SendAsync(string topic, string msgValue, string key = null)
        {
            var msg = new Message(msgValue);
            if (!string.IsNullOrEmpty(key))
            {
                msg.Key = Encoding.UTF8.GetBytes(key.ToString());
            }

            var resps = await _client.SendMessageAsync(topic, new[] { msg }).ConfigureAwait(false);

        }

    }
}
