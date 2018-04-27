using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace MQTTServerLoadTest
{
    class Program
    {
        static readonly MqttFactory factory = new MqttFactory();

        private static readonly MqttClientOptionsBuilder options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", 1883)
            .WithCleanSession(false)
            .WithCommunicationTimeout(TimeSpan.FromSeconds(30));

        private const int usersCount = 2000;
        private const int usersPerGroups = 20;
        private const int userMessagePerSecond = 10;
        private const int userMessageInterval = (userMessagePerSecond > 0) ? 1000 / userMessagePerSecond : int.MaxValue;
        private const int userMessageCountBeforeReset = 200;

        private const MqttQualityOfServiceLevel connectionQOS = MqttQualityOfServiceLevel.AtLeastOnce;

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            int groupsCount = usersCount / usersPerGroups;

            var tasks = new List<Task>();

            for (var groupId = 0; groupId < groupsCount; groupId++)
            {
                var groupTopic = $"test/group/{groupId}";
                for (var userId = groupId * usersPerGroups; userId < (groupId + 1) * usersPerGroups; userId++)
                {
                    var id = userId;
                    tasks.Add(Task.Factory.StartNew(() => runClient(id, groupTopic), TaskCreationOptions.LongRunning));
                }
            }

            await Task.WhenAll(tasks);

        }

        static async Task runClient(int clientId, string topic)
        {
            var clientOptions = options.WithClientId($"dotnet-stress-test-{clientId}").Build();
            var mqttClient = factory.CreateMqttClient();
            logClientEvents(mqttClient, clientId);
            await mqttClient.ConnectAsync(clientOptions);
            var sub = await mqttClient.SubscribeAsync(topic, connectionQOS);
            if (sub.Any(x => x.ReturnCode == MqttSubscribeReturnCode.Failure))
            {
                throw new Exception($"client {clientId} subscribing to {topic} failed with QOS {connectionQOS}");
            }

            var tasks = new List<Task>();

            int messageNumber = 0;

            while (true)
            {
                tasks.Add(mqttClient.PublishAsync(topic, $"from {clientId} - {messageNumber}th message"));
                messageNumber++;
                await Task.Delay(userMessageInterval);

                //if (messageNumber % userMessageCountBeforeReset == 0)
                //{
                //    await mqttClient.DisconnectAsync();
                //    await mqttClient.ConnectAsync(clientOptions);
                //}
            }
        }

        static void logClientEvents(IMqttClient client, int clientId)
        {
            client.Connected += (s, e) => { Console.WriteLine($"client {clientId} connected. thread {Thread.CurrentThread.ManagedThreadId}"); };
            client.Disconnected += (s, e) => { Console.WriteLine($"client {clientId} disconnected. thread {Thread.CurrentThread.ManagedThreadId}"); };
        }
    }
}
