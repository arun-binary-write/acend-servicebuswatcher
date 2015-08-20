

namespace ServiceBusWatcherTest
{
    using Microsoft.ServiceBus;
    using Microsoft.WindowsAzure;
    using Newtonsoft.Json;
    using SendGrid;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Mail;
    using System.Threading;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            var namespaceManager = NamespaceManager.CreateFromConnectionString(CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString"));

            var queues = CloudConfigurationManager.GetSetting("queues").Split(',');

            var intervalInMinutes = Convert.ToInt32(CloudConfigurationManager.GetSetting("pulse"));

            var pulseList = new List<ServiceBusQueuePulse>();



            var tasks = new List<Task>();

            queues.ToList().ForEach(queuename =>
            {
                tasks.Add(Task.Run(() =>
                {
                    ServiceBusQueuePulse previosPulse = null;

                    while (true)
                    {
                        var queue = namespaceManager.GetQueue(queuename);
                        var messageCountDetails = queue.MessageCountDetails;
                        var activeMessageCount = messageCountDetails.ActiveMessageCount;
                        var deadLetterMesageCountr = messageCountDetails.DeadLetterMessageCount;

                        var newPulse = new ServiceBusQueuePulse { LastAccessedAt = queue.AccessedAt, ActiveMessageCount = messageCountDetails.ActiveMessageCount, DeadLetterCount = messageCountDetails.DeadLetterMessageCount };

                        if (previosPulse != null)
                        {

                            var serviceBusPulseReport = new ServiceBusQueuePulseReport
                            {
                                IsLastAccessedAtFailed = previosPulse.LastAccessedAt == newPulse.LastAccessedAt,
                                IsActiveMessageCountIncreased = previosPulse.ActiveMessageCount <= newPulse.ActiveMessageCount,
                                IsDeadLetterCountIncreased = previosPulse.DeadLetterCount < newPulse.DeadLetterCount
                            };

                            Console.WriteLine(JsonConvert.SerializeObject(newPulse));

                            if (serviceBusPulseReport.IsLastAccessedAtFailed || serviceBusPulseReport.IsActiveMessageCountIncreased || serviceBusPulseReport.IsDeadLetterCountIncreased)
                            {
                                var message = JsonConvert.SerializeObject(serviceBusPulseReport);
                                Console.WriteLine(message);
                                SendAlert(message, queuename);
                            }

                        }

                        previosPulse = newPulse;

                        Thread.Sleep(TimeSpan.FromMinutes(intervalInMinutes));
                    }
                }));

            });

            

            Task.WaitAll(tasks.ToArray());

        }

        static void SendAlert(string text, string queueName)
        {

            // Create network credentials to access your SendGrid account
            var username = CloudConfigurationManager.GetSetting("emailusername");
            var pswd = CloudConfigurationManager.GetSetting("emailpassword");
            var toAddresses = CloudConfigurationManager.GetSetting("to").Split(',');

            var credentials = new NetworkCredential(username, pswd);

            // Create the email object first, then add the properties.
            SendGridMessage myMessage = new SendGridMessage();
            toAddresses.ToList().ForEach(email => myMessage.AddTo(email));
            //myMessage.AddTo("anna@example.com");
            myMessage.From = new MailAddress(username, "Ascend Service Bus Monitor");
            myMessage.Subject = string.Format("Service Bus Queue {0}", queueName);
            myMessage.Text = text;



            // Create an Web transport for sending email.
            var transportWeb = new Web(credentials);

            // Send the email.
            // You can also use the **DeliverAsync** method, which returns an awaitable task.
            transportWeb.DeliverAsync(myMessage);
        }
    }

    class ServiceBusQueuePulse
    {
        public DateTime LastAccessedAt { get; set; }

        public long ActiveMessageCount { get; set; }

        public long DeadLetterCount { get; set; }
    }

    class ServiceBusQueuePulseReport
    {
        public bool IsLastAccessedAtFailed { get; set; }

        public bool IsActiveMessageCountIncreased { get; set; }

        public bool IsDeadLetterCountIncreased { get; set; }
    }


}