using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using ServiceBusMessages;

namespace ServiceBusSubscriber
{
    public class ServiceBusSubscriber : IServiceBusSubscriber, IServiceBusSubscriberForDeadLetter
    {
        private readonly ServiceBusClient _client;
        private readonly ServiceBusAdministrationClient _administrationClient;
        private readonly ServiceBusSubscriberConfiguration _serviceBusSubscriberConfiguration;
        private readonly ILogger<ServiceBusSubscriber> _logger;

        private ServiceBusSubscriberProcessor? _serviceBusSubscriberProcessor;
        
        public ServiceBusSubscriber(
            ServiceBusClient client,
            ServiceBusAdministrationClient administrationClient,
            ServiceBusSubscriberConfiguration serviceBusSubscriberConfiguration,
            ILogger<ServiceBusSubscriber> logger)
        {
            _client = client;
            _administrationClient = administrationClient;
            _serviceBusSubscriberConfiguration = serviceBusSubscriberConfiguration;
            _logger = logger;
        }

        public async Task<T?> ReceiveMessage<T>(
            string queueOrTopicName, 
            string? subscriptionName = default,
            ServiceBusSubscriberReceiveOptions? options = default, 
            CancellationToken cancellationToken = default)
            where T : class
        {
            await using var receiver = await GetServiceBusReceiver(queueOrTopicName, subscriptionName, options, cancellationToken);
            var serviceBusMessage = await receiver.ReceiveMessageAsync(cancellationToken: cancellationToken);
            if (serviceBusMessage == null)
            {
                return null;
            }
            
            var receiveMessageType = options?.ReceiveMessageType ?? ServiceBusSubscriberReceiveMessageTypes.Payload;
            var payload = GetPayload<T>(serviceBusMessage, receiveMessageType, cancellationToken);
            await receiver.CompleteMessageAsync(serviceBusMessage, cancellationToken);
            return payload;
        }

        public async Task<object?> ReceiveMessage(
            string queueOrTopicName,
            string? subscriptionName = default,
            ServiceBusSubscriberReceiveOptions? options = default,
            CancellationToken cancellationToken = default)
        {
            await using var receiver = await GetServiceBusReceiver(queueOrTopicName, subscriptionName, options, cancellationToken);
            var serviceBusMessage = await receiver.ReceiveMessageAsync(cancellationToken: cancellationToken);
            if (serviceBusMessage == null)
            {
                return null;
            }

            var receiveMessageType = options?.ReceiveMessageType ?? ServiceBusSubscriberReceiveMessageTypes.Payload;
            var payload = GetPayload(serviceBusMessage, receiveMessageType, cancellationToken);
            await receiver.CompleteMessageAsync(serviceBusMessage, cancellationToken);
            return payload;
        }

        public async IAsyncEnumerable<T> ReceiveMessages<T>(
            string queueOrTopicName, 
            string? subscriptionName = default,
            ServiceBusSubscriberReceiveOptions? options = default, 
            CancellationToken cancellationToken = default)
            where T : class
        {
            var receiveMessageType = options?.ReceiveMessageType ?? ServiceBusSubscriberReceiveMessageTypes.Payload;
            await using var receiver = await GetServiceBusReceiver(queueOrTopicName, subscriptionName, options, cancellationToken);

            await foreach (var serviceBusMessage in receiver.ReceiveMessagesAsync(cancellationToken))
            {
                var payload = GetPayload<T>(serviceBusMessage, receiveMessageType, cancellationToken);
                await receiver.CompleteMessageAsync(serviceBusMessage, cancellationToken);
                yield return payload;
            }
        }

        public async IAsyncEnumerable<object> ReceiveMessages(
            string queueOrTopicName,
            string? subscriptionName = default, 
            ServiceBusSubscriberReceiveOptions? options = default,
            CancellationToken cancellationToken = default)
        {
            var receiveMessageType = options?.ReceiveMessageType ?? ServiceBusSubscriberReceiveMessageTypes.Payload;
            await using var receiver = await GetServiceBusReceiver(queueOrTopicName, subscriptionName, options, cancellationToken);

            await foreach (var serviceBusMessage in receiver.ReceiveMessagesAsync(cancellationToken))
            {
                var payload = GetPayload(serviceBusMessage, receiveMessageType, cancellationToken);
                await receiver.CompleteMessageAsync(serviceBusMessage, cancellationToken);
                yield return payload;
            }
        }

        public async Task StartReceiveMessages(
            string queueOrTopicName,
            string? subscriptionName = default,
            Func<ServiceBusProcessMessageEventArgs, Task>? processMessageFunc = default,
            Func<ServiceBusProcessErrorEventArgs, Task>? processErrorFunc = default,
            ServiceBusSubscriberReceiveOptions? options = default,
            CancellationToken cancellationToken = default)
        {
            if (_serviceBusSubscriberProcessor != null)
            {
                _logger.LogError("Service Bus processor already initialized. Stop it first");
                throw new ServiceBusSubscriberOperationException(
                    "Service Bus processor already initialized. Stop it first.");
            }

            try
            {
                _serviceBusSubscriberProcessor =
                    new ServiceBusSubscriberProcessor(_client, queueOrTopicName, subscriptionName, options)
                    {
                        QueueOrTopicName = queueOrTopicName,
                        SubscriptionName = subscriptionName,
                        ClientProcessMessageFunc = processMessageFunc,
                        ClientProcessErrorFunc = processErrorFunc
                    };
                
                _serviceBusSubscriberProcessor.ProcessMessageAsync += ProcessMessageInternal;
                _serviceBusSubscriberProcessor.ProcessErrorAsync += ProcessErrorInternal;

                await _serviceBusSubscriberProcessor.StartProcessingAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                await StopReceiveMessagesInternal(cancellationToken);
                
                _logger.LogError(ex, "Service Bus start receive messages from topic subscription failure");
                
                throw new ServiceBusSubscriberOperationException(
                    "Service Bus start receive messages from topic subscription failure", ex);
            }
        }

        public async Task StopReceiveMessages(CancellationToken cancellationToken = default)
        {
            if (_serviceBusSubscriberProcessor == null)
            {
                _logger.LogError("Service Bus processor not initialized");
                throw new ServiceBusSubscriberOperationException("Service Bus processor not initialized");
            }

            await StopReceiveMessagesInternal(cancellationToken);
        }

        public async Task EnsureTopicSubscription(
            string topicName, 
            string subscriptionName, 
            CancellationToken cancellationToken = default,
            string sqlFilterRule = "1=1")
        {
            try
            {
                if (!await _administrationClient.SubscriptionExistsAsync(topicName, subscriptionName, cancellationToken))
                {
                    const string defaultRuleName = "$Default";
                
                    var subscriptionOptions = new CreateSubscriptionOptions(topicName, subscriptionName)
                    {
                        Status = EntityStatus.Active,
                        MaxDeliveryCount = 10,
                        LockDuration = TimeSpan.FromSeconds(60)
                    };

                    var filterOptions = new CreateRuleOptions
                    {
                        Name = defaultRuleName,
                        Filter = new SqlRuleFilter(string.IsNullOrWhiteSpace(sqlFilterRule) ? "1=1" : sqlFilterRule)
                    };

                    await _administrationClient.CreateSubscriptionAsync(subscriptionOptions, filterOptions, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Service Bus ensure topic subscription operation failure");
                
                throw new ServiceBusSubscriberOperationException(
                    "Service Bus ensure topic subscription operation failure", ex);
            }
        }
        
        public async Task<long> GetMessageCount(string queueOrTopicName, string? subscriptionName = default, bool inDeadLetterQueue = default)
        {
            if (string.IsNullOrWhiteSpace(queueOrTopicName))
            {
                return 0;
            }
            
            if (!string.IsNullOrWhiteSpace(subscriptionName))
            {
                var topicSubscription =
                    (await _administrationClient.GetSubscriptionRuntimePropertiesAsync(queueOrTopicName,
                        subscriptionName)).Value;

                return topicSubscription == null 
                    ? 0
                    : inDeadLetterQueue 
                        ? topicSubscription.DeadLetterMessageCount
                        : topicSubscription.ActiveMessageCount;
            }

            var queue = (await _administrationClient.GetQueueRuntimePropertiesAsync(queueOrTopicName)).Value;

            return queue == null
                ? 0
                : inDeadLetterQueue
                    ? queue.DeadLetterMessageCount
                    : queue.ActiveMessageCount;
        }        
        
        private object GetPayload(ServiceBusReceivedMessage serviceBusReceivedMessage, ServiceBusSubscriberReceiveMessageTypes receiveMessageType, CancellationToken cancellationToken = default)
        {
            if (receiveMessageType == ServiceBusSubscriberReceiveMessageTypes.FullMessage)
            {
                return serviceBusReceivedMessage;
            }
            
            if (!serviceBusReceivedMessage.ApplicationProperties.TryGetValue(nameof(Metadata.EventName), out var eventNameAsObject)
                || !serviceBusReceivedMessage.ApplicationProperties.TryGetValue(nameof(Metadata.Version), out var versionAsObject)) 
            {
                return GetPayload<object>(serviceBusReceivedMessage, receiveMessageType, cancellationToken);
            }

            var eventName = (string)eventNameAsObject;
            var version = (int)versionAsObject;
            
            if (!_serviceBusSubscriberConfiguration.DeserializationDictionary!.TryGetValue((version, eventName), out var payloadType))
            {
                return GetPayload<object>(serviceBusReceivedMessage, receiveMessageType, cancellationToken);
            }
            
            try
            {
                var message = JsonConvert.DeserializeObject(serviceBusReceivedMessage.Body.ToString(), payloadType)!;
                return receiveMessageType == ServiceBusSubscriberReceiveMessageTypes.Message
                    ? message
                    : message.GetType().GetProperty(nameof(ServiceBusMessage<object>.Payload))!.GetValue(message, null)!;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Service Bus received message get payload failure");
                throw new ServiceBusSubscriberOperationException("Service Bus received message get payload failure", ex);
            }
        }
        
        private T GetPayload<T>(ServiceBusReceivedMessage serviceBusReceivedMessage, ServiceBusSubscriberReceiveMessageTypes receiveMessageType, CancellationToken cancellationToken = default)
            where T : class
        {
            if (receiveMessageType == ServiceBusSubscriberReceiveMessageTypes.FullMessage)
            {
                return (serviceBusReceivedMessage as T)!;
            }
            
            try
            {
                var message = JsonConvert.DeserializeObject(serviceBusReceivedMessage.Body.ToString(), typeof(ServiceBusMessage<T>))!;
                return receiveMessageType == ServiceBusSubscriberReceiveMessageTypes.Message
                ? (T) message
                : ((ServiceBusMessage<T>)message).Payload!;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Service Bus received message get payload failure");
                throw new ServiceBusSubscriberOperationException("Service Bus received message get payload failure", ex);
            }
        }       
        
        private async Task ProcessMessageInternal(object argsAsObject)
        {
            var (queueOrTopicName, subscriptionName, clientProcessMessageFunc, receiveMessageType, completeMessageType) = (
                _serviceBusSubscriberProcessor!.QueueOrTopicName, 
                _serviceBusSubscriberProcessor.SubscriptionName,
                _serviceBusSubscriberProcessor.ClientProcessMessageFunc,
                _serviceBusSubscriberProcessor.ServiceBusSubscriberReceiveOptions!.ReceiveMessageType,
                _serviceBusSubscriberProcessor.ServiceBusSubscriberReceiveOptions.CompleteMessageType);
            
            if (clientProcessMessageFunc == null)
            {
                return;
            }

            try
            {
                var clientAgrs = new ServiceBusProcessMessageEventArgs
                {
                    QueueOrTopicName = queueOrTopicName,
                    SubscriptionName = subscriptionName,
                    Message = null
                };
                
                var message = _serviceBusSubscriberProcessor.GetReceivedMessageFromArgs(argsAsObject);

                if (message == null)
                {
                    await clientProcessMessageFunc(clientAgrs);
                    return;
                }
                
                var payload = GetPayload(message, receiveMessageType);
                clientAgrs.Message = payload;
                await clientProcessMessageFunc(clientAgrs);

                switch (completeMessageType)
                {
                    case ServiceBusSubscriberCompleteMessageTypes.CompleteMessage:
                        await _serviceBusSubscriberProcessor.CompleteMessageAsync(argsAsObject);
                        break;
                    
                    case ServiceBusSubscriberCompleteMessageTypes.AbandonMessage:
                        await _serviceBusSubscriberProcessor.AbandonMessageAsync(argsAsObject);
                        break;
                    
                    default:
                        await _serviceBusSubscriberProcessor.CompleteMessageAsync(argsAsObject);
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Service Bus received message processing failure");
                await ProcessErrorInternal(new ServiceBusSubscriberOperationException("Service Bus received message processing failure", ex));
            }
        }

        private async Task ProcessErrorInternal(ProcessErrorEventArgs args)
        {
            await ProcessErrorInternal(args.Exception);
        }
        
        private async Task ProcessErrorInternal(Exception ex)
        {
            _logger.LogError(ex, "Service Bus processor error");
            
            var (queueOrTopicName, subscriptionName, clientProcessErrorFunc) = (
                _serviceBusSubscriberProcessor!.QueueOrTopicName, 
                _serviceBusSubscriberProcessor.SubscriptionName,
                _serviceBusSubscriberProcessor.ClientProcessErrorFunc);            
            
            if (clientProcessErrorFunc == null)
            {
                return;
            }

            try
            {
                var clientArgs = new ServiceBusProcessErrorEventArgs
                {
                    QueueOrTopicName = queueOrTopicName,
                    SubscriptionName = subscriptionName,
                    Exception = ex
                };
                
                await clientProcessErrorFunc(clientArgs);
            }
            catch
            {
                _logger.LogError(ex, "Service Bus processor error processing failure");
            }
        }
        
        private async Task StopReceiveMessagesInternal(CancellationToken cancellationToken = default)
        {
            if (_serviceBusSubscriberProcessor != null)
            {
                try
                {
                    await _serviceBusSubscriberProcessor.CloseAsync(cancellationToken);
                    await _serviceBusSubscriberProcessor.DisposeAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Service Bus receive messages stopping failure");
                }
                finally
                {
                    _serviceBusSubscriberProcessor = null;
                }                
            }
        }

        private async Task<ServiceBusReceiver> GetServiceBusReceiver(
            string queueOrTopicName, 
            string? subscriptionName = default,
            ServiceBusSubscriberReceiveOptions? options = default,
            CancellationToken cancellationToken = default)
        {
            var (sessionId, _, connectToDeadLetterQueue) = (options?.SessionId, ServiceBusSubscriberReceiveMessageTypes.Payload, options?.ConnectToDeadLetterQueue ?? false);

            if (string.IsNullOrWhiteSpace(subscriptionName))
            {
                if (string.IsNullOrWhiteSpace(sessionId))
                {
                    return _client.CreateReceiver(
                        queueOrTopicName,
                        new ServiceBusReceiverOptions
                        {
                            SubQueue = connectToDeadLetterQueue ? SubQueue.DeadLetter : SubQueue.None
                        });
                }

                return await _client.AcceptSessionAsync(
                    queueOrTopicName, 
                    sessionId, 
                    cancellationToken: cancellationToken);
            }
            
            if (string.IsNullOrWhiteSpace(sessionId))
            {
                return _client.CreateReceiver(
                    queueOrTopicName, 
                    subscriptionName,
                    new ServiceBusReceiverOptions
                    {
                        SubQueue = connectToDeadLetterQueue ? SubQueue.DeadLetter : SubQueue.None
                    });
            }
            
            return await _client.AcceptSessionAsync(
                queueOrTopicName, 
                subscriptionName, 
                sessionId, 
                cancellationToken: cancellationToken);
        }
    }
}