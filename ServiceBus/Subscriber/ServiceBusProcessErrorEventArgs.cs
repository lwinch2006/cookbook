using System;

namespace ServiceBusSubscriber;

public class ServiceBusProcessErrorEventArgs
{
	public string? QueueOrTopicName { get; set; }
	public string? SubscriptionName { get; set; }
	public Exception? Exception { get; set; } 
}