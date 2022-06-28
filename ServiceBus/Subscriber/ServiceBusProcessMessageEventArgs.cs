namespace ServiceBusSubscriber;

public class ServiceBusProcessMessageEventArgs
{
	public string? QueueOrTopicName { get; set; }
	public string? SubscriptionName { get; set; }
	public object? Message { get; set; } 
}