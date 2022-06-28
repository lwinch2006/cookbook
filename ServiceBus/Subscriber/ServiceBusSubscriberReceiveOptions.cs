namespace ServiceBusSubscriber
{
    public class ServiceBusSubscriberReceiveOptions
    {
        public string? SessionId { get; set; }
        
        public bool ConnectToDeadLetterQueue { get; set; }
        
        public ServiceBusSubscriberReceiveMessageTypes ReceiveMessageType { get; set; } 
            = ServiceBusSubscriberReceiveMessageTypes.Payload;
        
        public ServiceBusSubscriberCompleteMessageTypes CompleteMessageType { get; set; } =
            ServiceBusSubscriberCompleteMessageTypes.CompleteMessage;
    }
}