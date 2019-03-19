package com.microsoft.azure.servicebus.primitives;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.security.SecurityToken;

final class CommonRequestResponseOperations {
	static CompletableFuture<Collection<Message>> peekMessagesAsync(RequestResponseLink requestResponseLink, Duration operationTimeout, long fromSequenceNumber, int messageCount, String sessionId, String associatedLinkName)
	{
		HashMap requestBodyMap = new HashMap();
		requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_FROM_SEQUENCE_NUMER, fromSequenceNumber);
		requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_MESSAGE_COUNT, messageCount);		
		if(sessionId != null)
		{
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_SESSIONID, sessionId);
		}
		Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_PEEK_OPERATION, requestBodyMap, Util.adjustServerTimeout(operationTimeout), associatedLinkName);
		CompletableFuture<Message> responseFuture = requestResponseLink.requestAysnc(requestMessage, operationTimeout);
		return responseFuture.thenComposeAsync((responseMessage) -> {
			CompletableFuture<Collection<Message>> returningFuture = new CompletableFuture<Collection<Message>>();
			int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
			if(statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE)
			{
				List<Message> peekedMessages = new ArrayList<Message>();
				Object responseBodyMap = ((AmqpValue)responseMessage.getBody()).getValue();
				if(responseBodyMap != null && responseBodyMap instanceof Map)
				{					
					Object messages = ((Map)responseBodyMap).get(ClientConstants.REQUEST_RESPONSE_MESSAGES);
					if(messages != null && messages instanceof Iterable)
					{
						for(Object message : (Iterable)messages)
						{
							if(message instanceof Map)
							{
								Message peekedMessage = Message.Factory.create();
								Binary messagePayLoad = (Binary)((Map)message).get(ClientConstants.REQUEST_RESPONSE_MESSAGE);
								peekedMessage.decode(messagePayLoad.getArray(), messagePayLoad.getArrayOffset(), messagePayLoad.getLength());
								peekedMessages.add(peekedMessage);
							}
						}
					}
				}
				returningFuture.complete(peekedMessages);
			}
			else if(statusCode == ClientConstants.REQUEST_RESPONSE_NOCONTENT_STATUS_CODE ||
					(statusCode == ClientConstants.REQUEST_RESPONSE_NOTFOUND_STATUS_CODE && ClientConstants.MESSAGE_NOT_FOUND_ERROR.equals(RequestResponseUtils.getResponseErrorCondition(responseMessage))))
			{
				returningFuture.complete(new ArrayList<Message>());
			}
			else
			{
				// error response
			    Exception failureException = RequestResponseUtils.genereateExceptionFromResponse(responseMessage);
				returningFuture.completeExceptionally(failureException);
			}
			return returningFuture;
		}, MessagingFactory.INTERNAL_THREAD_POOL);
	}
	
	static CompletableFuture<Void> sendCBSTokenAsync(RequestResponseLink requestResponseLink, Duration operationTimeout, SecurityToken securityToken)
	{
        Message requestMessage = RequestResponseUtils.createRequestMessageFromValueBody(ClientConstants.REQUEST_RESPONSE_PUT_TOKEN_OPERATION, securityToken.getTokenValue(), Util.adjustServerTimeout(operationTimeout));
        requestMessage.getApplicationProperties().getValue().put(ClientConstants.REQUEST_RESPONSE_PUT_TOKEN_TYPE, securityToken.getTokenType().toString());
        requestMessage.getApplicationProperties().getValue().put(ClientConstants.REQUEST_RESPONSE_PUT_TOKEN_AUDIENCE, securityToken.getTokenAudience());
        CompletableFuture<Message> responseFuture = requestResponseLink.requestAysnc(requestMessage, operationTimeout);
        return responseFuture.thenComposeAsync((responseMessage) -> {
            CompletableFuture<Void> returningFuture = new CompletableFuture<Void>();
            int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
            if(statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE || statusCode == ClientConstants.REQUEST_RESPONSE_ACCEPTED_STATUS_CODE)
            {
                returningFuture.complete(null);
            }
            else
            {
                // error response
                Exception failureException = RequestResponseUtils.genereateExceptionFromResponse(responseMessage);
                returningFuture.completeExceptionally(failureException);
            }
            return returningFuture;
        }, MessagingFactory.INTERNAL_THREAD_POOL);
	}
}
