package com.microsoft.azure.servicebus.primitives;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.rules.RuleDescription;

public final class MiscRequestResponseOperationHandler extends ClientEntity
{
    
	private final Object requestResonseLinkCreationLock = new Object();
	private final String entityPath;
	private final MessagingEntityType entityType;
	private final MessagingFactory underlyingFactory;
	private RequestResponseLink requestResponseLink;
	private CompletableFuture<Void> requestResponseLinkCreationFuture;
	
	private MiscRequestResponseOperationHandler(MessagingFactory factory, String linkName, String entityPath, MessagingEntityType entityType)
	{
		super(linkName);
		
		this.underlyingFactory = factory;
		this.entityPath = entityPath;
		this.entityType = entityType;
	}	
	
	@Deprecated
	public static CompletableFuture<MiscRequestResponseOperationHandler> create(MessagingFactory factory, String entityPath)
	{
	   return create(factory, entityPath, null);
	}
	
	public static CompletableFuture<MiscRequestResponseOperationHandler> create(MessagingFactory factory, String entityPath, MessagingEntityType entityType)
	{
	    MiscRequestResponseOperationHandler requestResponseOperationHandler = new MiscRequestResponseOperationHandler(factory, StringUtil.getShortRandomString(), entityPath, entityType);
	    return CompletableFuture.completedFuture(requestResponseOperationHandler);		
	}
	
	private void closeInternals()
	{
        this.closeRequestResponseLink();
	}
	
	@Override
	protected CompletableFuture<Void> onClose() {
	    this.closeInternals();
	    return CompletableFuture.completedFuture(null);
	}
	
	private CompletableFuture<Void> createRequestResponseLink()
	{
	    synchronized (this.requestResonseLinkCreationLock) {
            if(this.requestResponseLinkCreationFuture == null)
            {                
                this.requestResponseLinkCreationFuture = new CompletableFuture<Void>();
                this.underlyingFactory.obtainRequestResponseLinkAsync(this.entityPath, this.entityType).handleAsync((rrlink, ex) ->
                {
                    if(ex == null)
                    {
                        this.requestResponseLink = rrlink;
                        this.requestResponseLinkCreationFuture.complete(null);
                    }
                    else
                    {
                        Throwable cause = ExceptionUtil.extractAsyncCompletionCause(ex);
                        this.requestResponseLinkCreationFuture.completeExceptionally(cause);
                        // Set it to null so next call will retry rr link creation
                        synchronized (this.requestResonseLinkCreationLock)
                        {
                            this.requestResponseLinkCreationFuture = null;
                        }
                    }
                    return null;
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
            
            return this.requestResponseLinkCreationFuture;
        }
	}
	
	private void closeRequestResponseLink()
    {
	    synchronized (this.requestResonseLinkCreationLock)
        {
            if(this.requestResponseLinkCreationFuture != null)
            {
                this.requestResponseLinkCreationFuture.thenRun(() -> {
                    this.underlyingFactory.releaseRequestResponseLink(this.entityPath);
                    this.requestResponseLink = null;
                });
                this.requestResponseLinkCreationFuture = null;
            }
        }
    }
	
	public CompletableFuture<Pair<String[], Integer>> getMessageSessionsAsync(Date lastUpdatedTime, int skip, int top, String lastSessionId)
	{
		return this.createRequestResponseLink().thenComposeAsync((v) -> {
			HashMap requestBodyMap = new HashMap();
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_LAST_UPDATED_TIME, lastUpdatedTime);
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_SKIP, skip);
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_TOP, top);
			if(lastSessionId != null)
			{
				requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_LAST_SESSION_ID, lastSessionId);
			}
			
			Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_GET_MESSAGE_SESSIONS_OPERATION, requestBodyMap, Util.adjustServerTimeout(this.underlyingFactory.getOperationTimeout()));
			CompletableFuture<Message> responseFuture = this.requestResponseLink.requestAysnc(requestMessage, this.underlyingFactory.getOperationTimeout());
			return responseFuture.thenComposeAsync((responseMessage) -> {
				CompletableFuture<Pair<String[], Integer>> returningFuture = new CompletableFuture<Pair<String[], Integer>>();
				int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
				if(statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE)
				{				    
					Map responseBodyMap = RequestResponseUtils.getResponseBody(responseMessage);
					int responseSkip = (int)responseBodyMap.get(ClientConstants.REQUEST_RESPONSE_SKIP);
					String[] sessionIds = (String[])responseBodyMap.get(ClientConstants.REQUEST_RESPONSE_SESSIONIDS);
					returningFuture.complete(new Pair<>(sessionIds, responseSkip));				
				}
				else if(statusCode == ClientConstants.REQUEST_RESPONSE_NOCONTENT_STATUS_CODE ||
						(statusCode == ClientConstants.REQUEST_RESPONSE_NOTFOUND_STATUS_CODE && ClientConstants.SESSION_NOT_FOUND_ERROR.equals(RequestResponseUtils.getResponseErrorCondition(responseMessage))))
				{
					returningFuture.complete(new Pair<>(new String[0], 0));
				}
				else
				{
					// error response
					returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
				}
				return returningFuture;
			}, MessagingFactory.INTERNAL_THREAD_POOL);
		}, MessagingFactory.INTERNAL_THREAD_POOL);
	}
	
	public CompletableFuture<Void> removeRuleAsync(String ruleName)
	{
		return this.createRequestResponseLink().thenComposeAsync((v) -> {
			HashMap requestBodyMap = new HashMap();
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_RULENAME, ruleName);
			
			Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_REMOVE_RULE_OPERATION, requestBodyMap, Util.adjustServerTimeout(this.underlyingFactory.getOperationTimeout()));
			CompletableFuture<Message> responseFuture = this.requestResponseLink.requestAysnc(requestMessage, this.underlyingFactory.getOperationTimeout());
			return responseFuture.thenComposeAsync((responseMessage) -> {
				CompletableFuture<Void> returningFuture = new CompletableFuture<Void>();
				int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
				if(statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE)
				{
					returningFuture.complete(null);
				}
				else
				{
					// error response
					returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
				}
				return returningFuture;
			}, MessagingFactory.INTERNAL_THREAD_POOL);
		}, MessagingFactory.INTERNAL_THREAD_POOL);
	}
	
	public CompletableFuture<Void> addRuleAsync(RuleDescription ruleDescription)
	{
		return this.createRequestResponseLink().thenComposeAsync((v) -> {
			HashMap requestBodyMap = new HashMap();
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_RULENAME, ruleDescription.getName());
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_RULEDESCRIPTION, RequestResponseUtils.encodeRuleDescriptionToMap(ruleDescription));
			
			Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_ADD_RULE_OPERATION, requestBodyMap, Util.adjustServerTimeout(this.underlyingFactory.getOperationTimeout()));
			CompletableFuture<Message> responseFuture = this.requestResponseLink.requestAysnc(requestMessage, this.underlyingFactory.getOperationTimeout());
			return responseFuture.thenComposeAsync((responseMessage) -> {
				CompletableFuture<Void> returningFuture = new CompletableFuture<Void>();
				int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
				if(statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE)
				{
					returningFuture.complete(null);
				}
				else
				{
					// error response
					returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
				}
				return returningFuture;
			}, MessagingFactory.INTERNAL_THREAD_POOL);
		}, MessagingFactory.INTERNAL_THREAD_POOL);		
	}

	public CompletableFuture<Collection<RuleDescription>> getRulesAsync(int skip, int top)
	{
		return this.createRequestResponseLink().thenComposeAsync((v) -> {
			HashMap requestBodyMap = new HashMap();
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_SKIP, skip);
			requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_TOP, top);

			Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(
					ClientConstants.REQUEST_RESPONSE_GET_RULES_OPERATION,
					requestBodyMap,
					Util.adjustServerTimeout(this.underlyingFactory.getOperationTimeout()));
			CompletableFuture<Message> responseFuture = this.requestResponseLink.requestAysnc(requestMessage, this.underlyingFactory.getOperationTimeout());
			return responseFuture.thenComposeAsync((responseMessage) -> {
				CompletableFuture<Collection<RuleDescription>> returningFuture = new CompletableFuture<>();

				Collection<RuleDescription> rules = new ArrayList<RuleDescription>();
				int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
				if(statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE)
				{
					Map responseBodyMap = RequestResponseUtils.getResponseBody(responseMessage);
					ArrayList<Map> rulesMap = (ArrayList<Map>)responseBodyMap.get(ClientConstants.REQUEST_RESPONSE_RULES);
					for (Map ruleMap : rulesMap)
					{
						DescribedType ruleDescription = (DescribedType) ruleMap.getOrDefault("rule-description", null);
						rules.add(RequestResponseUtils.decodeRuleDescriptionMap(ruleDescription));
					}

					returningFuture.complete(rules);
				}
				else if(statusCode == ClientConstants.REQUEST_RESPONSE_NOCONTENT_STATUS_CODE)
				{
					returningFuture.complete(rules);
				}
				else
				{
					// error response
					returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
				}

				return returningFuture;
			}, MessagingFactory.INTERNAL_THREAD_POOL);
		}, MessagingFactory.INTERNAL_THREAD_POOL);
	}
}
