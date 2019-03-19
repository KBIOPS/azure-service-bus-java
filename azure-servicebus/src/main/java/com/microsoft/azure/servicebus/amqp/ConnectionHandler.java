/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.amqp;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.SslDomain;
import org.apache.qpid.proton.engine.SslPeerDetails;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.reactor.Handshaker;

import com.microsoft.azure.servicebus.primitives.ClientConstants;
import com.microsoft.azure.servicebus.primitives.StringUtil;

// ServiceBus <-> ProtonReactor interaction handles all
// amqp_connection/transport related events from reactor
public final class ConnectionHandler extends BaseHandler
{
	private static final SslDomain.VerifyMode VERIFY_MODE;
	private final IAmqpConnection messagingFactory;

	static
	{
		String verifyModePropValue = System.getProperty(ClientConstants.SSL_VERIFY_MODE_PROPERTY_NAME);
		if(ClientConstants.SSL_VERIFY_MODE_ANONYMOUS.equalsIgnoreCase(verifyModePropValue))
		{
			VERIFY_MODE = SslDomain.VerifyMode.ANONYMOUS_PEER;
		}
		else if(ClientConstants.SSL_VERIFY_MODE_CERTONLY.equalsIgnoreCase(verifyModePropValue))
		{
			VERIFY_MODE = SslDomain.VerifyMode.VERIFY_PEER;
		}
		else
		{
			VERIFY_MODE = SslDomain.VerifyMode.VERIFY_PEER_NAME;
		}
	}
	
	public ConnectionHandler(final IAmqpConnection messagingFactory)
	{
		add(new Handshaker());
		this.messagingFactory = messagingFactory;
	}
	
	public int getPort()
	{
		return ClientConstants.AMQPS_PORT;
	}
	
	@Override
	public void onConnectionInit(Event event)
	{
		final Connection connection = event.getConnection();
		final String hostName = event.getReactor().getConnectionAddress(connection);
		connection.setHostname(hostName);
		connection.setContainer(StringUtil.getShortRandomString());
		
		final Map<Symbol, Object> connectionProperties = new HashMap<Symbol, Object>();
        connectionProperties.put(AmqpConstants.PRODUCT, ClientConstants.PRODUCT_NAME);
        connectionProperties.put(AmqpConstants.VERSION, ClientConstants.CURRENT_JAVACLIENT_VERSION);
        connectionProperties.put(AmqpConstants.PLATFORM, ClientConstants.PLATFORM_INFO);
        connection.setProperties(connectionProperties);
        
		connection.open();
	}

	@Override
	public void onConnectionBound(Event event)
	{
		Transport transport = event.getTransport();
		
		Sasl sasl = transport.sasl();
		sasl.setMechanisms("ANONYMOUS");
		
		SslDomain domain = Proton.sslDomain();
		domain.init(SslDomain.Mode.CLIENT);
		
		if(VERIFY_MODE == SslDomain.VerifyMode.VERIFY_PEER_NAME)
		{
			try {
				// Default SSL context will have the root certificate from azure in truststore anyway
				SSLContext defaultContext = SSLContext.getDefault();
				StrictTLSContextSpi strictTlsContextSpi = new StrictTLSContextSpi(defaultContext);
				SSLContext strictTlsContext = new StrictTLSContext(strictTlsContextSpi, defaultContext.getProvider(), defaultContext.getProtocol());
				domain.setSslContext(strictTlsContext);
				domain.setPeerAuthentication(SslDomain.VerifyMode.VERIFY_PEER_NAME);
				SslPeerDetails peerDetails = Proton.sslPeerDetails(this.messagingFactory.getHostName(), this.getPort());
				transport.ssl(domain, peerDetails);
			} catch (NoSuchAlgorithmException e) {
				// Should never happen
//				this.messagingFactory.onConnectionError(new ErrorCondition(AmqpErrorCode.InternalError, e.getMessage()));
			}
		}
		else if (VERIFY_MODE == SslDomain.VerifyMode.VERIFY_PEER)
		{
			// Default SSL context will have the root certificate from azure in truststore anyway
			try {
				SSLContext defaultContext = SSLContext.getDefault();
				domain.setSslContext(defaultContext);
				domain.setPeerAuthentication(SslDomain.VerifyMode.VERIFY_PEER);
				transport.ssl(domain);
			} catch (NoSuchAlgorithmException e) {
				// Should never happen
//				this.messagingFactory.onConnectionError(new ErrorCondition(AmqpErrorCode.InternalError, e.getMessage()));
			}
			
		}
		else
		{
			domain.setPeerAuthentication(SslDomain.VerifyMode.ANONYMOUS_PEER);
			transport.ssl(domain);
		}
	}

	@Override
	public void onTransportError(Event event)
	{
		ErrorCondition condition = event.getTransport().getCondition();
		if (condition != null)
		{			
		}
		else
		{			
		}

		this.messagingFactory.onConnectionError(condition);
		Connection connection = event.getConnection();
		if(connection != null)
		{
		    connection.free();
		}
	}

	@Override
	public void onConnectionRemoteOpen(Event event)
	{		
		this.messagingFactory.onConnectionOpen();
	}

	@Override
	public void onConnectionRemoteClose(Event event)
	{
		final Connection connection = event.getConnection();
		final ErrorCondition error = connection.getRemoteCondition();
		
		boolean shouldFreeConnection = connection.getLocalState() == EndpointState.CLOSED;		
		this.messagingFactory.onConnectionError(error);
		if(shouldFreeConnection)
		{
		    connection.free();
		}
	}
	
	@Override
    public void onConnectionFinal(Event event) {
    }
	
	@Override
    public void onConnectionLocalClose(Event event) {
	    Connection connection = event.getConnection();
	    if(connection.getRemoteState() == EndpointState.CLOSED)
	    {
	        // Service closed it first. In some such cases transport is not unbound and causing a leak.
	        if(connection.getTransport() != null)
	        {
	            connection.getTransport().unbind();
	        }
	        
	        connection.free();
	    }
    }
}
