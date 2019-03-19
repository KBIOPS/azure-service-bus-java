/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.amqp;

import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Session;

public class SessionHandler extends BaseHandler
{

	private final String name;

	public SessionHandler(final String name)
	{
		this.name = name;
	}

	@Override
	public void onSessionRemoteOpen(Event e) 
	{		

		Session session = e.getSession();
		if (session != null && session.getLocalState() == EndpointState.UNINITIALIZED)
		{
			session.open();
		}
	}


	@Override 
	public void onSessionLocalClose(Event e)
	{		
	}

	@Override
	public void onSessionRemoteClose(Event e)
	{		

		Session session = e.getSession();
		if (session != null && session.getLocalState() != EndpointState.CLOSED)
		{
			session.close();
		}
	}

	@Override
	public void onSessionFinal(Event e)
	{ 
	}
}
