/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.amqp;

import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.reactor.Reactor;

import com.microsoft.azure.servicebus.primitives.ClientConstants;

public class ReactorHandler extends BaseHandler
{

	@Override
	public void onReactorInit(Event e)
	{		

		final Reactor reactor = e.getReactor();
		reactor.setTimeout(ClientConstants.REACTOR_IO_POLL_TIMEOUT);
	}

	@Override 
	public void onReactorFinal(Event e)
	{		
	}
}
