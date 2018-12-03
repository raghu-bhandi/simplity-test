/*
 * Copyright (c) 2018 simplity.org
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.simplity.gateway;

import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.value.BooleanValue;
import org.simplity.service.ServiceContext;
import org.simplity.tp.Service;

/**
 * gateway to be used when this application is simplity-based, and has the flexibility to be either bundled with this app/module, or deployed separately
 * @author simplity.org
 *
 */
public class LocalServiceGateway extends Gateway{

	/* (non-Javadoc)
	 * @see org.simplity.gateway.Gateway#getAssistant(java.lang.String, org.simplity.service.ServiceContext)
	 */
	@Override
	public ServiceAssistant getAssistant(String serviceName, ServiceContext ctx) {
		return new Assistant(serviceName);
	}

	/* (non-Javadoc)
	 * @see org.simplity.gateway.Gateway#getReady()
	 */
	@Override
	public void getReady() {
		//

	}

	/* (non-Javadoc)
	 * @see org.simplity.gateway.Gateway#shutdown()
	 */
	@Override
	public void shutdown() {
		//
	}

	class Assistant implements ServiceAssistant{
		private String serviceName;

		Assistant(String serviceName){
			this.serviceName = serviceName;
		}

		/* (non-Javadoc)
		 * @see org.simplity.gateway.ServiceAssistant#execute(org.simplity.service.ServiceContext, org.simplity.kernel.db.DbDriver, boolean)
		 */
		@Override
		public BooleanValue execute(ServiceContext ctx, DbDriver driver, boolean transactionIsDelegated) {
			Service service = ComponentManager.getService(this.serviceName);
			return service.executeAsAction(ctx, driver, transactionIsDelegated);
		}
	}

}
