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
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.simplity.tp;

import org.simplity.gateway.Gateways;
import org.simplity.gateway.ServiceAssistant;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.kernel.value.BooleanValue;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

/**
 * execute an external application.
 *
 * @author simplity.org
 */
public class ExecuteExternalService extends Action {

	/**
	 * name of the external application
	 */
	@FieldMetaData(isRequired = true)
	String applictionName;

	/**
	 * service name. This service name must be defined under external services
	 * for the specified application
	 */
	@FieldMetaData(isRequired = true)
	String serviceName;

	/**
	 * set at boot time
	 */
	private boolean transactionIsDelegated;

	@Override
	protected Value delegate(ServiceContext ctx, DbDriver driver) {
		ServiceAssistant assistant = Gateways.getAssistant(this.applictionName, this.serviceName, ctx);
		BooleanValue result = assistant.execute(ctx, driver, this.transactionIsDelegated);
		return result;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.tp.Action#getReady(int)
	 */
	@Override
	public void getReady(int idx, Service service) {
		super.getReady(idx, service);
		if (service.dbAccessType == DbAccessType.SUB_SERVICE) {
			this.transactionIsDelegated = true;
		}
	}
}
