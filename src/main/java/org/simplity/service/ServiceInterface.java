/*
 * Copyright (c) 2015 EXILANT Technologies Private Limited (www.exilant.com)
 * Copyright (c) 2016 simplity.org
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

package org.simplity.service;

import org.simplity.kernel.db.DbDriver;

/** * Defines a Service */
public interface ServiceInterface {
	/**
	 * execute this service based on the input made available in the context.
	 *
	 * @param ctx
	 *            any input data requirement of this service is assumed to be
	 *            already made available here.
	 * @param driver
	 *            db driver that is currently being used
	 * @param transactionIsDelegated
	 *            if this is true, this service should manage its own
	 *            transaction, in case some updates are performed. Used in very
	 *            special cases of batch jobs
	 * @return true if all OK. false if the service did not execute, and an
	 *         error message is added to the ctx
	 */
	public boolean execute(ServiceContext ctx, DbDriver driver, boolean transactionIsDelegated);
}
