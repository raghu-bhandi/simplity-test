/*
 * Copyright (c) 2017 simplity.org
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

package org.simplity.service;

import java.io.IOException;

import org.simplity.sa.ServiceAgent;
import org.simplity.sa.ServiceRequest;
import org.simplity.sa.ServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * convenient class that can be used to run a service in the background
 *
 * @author simplity.org
 */
public class ServiceSubmitter implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ServiceSubmitter.class);

	private final ServiceRequest request;
	private final ServiceResponse response;

	/**
	 * * instantiate with required attributes
	 *
	 * @param req
	 * @param resp
	 */
	public ServiceSubmitter(ServiceRequest req, ServiceResponse resp) {
		this.request = req;
		this.response = resp;
	}

	@Override
	public void run() {
		ServiceAgent.getAgent().serve(this.request, this.response);
		if (this.response.payloadIsStream() == false) {
			return;
		}
		try {
			this.response.getPayloadStream().close();
		} catch (IOException e) {
			logger.error("Error while writing response from background service onto stream", e);
		}
	}
}
