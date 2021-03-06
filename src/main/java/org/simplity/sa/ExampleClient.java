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

package org.simplity.sa;

import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.simplity.http.HttpClient;
import org.simplity.kernel.value.Value;

/**
 * an example class that can be used as a guide to write your http controller
 * Must refer to <code>HttpClient</code> to understand how control flows from
 * Web container to the controller etc..
 *
 * @author simplity.org
 *
 */
public class ExampleClient extends HttpClient {

	private static final long serialVersionUID = 1L;


	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.http.HttpClient#postProcess(javax.servlet.http.
	 * HttpServletRequest, javax.servlet.http.HttpServletResponse,
	 * org.simplity.sa.ServiceRequest, org.simplity.sa.ServiceResponse)
	 */
	@Override
	protected void postProcess(HttpServletRequest httpRequest, HttpServletResponse httpResponse,
			ServiceRequest serviceRequest, ServiceResponse serviceResponse) {
		// anything from response to be set to session?, logging?

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.http.HttpClient#appSpecificInit(javax.servlet.
	 * ServletContext)
	 */
	@Override
	protected void appSpecificInit(ServletContext ctx) {
		/*
		 * should this.useStreamingPayload be changed based on some setting?
		 * anything to be cached into this singleton that is repeatedly called
		 * for every request?
		 */
	}

	/* (non-Javadoc)
	 * @see org.simplity.http.HttpClient#prepareRequestAndResponse(java.lang.String, javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse, org.simplity.sa.ServiceRequest, org.simplity.sa.ServiceResponse, java.util.Map)
	 */
	@Override
	protected boolean prepareRequestAndResponse(String serviceName, HttpServletRequest req, HttpServletResponse resp,
			ServiceRequest request, ServiceResponse response, Map<String, Object> fields) {
		boolean allOk = true;
		/*
		 * we may want to process fields collection, and change names??.
		 * fields are to be passed on to request
		 */
		request.setFields(fields);

		/*
		 * app user. We add a dummy user
		 *
		 */
		request.setUser(new AppUser(Value.newTextValue("420")));

		/*
		 * client context ?
		 */
		request.setClientContext(null);

		/*
		 * we may chose to return false in case of any error
		 */
		return allOk;
	}

}
