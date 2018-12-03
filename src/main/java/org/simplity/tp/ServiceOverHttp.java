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

package org.simplity.tp;

import org.simplity.gateway.Gateways;
import org.simplity.gateway.HttpGateway;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

/**
 * an external service request using http protocol
 *
 * @author simplity.org
 *
 */
public class ServiceOverHttp extends Action {

	/**
	 * unique name of external application that is contacted for this service
	 */
	@FieldMetaData(isRequired = true)
	String applicationName;
	/**
	 * external service name that is to be invoked
	 */
	@FieldMetaData(isRequired = true)
	String serviceName;

	/**
	 * url to be appended to the base path set at the gateway. null if path set
	 * at gateway is to be used. you my use the REST convention of a pair of
	 * flower brackets to embed field names. for example
	 * /customer/{custId}/view. if custId=abcd then the url would be
	 * /customer/abcd/view
	 *
	 * if it is a.do?customer={custId} would result in a.do?customer=abcd
	 */
	String urlString;
	/**
	 * GET, POST etc.. null if method set at the gateway is to be used
	 */
	String httpMethod;

	/**
	 * headers to be set.
	 */
	String[] headerNamesToSend;
	/**
	 * in case the header names are different from the field names in the
	 * service context. Default to headerNames;
	 */
	@FieldMetaData(leaderField = "headerNamesToSend")
	String[] headerFieldSources;

	/**
	 * header names to be received after the call.
	 */
	String[] headerNamesToReceive;

	/**
	 * in case the received headers to be extracted to service context with
	 * different field names. Defaults to headerNamesToReceive
	 */
	@FieldMetaData(leaderField = "headerNamesToReceive")
	String[] headerFieldDestinations;

	/**
	 * if the http status code returned by the external server needs to be set a
	 * field in service context
	 */
	String setStatusCodeTo;

	/**
	 * content type. Specify only if you have to over-ride the default content
	 * type specified on the gateway
	 */
	String contentType;
	/**
	 * in case url has variables in it, cache its parts for efficiency at run
	 * time. If url has n fields, then urlParts will have n+1 elements. In case
	 * the urlString is just one field name, then this will have empty strings.
	 */
	private String[] urlParts;
	/**
	 * in case url has field names in it, the fields are cached to this
	 */
	private String[] urlFieldNames;

	@Override
	protected Value doAct(ServiceContext ctx) {
		HttpGateway.Assistant assistant = (HttpGateway.Assistant) Gateways.getAssistant(this.applicationName,
				this.serviceName, ctx);
		this.putThem(assistant, ctx);
		boolean allOk = assistant.sendAndReceive(ctx);
		if (allOk) {
			this.getThem(assistant, ctx);
			return Value.VALUE_TRUE;
		}
		return Value.VALUE_FALSE;
	}

	private void getThem(HttpGateway.Assistant assistant, ServiceContext ctx) {
		/*
		 * payload is already extracted by the assistant, based on spec in the
		 * external service
		 */
		if (this.headerNamesToReceive != null) {
			String[] values = assistant.getHeaders(this.headerNamesToReceive);
			int i = 0;
			for (String val : values) {
				if (val != null && val.isEmpty() == false) {
					ctx.setTextValue(this.headerFieldDestinations[i], val);
				}
				i++;
			}
		}
	}

	private void putThem(HttpGateway.Assistant assistant, ServiceContext ctx) {
		if (this.httpMethod != null) {
			assistant.setMethod(this.httpMethod);
		}
		if (this.urlString != null) {
			String path = null;
			if (this.urlParts == null) {
				path = this.urlString;
			} else {
				StringBuilder sbf = new StringBuilder(this.urlParts[0]);
				int i = 1; // urlParts starts with 1
				for (String field : this.urlFieldNames) {
					String val = ctx.getTextValue(field);
					if (val != null) {
						sbf.append(val);
					}
					sbf.append(this.urlParts[i]);
					i++;
				}
				path = sbf.toString();
			}
			assistant.setPath(path);
		}

		if (this.headerFieldSources != null) {
			Value[] values = new Value[this.headerFieldSources.length];
			for (int i = 0; i < values.length; i++) {
				values[i] = ctx.getValue(this.headerFieldSources[i]);
			}
			assistant.setHeaders(this.headerNamesToSend, values);
		}
	}

	@Override
	public void getReady(int idx, Service service) {
		super.getReady(idx, service);
		if (this.urlString != null) {
			this.setUrlParts();
		}

		if (this.headerNamesToSend != null) {
			if (this.headerFieldSources == null) {
				this.headerFieldSources = this.headerNamesToSend;
			}
		}

		if (this.headerNamesToReceive != null) {
			if (this.headerFieldDestinations == null) {
				this.headerFieldDestinations = this.headerNamesToReceive;
			}
		}
	}

	/**
	 * use an example a/{a}/b.do?c={d}.
	 *
	 */
	private void setUrlParts() {
		int idx = this.urlString.indexOf('{');
		if (idx == -1) {
			/*
			 * no fields at all
			 */
			return;
		}
		String[] parts = this.urlString.split("{");
		/*
		 * there will be at least 2 parts. if there are n fields, there will be
		 * n+1 parts
		 */
		String[] fields = new String[parts.length - 1];
		/*
		 * each part (except the 0th) starts with the name of the field,
		 * delimited b y'}'
		 */
		for (int i = 1; i < parts.length; i++) {
			String[] pair = parts[i].split("}");
			if (pair.length != 2) {
				/*
				 * error. assume that design time validations have taken care of
				 */
				return;
			}
			fields[i - 1] = pair[0];
			parts[i] = pair[1];
		}
		this.urlParts = parts;
		this.urlFieldNames = fields;
	}

	@Override
	public void validateSpecific(ValidationContext vtx, Service service) {
		super.validateSpecific(vtx, service);

		if (this.headerNamesToSend != null && this.headerFieldSources != null
				&& this.headerFieldSources.length != this.headerNamesToSend.length) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"headerFieldSources defaults to headerNamesToSend, but if you specify it, number of field names should correspond to header names ",
					"headerFieldSources"));
		}

		if (this.headerNamesToReceive != null && this.headerFieldDestinations != null
				&& this.headerFieldDestinations.length != this.headerNamesToReceive.length) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_ERROR,
					"headerFieldDestinations defaults to headerNamesToReceive, but if you specify it, number of field names should correspond to header names ",
					"headerFieldDestinations"));
		}
	}
}
