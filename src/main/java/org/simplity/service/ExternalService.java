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

package org.simplity.service;

import org.simplity.kernel.comp.Component;
import org.simplity.kernel.comp.ComponentManager;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.comp.ValidationContext;
import org.simplity.kernel.comp.ValidationMessage;
import org.simplity.kernel.comp.ValidationUtil;

/**
 * Represents an external service dependency for this app.
 *
 * @author simplity.org
 *
 */
public class ExternalService implements Component {

	/**
	 * name of service. unique for an applicationName
	 */
	@FieldMetaData(isRequired = true)
	String serviceName;
	/**
	 * external application that serves this service.
	 */
	@FieldMetaData(isRequired = true)
	String applicationName;

	/**
	 * data that this service expects as input
	 */
	OutputData requestData;
	/**
	 * Data that this service returns
	 */
	InputData responseData;

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.kernel.comp.Component#getReady()
	 */
	@Override
	public void getReady() {
		this.requestData.getReady();
		this.responseData.getReady();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.simplity.kernel.comp.Component#validate(org.simplity.kernel.comp.
	 * ValidationContext)
	 */
	@Override
	public void validate(ValidationContext vtx) {
		ValidationUtil.validateMeta(vtx, this);
		if (this.requestData == null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_INFO,
					"This service is not expecting any data.", "requestData"));
		} else {
			this.requestData.validate(vtx);
		}
		if (this.responseData == null) {
			vtx.message(new ValidationMessage(this, ValidationMessage.SEVERITY_INFO,
					"This service is not outputting any data.", "responseData"));
		} else {
			this.responseData.validate(vtx);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.kernel.comp.Component#getSimpleName()
	 */
	@Override
	public String getSimpleName() {
		return this.serviceName;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.kernel.comp.Component#getQualifiedName()
	 */
	@Override
	public String getQualifiedName() {
		return this.applicationName + ComponentManager.APP_SEP + this.serviceName;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.simplity.kernel.comp.Component#getComponentType()
	 */
	@Override
	public ComponentType getComponentType() {
		return ComponentType.EXTERN;
	}

	/**
	 *
	 * @return specification of expected data that is received as
	 *         payload/response from this service
	 */
	public InputData getResponseSpec() {
		return this.responseData;
	}

	/**
	 *
	 * @return specification of expected data to be sent as payload/request for
	 *         this service
	 */
	public OutputData getRequestSpec() {
		return this.requestData;
	}
}
