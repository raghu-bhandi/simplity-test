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
package org.simplity.adapter.target;

import java.util.Date;

import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

/**
 * Service Context as target of data from an adapter
 *
 * @author simplity.org
 *
 */
public class ContextDataTarget implements DataTarget {
	private ServiceContext ctx;

	/**
	 * use ServiceContext as target
	 *
	 * @param ctx
	 */
	public ContextDataTarget(ServiceContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public void setPrimitiveValue(String fieldName, String fieldValue) {
		this.ctx.setValue(fieldName, Value.parseValue(fieldValue));
	}

	@Override
	public DataTarget getChildTarget(String fieldName, String childClassName) {
		return this.ctx.getDataTarget(fieldName, childClassName);
	}

	@Override
	public DataListTarget getChildListTarget(String fieldName, String childClassName) {
		return this.ctx.getListTarget(fieldName, childClassName);
	}

	@Override
	public void setObjectValue(String fieldName, Object fieldValue) {
		this.ctx.setObject(fieldName, fieldValue);
	}

	@Override
	public void setDateValue(String fieldName, Date fieldValue) {
		this.ctx.setDateValue(fieldName, fieldValue);
	}
}
