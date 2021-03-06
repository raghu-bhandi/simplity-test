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

package org.simplity.adapter.source;

import java.util.Date;

import org.simplity.adapter.DataAdapterExtension;
import org.simplity.json.JSONArray;
import org.simplity.kernel.util.DateUtil;
import org.simplity.kernel.value.DateValue;
import org.simplity.kernel.value.Value;
import org.simplity.kernel.value.ValueType;
import org.simplity.service.ServiceContext;

/**
 * wrapper on service context to make it a data source
 *
 * @author simplity.org
 *
 */
public class ContextDataSource implements DataSource {
	private ServiceContext ctx;

	/**
	 * use data contained in ServiceContext as data source
	 *
	 * @param ctx
	 */
	public ContextDataSource(ServiceContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public String getPrimitiveValue(String fieldName) {
		Value value = this.ctx.getValue(fieldName);
		if (Value.isNull(value)) {
			return null;
		}
		return value.toString();
	}

	@Override
	public DataSource getChildSource(String fieldName) {
		return this.ctx.getDataSource(fieldName);
	}

	@Override
	public DataListSource getChildListSource(String fieldName) {
		return this.ctx.getListSource(fieldName);
	}

	@Override
	public Object getObjectValue(String fieldName) {
		Object obj = this.ctx.getObject(fieldName);
		if (obj == null) {
			return null;
		}
		if (obj instanceof JSONArray) {
			return ((JSONArray) obj).opt(0);
		}
		/*
		 * try app specific object
		 */
		DataAdapterExtension extension = ServiceContext.getDataAdapterExtension();
		if (extension != null) {
			return extension.getObjectValue(obj, fieldName, this.ctx);
		}
		return obj;
	}

	@Override
	public Date getDateValue(String fieldName) {
		Value value = this.ctx.getValue(fieldName);
		if (Value.isNull(value)) {
			return null;
		}
		ValueType vt = value.getValueType();
		if (vt == ValueType.DATE) {
			return new Date(((DateValue) value).getDate());
		}
		if (vt == ValueType.TEXT) {
			return DateUtil.parseDateWithOptionalTime(value.toString());
		}
		return null;
	}
}
