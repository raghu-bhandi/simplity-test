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
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.simplity.tp;

import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

/**
 * get message in the message box
 *
 * @author simplity.org
 */
public class PickupFromMessageBox extends Action {
	/** field name to be picked up from the message box */
	@FieldMetaData(isRequired = true)
	String fieldName;

	@Override
	protected Value doAct(ServiceContext ctx) {
		Object val = ctx.getMessageFromBox();
		if (val == null) {
			return Value.VALUE_FALSE;
		}
		if (this.fieldName != null) {
			ctx.setTextValue(this.fieldName, val.toString());
		}
		return Value.VALUE_TRUE;
	}
}
