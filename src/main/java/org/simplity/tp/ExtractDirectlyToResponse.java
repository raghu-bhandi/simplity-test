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

import org.simplity.kernel.ApplicationError;
import org.simplity.kernel.comp.ComponentType;
import org.simplity.kernel.comp.FieldMetaData;
import org.simplity.kernel.db.DbAccessType;
import org.simplity.kernel.db.DbDriver;
import org.simplity.service.ServiceContext;

/**
 * Read a row/s from as output of a prepared statement/sql
 *
 * @author simplity.org
 */
public class ExtractDirectlyToResponse extends DbAction {

	/**
	 * fully qualified sql name
	 */
	@FieldMetaData(isRequired = true, isReferenceToComp = true, referredCompType = ComponentType.SQL)
	String sqlName;

	/**
	 * qualified name of the field this result is going to be assigned to. For
	 * example if this is orderDetails of an order for a customer, (and customer
	 * is the root object) then orders.orderDetails is the qualified field nme.
	 */
	String qualifiedFieldName;

	@Override
	protected int doDbAct(ServiceContext ctx, DbDriver driver) {
		throw new ApplicationError("ExtractDirectlyToResponse action is not yet implemented");
		// Sql sql = ComponentManager.getSql(this.sqlName);
		// JSONWriter writer = new JSONWriter();
		// writer.object().key(this.jsonName).array();
		// sql.sqlToJson(ctx, driver, this.useCompactFormat, writer);
		// writer.endArray().endObject();
		// ctx.setObject(this.jsonName, writer.toString());
		// return 1;
	}

	@Override
	public DbAccessType getDataAccessType() {
		return DbAccessType.READ_ONLY;
	}
}
