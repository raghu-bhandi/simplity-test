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

/** @author simplity.org */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.simplity.kernel.data.DataSheet;
import org.simplity.kernel.mail.Mail;
import org.simplity.kernel.mail.MailAttachment;
import org.simplity.kernel.mail.MailConnector;
import org.simplity.kernel.mail.MailContent;
import org.simplity.kernel.value.Value;
import org.simplity.service.ServiceContext;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * send a mail
 * @author simplity.org
 *
 */
public class SendMail extends Action {

	String fromId;
	String toIds;
	String ccIds;
	String bccIds;
	String subject;
	String attachmentSheetName;
	String inlineAttachmentSheetName;

	MailContent content;

	/**
	 * receives data from service, creates mail object, sets fromId, toIds,
	 * ccIds, bccIds, subject set
	 * mail content (either text or template)
	 */
	@Override
	protected Value doAct(ServiceContext ctx) {

		Mail mail = new Mail();
		mail.fromId = this.fromId;
		mail.toIds = this.toIds;
		mail.ccIds = this.ccIds;
		mail.bccIds = this.bccIds;
		mail.subject = this.subject;

		Map<String, Object> data = new HashMap<String, Object>();

		DataSheet attachmentDataSheet = ctx.getDataSheet(this.attachmentSheetName);
		if (attachmentDataSheet != null) {
			String[][] rawAttachmentData = attachmentDataSheet.getRawData();
			mail.attachment = new MailAttachment[attachmentDataSheet.length()];

			for (int i = 0; i < attachmentDataSheet.length(); i++) {
				mail.attachment[i] = new MailAttachment(rawAttachmentData[i + 1][0],
						rawAttachmentData[i + 1][1].replace("\\", "/"));
			}
		}

		DataSheet inlineAttachmentDataSheet = ctx.getDataSheet(this.inlineAttachmentSheetName);
		if (inlineAttachmentDataSheet != null) {
			String[][] rawInlineAttachmentData = inlineAttachmentDataSheet.getRawData();
			mail.inlineAttachment = new MailAttachment[inlineAttachmentDataSheet.length()];

			for (int i = 0; i < inlineAttachmentDataSheet.length(); i++) {
				mail.inlineAttachment[i] = new MailAttachment(rawInlineAttachmentData[i + 1][0],
						rawInlineAttachmentData[i + 1][1].replace("\\", "/"));
			}
		}

		if (this.content.templatePath != null) {
			Configuration templateConfiguration = new Configuration();

			try {

				templateConfiguration.setDirectoryForTemplateLoading(new File(this.content.templatePath));
				Template template = templateConfiguration.getTemplate(this.content.template);

				for (String sheetName : this.content.inputSheetNames) {
					DataSheet dataSheet = ctx.getDataSheet(sheetName);

					String[] columnNames = dataSheet.getColumnNames();
					String[][] rawData = dataSheet.getRawData();

					if (dataSheet.length() == 1) {
						for (int i = 0; i < dataSheet.width(); i++) {
							data.put(columnNames[i], rawData[1][i]);
						}
					} else {
						for (int i = 0; i < dataSheet.width(); i++) {
							List<String> rowValues = new ArrayList<String>();
							for (int j = 1; j <= dataSheet.length(); j++) {
								rowValues.add(rawData[j][i]);
							}
							data.put(columnNames[i], rowValues);
						}
					}
				}

				StringWriter stringWriter = new StringWriter();
				template.process(data, stringWriter);

				mail.content = stringWriter.toString();
				stringWriter.flush();
				stringWriter.close();

				ctx.setObject("mail", new ByteArrayInputStream(SendMail.serialize(mail)));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (TemplateException e) {
				e.printStackTrace();
			}
		} else {
			try {
				mail.content = this.content.text;
				ctx.setObject("mail", new ByteArrayInputStream(SendMail.serialize(mail)));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}

		new MailConnector().sendEmail(mail);

		return Value.newBooleanValue(true);
	}

	private static byte[] serialize(Object obj) throws IOException {

		ByteArrayOutputStream b = new ByteArrayOutputStream();

		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);

		return b.toByteArray();
	}
}
