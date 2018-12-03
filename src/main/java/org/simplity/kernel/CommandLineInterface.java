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

package org.simplity.kernel;

import org.simplity.json.JSONWriter;
import org.simplity.kernel.value.Value;
import org.simplity.sa.AppUser;
import org.simplity.sa.PayloadType;
import org.simplity.sa.ServiceAgent;
import org.simplity.sa.ServiceRequest;
import org.simplity.sa.ServiceResponse;

/**
 * @author simplity.org
 *
 */
public class CommandLineInterface {
	/**
	 * command line utility to run any service. Since this is command line, we
	 * assume that security is
	 * already taken care of. (If user could do delete *.* what am I trying to
	 * stop him/her from
	 * doing??) We pick-up logged-in user name. Note that java command line has
	 * an option to specify
	 * the login-user. One can use this to run the application as that user
	 *
	 * @param args
	 *            comFolderName serviceName param1=value1 param2=value2 .....
	 */
	public static void main(String[] args) {
			doRun(args);
	}

	private static void doRun(String[] args) {
		int nbr = args.length;
		if (nbr < 2) {
			printUsage();
			return;
		}

		String compPath = args[0];
		try {
			boolean ok = Application.bootStrap(compPath);
			if(!ok) {
				System.err.println("Application failed to bootstrap with root=" + compPath);
				return;
			}
		} catch (Exception e) {
			System.err.println("error while bootstrapping with compFolder=" + compPath);
			e.printStackTrace(System.err);
			return;
		}

		String serviceName = args[1];
		String user = System.getProperty("user.name");

		String json = null;
		if (nbr > 2) {
			JSONWriter w = new JSONWriter();
			w.object();
			for (int i = 2; i < nbr; i++) {
				String[] parms = args[i].split("=");
				if (parms.length != 2) {
					printUsage();
					System.exit(-3);
				}
				w.key(parms[0]).value(parms[1]);
			}
			w.endObject();
			json = w.toString();
		} else {
			json = "{}";
		}

		System.out.println("path:" + compPath);
		System.out.println("userId:" + user);
		System.out.println("service:" + serviceName);
		System.out.println("request:" + json);

		AppUser appuser = new AppUser(Value.newTextValue(user));
		ServiceRequest req = new ServiceRequest(serviceName, PayloadType.JSON_OBJECT, json);
		req.setUser(appuser);
		ServiceResponse resp = new ServiceResponse(PayloadType.JSON_OBJECT);
		ServiceAgent.getAgent().serve(req, resp);

		System.out.println("response :" + resp.getPayloadText());
	}

	private static void printUsage() {
		System.out.println(
				"Usage : java  org.simplity.kernel.Applicaiton componentFolderPath serviceName inputParam1=vaue1 ...");
		System.out.println(
				"example : java  org.simplity.kernel.Applicaiton /user/data/ serviceName inputParam1=vaue1 ...");
	}

}
