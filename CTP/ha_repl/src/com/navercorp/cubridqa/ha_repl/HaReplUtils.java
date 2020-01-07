/**
 * Copyright (c) 2016, Search Solution Corporation. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 *   * Redistributions of source code must retain the above copyright notice, 
 *     this list of conditions and the following disclaimer.
 * 
 *   * Redistributions in binary form must reproduce the above copyright 
 *     notice, this list of conditions and the following disclaimer in 
 *     the documentation and/or other materials provided with the distribution.
 * 
 *   * Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products 
 *     derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. 
 */
package com.navercorp.cubridqa.ha_repl;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.navercorp.cubridqa.common.CommonUtils;
import com.navercorp.cubridqa.common.ConfigParameterConstants;
import com.navercorp.cubridqa.ha_repl.common.Constants;
import com.navercorp.cubridqa.common.Log;
import com.navercorp.cubridqa.shell.common.GeneralScriptInput;
import com.navercorp.cubridqa.shell.common.SSHConnect;

public class HaReplUtils {

	private static int MAX_TRY_WAIT_STATUS = 180;
	
	private static String getResultId(int taskId, String hitHost, String cat, String buildId) {
		return cat.toUpperCase().trim() + "_" + buildId + "_" + CommonUtils.dateToString(new java.util.Date(), Constants.FM_DATE_SNAPSHOT) + "_" + taskId + ((hitHost == null) ? "" : "_" + hitHost);
	}
	
	private static GeneralScriptInput getBackupScripts(String resultId, String testCase) {

		GeneralScriptInput script = new GeneralScriptInput("");
		script.addCommand("backup_dir_root=" + Constants.DIR_ERROR_BACKUP);
		script.addCommand("mkdir -p ${backup_dir_root}");
		script.addCommand("freadme=${backup_dir_root}/readme.txt ");
		script.addCommand("echo > ${freadme}");
		script.addCommand("echo 1.TEST CASE: " + testCase + " > $freadme");
		script.addCommand("echo 2.CUBRID VERSION: `cubrid_rel | grep CUBRID` >> $freadme");
		script.addCommand("echo 3.TEST DATE: `date` >> $freadme");
		script.addCommand("echo 4.ENVIRONMENT: >> $freadme");
		script.addCommand("set >> $freadme");
		script.addCommand("echo 5.PROCESSES >> $freadme");
		script.addCommand("ps -ef >> $freadme");
		script.addCommand("echo 6.CURRENT USER PROCESSES >> $freadme");
		script.addCommand("ps -u $USER -f >> $freadme");
		script.addCommand("echo 7.IPCS >> $freadme");
		script.addCommand("ipcs >> $freadme");
		script.addCommand("echo 8.DISK STATUS >> $freadme");
		script.addCommand("df >> $freadme");
		script.addCommand("echo 9.LOGGED >> $freadme");
		script.addCommand("who >> $freadme");
		script.addCommand("cd ${backup_dir_root}");
		script.addCommand("tar czvf " + resultId + ".tar.gz ../CUBRID/log readme.txt");
		return script;
	}
	
	private static void backupLogs (Context context, SSHConnect ssh, Log log) {
		log.println("------------ Backup logs for host : " + ssh.getHost ());
		String cat = "START_FAIL_" + context.getStartDBFailCount ();
		String hitHost = ssh.getHost().trim();
		String resultId = getResultId(context.getFeedback().getTaskId(), hitHost, cat, context.getBuildId());
		
		try {
			String result = ssh.execute(getBackupScripts(resultId, "STARTDB_FAIL"));
			log.println("Execute environement backup: " + cat);
			log.println(result);
		}
		catch (Exception e) {
			log.println("fail to check error: " + e.getMessage() + " in " + ssh.toString());
		}
	}

	public static boolean rebuildFinalDatabase(Context context, InstanceManager hostManager, Log log, String... params) {
		int maxTry = 5;
		int loop = 1;
		boolean success = false;
		while (maxTry-- > 0) {
			try {
				success = true;
				log.println("DATABASE IS DIRTY. REBUILD ... try " + loop);
				__rebuildDatabase(context, hostManager, log, params);
				break;
			} catch (Exception e) {
				log.println("Rebuild DB Error: " + e.getMessage());
				context.incStartDBFailCount ();
				success = false;
			}
			loop ++;
		}
		
		return success;
	}

	private static void __rebuildDatabase(Context context, InstanceManager hostManager, Log log, String... params) throws Exception {

		ArrayList<SSHConnect> allHosts = hostManager.getAllNodeList();
		
		boolean enableDebug = CommonUtils.convertBoolean(System.getenv(ConfigParameterConstants.CTP_DEBUG_ENABLE), false)
		                      || CommonUtils.convertBoolean(context.getProperty(ConfigParameterConstants.ENABLE_CTP_DEBUG, "false"));

		log.println("------------ CLEANUP processes and deletedb on all nodes; hard delete : " + context.shouldHardDeleteOnRebuildDB ());
		StringBuffer s = new StringBuffer();
		s.append(
				"pkill -u $USER cub;ps -u $USER | grep cub | awk '{print $1}' | grep -v PID | xargs -i  kill -9 {}; ipcs | grep $USER | awk '{print $2}' | xargs -i ipcrm -m {};");
		
		if (context.shouldHardDeleteOnRebuildDB ()) {
			s.append("rm -rf ${CUBRID}/databases/*").append(";");
			s.append("touch ${CUBRID}/databases/databases.txt").append(";");
			s.append("rm -rf ${CUBRID}/log/*").append(";");
		}
		else {
			// soft delete DB
			s.append ("cubrid deletedb " + hostManager.getTestDb()).append(";");
			s.append("cd ${CUBRID}/databases/").append(";");
			if (CommonUtils.isEmpty(hostManager.getTestDb()) == false) {
				s.append("rm -rf ").append(hostManager.getTestDb().trim() + "*").append(";");
			}
		}
		s.append("cd ~;");

		GeneralScriptInput script = new GeneralScriptInput(s.toString());
		for (SSHConnect ssh : allHosts) {
			if (context.getStartDBFailCount () > 0) {
				backupLogs (context, ssh, log);
			}
			ssh.execute(script);
			log.println("------------ CLEANUP DONE for host : " + ssh.getHost ());
		}

		s = new StringBuffer();
		s.append("cd ${CUBRID}/databases").append(";");
		s.append("mkdir ").append(hostManager.getTestDb()).append(";");
		s.append("cd ").append(hostManager.getTestDb()).append(";");

		if (params != null) {
			for (String p : params) {
				if (p != null && p.trim().equals("") == false) {
					s.append("echo " + p + " >> $CUBRID/conf/cubrid.conf; ");
				}
			}
		}

		if (CommonUtils.haveCharsetToCreateDB(context.getBuildId())) {
			// use different DBcharset to run test
			String dbCharset = context.getProperty(ConfigParameterConstants.CUBRID_DB_CHARSET, "").trim();
			if (dbCharset.equals("")) {
				dbCharset = "en_US";
			}
			s.append("cubrid createdb ").append(hostManager.getTestDb()).append(" --db-volume-size=50M --log-volume-size=50M " + dbCharset + ";");
		} else {
			s.append("cubrid createdb ").append(hostManager.getTestDb()).append(" --db-volume-size=50M --log-volume-size=50M;");
		}

		s.append("cubrid hb start ").append(";");
		s.append("cubrid broker start").append(";");
		s.append("cd ~;");

		script = new GeneralScriptInput(s.toString());

		SSHConnect master = hostManager.getHost("master");
		master.setEnableDebug (enableDebug);
		master.setTimeout (50 * 1000);
		log.println("------------ MASTER : CREATE DATABASE -----------------");

		String result = master.execute(script);
		log.println(result);
		System.out.println(result);
		if (result.indexOf("fail") != -1) {
			throw new Exception("fail to create on master.");
		}
		boolean succ = waitDatabaseReady(master, hostManager.getTestDb(), "(to-be-active|is active)", log, MAX_TRY_WAIT_STATUS, enableDebug);
		if (!succ)
			throw new Exception("timeout when wait 'to-be-active' or 'is active' in master");

		ArrayList<SSHConnect> slaveAndReplicaList = hostManager.getAllSlaveAndReplicaList();
		for (SSHConnect ssh : slaveAndReplicaList) {
			log.println("------------ SLAVE/REPLICA : CREATE DATABASE -----------------");
			ssh.setEnableDebug (enableDebug);
			ssh.setTimeout (50 * 1000);
			result = ssh.execute(script);
			log.println(result);
			System.out.println(result);
			if (result.indexOf("fail") != -1) {
				throw new Exception("fail to create on slave/replica.");
			}
			succ = waitDatabaseReady(ssh, hostManager.getTestDb(), "is standby", log, MAX_TRY_WAIT_STATUS, enableDebug);
			if (!succ)
				throw new Exception("timeout when wait standby in slave/replica");
		}
		log.println("------------ MASTER : WAIT ACTIVE -----------------");
		succ = waitDatabaseReady(master, hostManager.getTestDb(), "is active", log, MAX_TRY_WAIT_STATUS, enableDebug);
		if (!succ)
			throw new Exception("timout when wait active in master");

		CommonUtils.sleep(1);
		log.println("REBUILD DONE");
	}

	private static boolean waitDatabaseReady(SSHConnect ssh, String dbName, String expectedStatus, Log log, int maxTry, boolean enableDebug) throws Exception {
		GeneralScriptInput script = new GeneralScriptInput("cd $CUBRID");
		script.addCommand("cubrid changemode " + dbName);
		String result;
		String side = "[\\s\\S]*";
		log.println(" **** waitDatabaseReady for host:  " + ssh.getHost () + " expectet status : " + expectedStatus);
		while (maxTry-- > 0) {
			ssh.setTimeout (20 * 1000);
			result = ssh.execute(script);
			log.println(result);
			if (Pattern.matches(side + expectedStatus + side, result)) {
				return true;
			}
			if (enableDebug && maxTry % 5 == 0) {
				log.println(" **** waitDatabaseReady remaining wait : " + maxTry);
			}
			CommonUtils.sleep(1);
		}
			
		if (enableDebug) {
			log.println(" **** waitDatabaseReady FAILED  ***");
			log.println(" **** CUBRID logs START ***");
			display_cubrid_logs (ssh, dbName, log);
			log.println(" **** CUBRID logs END ***");
		}		
		return false;
	}

	public static ArrayList<String[]> extractTableToBeVerified(String input, String flag) {

		ArrayList<String[]> list = new ArrayList<String[]>();
		if (input == null)
			return list;

		Pattern pattern = Pattern.compile("'" + flag + "'\\s*'(.*?)'\\s*([0-9]*)");
		Matcher matcher = pattern.matcher(input);

		String[] item;

		while (matcher.find()) {
			item = new String[2];
			item[0] = matcher.group(1);
			item[1] = matcher.group(2);

			list.add(item);
		}
		return list;

	}
	
	private static void display_cubrid_logs (SSHConnect ssh, String dbName, Log log) throws Exception {
		GeneralScriptInput script = new GeneralScriptInput("cd $CUBRID/log");
		script.addCommand("tail -n 100 -v *_createdb.err");
		script.addCommand("tail -n 1000 -v *_master.err");
		script.addCommand("tail -n 1000 -v *_changemode.err");
		script.addCommand("tail -n 1000 -v server/" + dbName + "_*.err");
		String result;

		result = ssh.execute(script);
		log.println(result);
	}
}
