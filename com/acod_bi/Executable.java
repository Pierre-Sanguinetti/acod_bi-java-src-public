package com.acod_bi;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

public class Executable {
	class ExecParam{
		String sCode;
		String sDesc;
		boolean bRequired;
		String sName; // Nom de la valeur du parametre
		boolean bFound;
		Vector<String> values;
	}
	protected Map<String, ExecParam> paramMap = new HashMap<>();
	protected List<ExecParam> paramList = new LinkedList<ExecParam>();
	
	public ExecParam addParam(String sCode, String sDesc, boolean bRequired, String sName) {
		if(paramMap.get(sCode) != null)
		{
            throw new RuntimeException("Exec param " + sCode + " allready exists");
		}
		ExecParam newExecParam = new ExecParam();
		newExecParam.sCode = sCode;
		newExecParam.sDesc = sDesc;
		newExecParam.bRequired = bRequired;
		newExecParam.sName = sName; 
		paramMap.put(sCode, newExecParam);
		paramList.add(newExecParam);
		return newExecParam;
	}
	
	public boolean hasParam(String sCode)
	{
		ExecParam param = paramMap.get(sCode);
		if(param == null)
            throw new RuntimeException("Invalid param -" + sCode);
		return param.bFound;
	}
	
	public String[] getParamValues(String sCode)
	{
		ExecParam param = paramMap.get(sCode);
		
		if(param == null)
            throw new RuntimeException("Invalid param -" + sCode);
		
		String[] sValues = new String[param.values.size()];
		int iIndex = 0;
		for(String sValue : param.values)
		{
			sValues[iIndex] = sValue; 
			++iIndex;
		}
		return sValues;
	}
	
	public String getParamValue(String sCode, int iValueIndex, String sDefaultValue)
	{
		String sValue;
		ExecParam param = paramMap.get(sCode);
		if(param == null)
            throw new RuntimeException("Invalid param -" + sCode);
		if(iValueIndex >= param.values.size())
			sValue = sDefaultValue;
		else
			sValue = param.values.get(iValueIndex);
		return sValue;
	}
	
	public String getParamValue(String sCode, String sDefaultValue)
	{
		return getParamValue(sCode, 0, sDefaultValue);
	}
	
	public String getParamValue(String sCode, int iValueIndex)
	{
		return getParamValue(sCode, iValueIndex, null);
	}
	
	public String getParamValue(String sCode)
	{
		return getParamValue(sCode, 0, null);
	}
	
	void parseArgs(String[] args)
	{
		setParams();
    	// reset paramMap
		for (Entry<String, ExecParam> entry: paramMap.entrySet()) {
    		ExecParam param = entry.getValue();
    		param.bFound = false;
    		param.values = new Vector<String>();
    	}
		// read args
		ExecParam currentParam = null;
    	for (String sArg : args) {
    		if(sArg.startsWith("-"))
    		{
    			String sCurrentCode = sArg.substring(1); 
    			currentParam = paramMap.get(sCurrentCode);
    			if(currentParam == null)
                	throw new RuntimeException("Invalid param " + sArg + " Try : java " + this.getClass().getCanonicalName() + " -h");
    			currentParam.bFound = true;
    		}
    		else 
    		{
    			if(currentParam == null)
    	            throw new RuntimeException("missing param name starting with -" + " Try : java " + this.getClass().getCanonicalName() + " -h");
    			currentParam.values.add(sArg);
    		}
    	}
    	// check mandatory parameters
		if(!(hasParam("h") || ((args.length==0)&&helpWitgNoArg())))
    	{
			for (Entry<String, ExecParam> entry: paramMap.entrySet()) {
	    		ExecParam param = entry.getValue();
	    		if((!param.bFound)&&(param.bRequired))
		            throw new RuntimeException("missing mandatory param -" + entry.getKey() + " (" + param.sDesc + ")" + " Try : java " + this.getClass().getCanonicalName() + " -h");
	    	}
    	}
	    
		if(hasParam("h") || ((args.length==0)&&helpWitgNoArg()))
		{
/*
usage: java com.acod_bi.exec_rs.ExecRS [-options]
 -c,--conn <database url>         Database url
 -h,--help                        Print this message
 -log,--log <log full path>       Full log file path
 -p,--denpassw <password>         Database password
 -quiet,--quiet                   Be quiet
 -s,--script <script full path>   Full SQL file path (with request set)
 -showsql,--showsql               Show queries
 -t,--timing                      show ellapsed time
 -u,--user <user>                 Database user
 -verbose,--verbose               Be verbose
*/
			System.out.println("usage: java " + this.getClass().getCanonicalName() + " [-options]");
			for (ExecParam param : paramList) {
	    		String sLine = " -" + param.sCode;
	    		if(param.sName != null)
	    			sLine += " <" + param.sName + "> ";
	    		while(sLine.length()<40) sLine+=' ';
	    		if(param.sDesc != null)
	    			sLine += param.sDesc;
	    		if(param.bRequired)
	    			sLine += " (mandatory)";
				System.out.println(sLine);
	    	}
			System.exit(0);
		}
		
		bVerbose = hasParam("verbose");
		bQuiet = hasParam("quiet");
		sLogFile = getParamValue("log");
	}
	
	// Overload with specific action name
	protected String getActionName() {
		return getClass().getName();
	}
	
	// Overload with specific action
	protected void doAction() {}
	
	// Overload with specific options
	protected void setParams() {
		addParam("h", "Print this message", false, null);
		addParam("verbose", "Be verbose", false, null);
		addParam("quiet", "Be quiet", false, null);
		addParam("log", "Full log file path", false, "log full path");
	}
	
	protected boolean helpWitgNoArg() {
		return true;
	}
	
	// Overload with specific parameters 
	protected void setVariablesFromParameters() {
		// String sArg1 = commandLine.getArgs()[0];
		// sDestDir = commandLine.getOptionValue("destdir", ".");

	}
	
	// Overload with specific parameters and options 
	protected void showParameters() {
		//if(!bQuiet) println("<verbose parameter name> : " + <verbose parameter string value>);
	    if(bVerbose)
	    {
	        println("Verbose");
	        if(bQuiet) 
	        	println("Quiet");
	        //println("<quiet parameter name> : " + <quiet parameter string value>);
	    }
        if(!bQuiet && sLogFile != null) 
        	println("Log File \"" + sLogFile + "\"");
	}
	
	public static void exitWithError() {
		if(System.getProperty("os.name").startsWith("Windows"))
            System.exit(1);
		else
            System.exit(-1);
	}
	
	public void exitWithParametersError(String errorMsg) {
        printlnErr(errorMsg);
        printlnErr("Help : " + getExecString() + " -h");
        exitWithError();
	}
	
	public void println(String sText, boolean bShow, boolean bLog)
	{
        if(bShow) 
        	System.out.println(sText);
        if(bLog&&(outLog != null))
        	outLog.println(sText);
	}
	public void println(String sText)
	{
		println(sText, true, true);
	}
	
	public void print(String sText, boolean bShow, boolean bLog)
	{
        if(bShow) 
        	System.out.print(sText);
        if(bLog&&(outLog != null))
        	outLog.print(sText);
	}
	public void print(String sText)
	{
		print(sText, true, true);
	}
	
	public void printlnErr(String sText)
	{
        System.err.println(sText);
        if(outLog != null)
        	outLog.println(sText);
	}
	
	public void printErr(String sText)
	{
        System.err.print(sText);
        if(outLog != null)
        	outLog.print(sText);
	}
	
	protected boolean bVerbose;
	protected boolean bQuiet;
	
	// Une amelioration consisterait a sortir dans des classes specifiques println et logs  
	protected String sLogFile = null;
	protected FileWriter fwLog = null;
	protected BufferedWriter bwLog = null;
	protected PrintWriter outLog = null;

	protected String getExecString() {
		String execString;
		execString = "java " + getClass().getCanonicalName() + " [-options]";
		return execString;
	}
	
	public void doMain(String[] args) {
		try {
			Locale.setDefault(Locale.US);
			
			parseArgs(args);
			
			try {
				if(sLogFile != null)
				{
					    fwLog = new FileWriter(sLogFile, true);
					    bwLog = new BufferedWriter(fwLog);
					    outLog = new PrintWriter(bwLog);
				}
				
			    setVariablesFromParameters();
			    
        		Instant beginAction, endAction;
        		beginAction = Instant.now();
			    if(!bQuiet) {
				    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").withZone(ZoneId.systemDefault());;  
			    	//-----LocalDateTime now = LocalDateTime.now();  
			    	//-----println(dtf.format(now));  
				    println(dtf.format(beginAction));  
			    	println("Begin " + getActionName() + " ... ");
			    }
		        
		        showParameters();
		        
		        doAction();
		        
		        endAction = Instant.now();
		        if(!bQuiet) {
		        	println("End " + getActionName() + " ... ");
				    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").withZone(ZoneId.systemDefault());  
				    //-----LocalDateTime now = LocalDateTime.now();  
				    //-----println(dtf.format(now));  
				    println(dtf.format(endAction));
        			print("Ellapsed:");
        			println(Duration.between(beginAction, endAction).toString());
		        }
			} 
			catch (IOException e) 
			{
	            throw new RuntimeException("Error writing log file", e);
			}
			catch (Exception e) 
			{
		        if(outLog != null)
			    	e.printStackTrace(outLog);
	            throw e;
			}
			finally {
			    try {
			        if(outLog != null)
			            outLog.close();
			    } finally {
				    try {
				        if(bwLog != null)
				            bwLog.close();
				    } catch (IOException e) {
			            throw new RuntimeException("Error closing log file", e);
				    } finally {
					    try {
					        if(fwLog != null)
					            fwLog.close();
					    } catch (IOException e) {
				            throw new RuntimeException("Error closing log file", e);
					    }
				    }
			    }
			}
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	exitWithError();
	    }
	}

	// Overload creating an object of the new Class
	public static void main(String[] args) {
		Executable executable = new Executable();
		executable.doMain(args);
	}

}
