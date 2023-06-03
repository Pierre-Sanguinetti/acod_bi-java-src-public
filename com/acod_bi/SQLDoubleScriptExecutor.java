package com.acod_bi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * SQLDoubleScriptExecutor execute a main script calling sub scripts with Denodo and RDBMS connexion    
 */
public class SQLDoubleScriptExecutor extends SQLScriptExecutor {
	public SQLDoubleScriptExecutor(Executable executable)
	{
	    super(executable, null);
		sqlDoubleConnection = new SQLDoubleConnection();
	}
	
	protected SQLDoubleConnection sqlDoubleConnection;
	
	String sSrcDir;
	String sMainScript;
	boolean bShowSQL;
	
    public void executableSetParams() {
        executable.addParam("showsql", "Show queries", false, null);
        
        executable.addParam("sd", "Source directory with SQL files to execute", false, "source directory");
        executable.addParam("m", "Main script calling others SQL files to execute", true, "main script");
        
        sqlDoubleConnection.executableSetParams(executable);
    }
    
    public void executableSetVariablesFromParameters() {
        bShowSQL = executable.hasParam("showsql");
        
        sSrcDir = executable.getParamValue("sd", ".");
        File srcFile = new File(sSrcDir);
        if(!srcFile.isDirectory())
        {
            executable.printlnErr("Invalid source directory : \"" + sSrcDir + "\"");
            Executable.exitWithError();
        }

        sMainScript = executable.getParamValue("m");
        File mainScript = new File(sSrcDir + File.separator + sMainScript);
        if(!mainScript.isFile())
        {
            executable.printlnErr("Invalid main script : \"" + sSrcDir + File.separator + sMainScript + "\"");
            Executable.exitWithError();
        }

        sqlDoubleConnection.executableSetVariablesFromParameters(executable);
    }
    
    public void executableShowParameters() {
        if(!executable.bQuiet) {
            executable.println("Source directory : \"" + sSrcDir + "\"");
            executable.println("Main script : \"" + sMainScript + "\"");
        }
        sqlDoubleConnection.executableShowParameters(executable);
        if(executable.bVerbose)
        {
            if(bShowSQL) executable.println("Show SQL");
        }
    }
    
	protected SQLScriptExecutor buildSQLScriptExecutor(Connection connection)
	{
        SQLScriptExecutor newScriptExecutor = new SQLScriptExecutor(executable, connection);
        newScriptExecutor.bQuiet = executable.bQuiet;
        newScriptExecutor.bShowSQL = this.bShowSQL;
		return newScriptExecutor;
	}
	
	public void executeScript() throws SQLException, IOException {
		sqlDoubleConnection.loadDriver(executable);
		
    	// Create Connection to database
        try(
        		Connection denConn = sqlDoubleConnection.getDenConnection(executable); 
        		Connection dtmConn = sqlDoubleConnection.getDtmConnection(executable); 
        )
        {
            SQLScriptExecutor denScriptExecutor = buildSQLScriptExecutor(denConn);
            denScriptExecutor.setExecMessage("Executing Denodo script ");
            denScriptExecutor.setSqlExec(new DenSQLExec(executable, denConn));
            
            SQLScriptExecutor rdbScriptExecutor = buildSQLScriptExecutor(dtmConn);
            rdbScriptExecutor.setExecMessage("Executing RDBMS script ");
        	
        	Pattern patternPromptLine = Pattern.compile("^((prompt)|(--p))([ \\t]|$)");
    		Pattern patternCommentLine = Pattern.compile("^--|^--$");
    		Pattern patternStartDen = Pattern.compile("^start[ \\t]+den[ \\t]+");
    		Pattern patternStartDtm = Pattern.compile("^start[ \\t]+rdb[ \\t]+");
    		String scriptFileName = sSrcDir + File.separator + sMainScript;
	        try(BufferedReader br = new BufferedReader(new FileReader(scriptFileName))) 
	        {
	            int lineNo = 0;
	            for(String line; (line = br.readLine()) != null; ) 
	            {
	                ++lineNo;
	                try
	                {
    	            	Matcher matcherPromptLine = patternPromptLine.matcher(line);
    	        		if(matcherPromptLine.find())
    	        		{
    	        			executable.println(line.substring(matcherPromptLine.end()));
    	        		}
    	        		else 
    	        		{
    	                	Matcher matcherCommentLine = patternCommentLine.matcher(line);
    	            		if(!matcherCommentLine.find())
    	            		{
    		                	Matcher matcherStartDen = patternStartDen.matcher(line);
    		                	if(matcherStartDen.find())
    	                        {
    		                		executable.println("");
    		                		String subScriptPathName = sSrcDir + File.separator + line.substring(matcherStartDen.end()); 
    		                		File scriptDen = new File(subScriptPathName);
    	                        	denScriptExecutor.executeScript(scriptDen);
    		                	}
    		                	else
    		                	{
    			                	Matcher matcherStartDtm = patternStartDtm.matcher(line);
    			                	if(matcherStartDtm.find())
    		                        {
    			                		executable.println("");
                                        String subScriptPathName = sSrcDir + File.separator + line.substring(matcherStartDtm.end()); 
    			                		File scriptRdb = new File(subScriptPathName);
    		                        	rdbScriptExecutor.executeScript(scriptRdb);
    			                	}
    			                	else
    			                	{
    				                	if(!line.isEmpty())
    			                		{
    				                		executable.printlnErr("Unknowm command \"" + line + "\"");
    			                            Executable.exitWithError();
    			                		}
    			                	}
    		                	}
    	            		}
    	        		}
    	            }
	                catch (Exception e){
	                    throw new RuntimeException("Error in file \"" + scriptFileName + "\" line " + String.valueOf(lineNo) , e);
	                }                
	            }
    		}
            // Close Connection to database
        }
        if(!executable.bQuiet) executable.println("Database closed ... ");
	}
	
}
