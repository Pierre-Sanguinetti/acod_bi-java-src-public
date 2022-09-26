package com.acod_bi.den_dtm_creator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.acod_bi.Executable;
import com.acod_bi.SQLScriptExecutor;
import com.acod_bi.SQLDoubleConnection;

/** 
 * ACOD Create a Denodo datamart from script
 */
public class DenDtmCreator extends Executable {

	protected SQLDoubleConnection sqlDoubleConnection = new SQLDoubleConnection(this);
	
	String sSrcDir;
	String sMainScript;
	boolean bShowSQL;
	
	protected void setParams() {
		super.setParams();
		
	    addParam("showsql", "Show queries", false, null);
	    
	    addParam("sd", "Source directory with create datamart SQL files", false, "source directory");
	    addParam("m", "Main script calling others SQL files to create datamart", true, "main script");
	    
	    sqlDoubleConnection.executableSetParams();
	}
	
	protected void setVariablesFromParameters() {
		super.setVariablesFromParameters();
		
		bShowSQL = hasParam("showsql");
		
		sSrcDir = getParamValue("sd", ".");
		File srcFile = new File(sSrcDir);
		if(!srcFile.isDirectory())
		{
			printlnErr("Invalid source directory : \"" + sSrcDir + "\"");
	    	exitWithError();
		}

		sMainScript = getParamValue("m");
		File mainScript = new File(sSrcDir + File.separator + sMainScript);
		if(!mainScript.isFile())
		{
			printlnErr("Invalid main script : \"" + sSrcDir + File.separator + sMainScript + "\"");
	    	exitWithError();
		}

		sqlDoubleConnection.executableSetVariablesFromParameters();
	}
	
	protected void showParameters() {
		if(!bQuiet) {
			println("Source directory : \"" + sSrcDir + "\"");
			println("Main script : \"" + sMainScript + "\"");
		}
		sqlDoubleConnection.executableShowParameters();
		super.showParameters();
        if(bVerbose)
        {
	        if(bShowSQL) println("Show SQL");
        }
	}
	
	protected String getActionName() {
		return "Create datamart";
	}
	
	protected SQLScriptExecutor buildSQLScriptExecutor()
	{
        SQLScriptExecutor newScriptExecutor = new SQLScriptExecutor();
        newScriptExecutor.bQuiet = this.bQuiet;
        newScriptExecutor.bShowSQL = this.bShowSQL;
		return newScriptExecutor;
	}
	
	protected void doAction() {
		sqlDoubleConnection.loadDriver();
		
        SQLScriptExecutor denScriptExecutor = buildSQLScriptExecutor();
        denScriptExecutor.setPatternQueryEnd(";$");
        denScriptExecutor.setExecMessage("Executing Denodo script ");
        
        SQLScriptExecutor dtmScriptExecutor = buildSQLScriptExecutor();
        dtmScriptExecutor.setPatternQueryEnd("^/$");
        dtmScriptExecutor.setExecMessage("Executing datamart script ");
        
    	// Create Connection to database
        try(
        		Connection denConn = sqlDoubleConnection.getDenConnection(); 
        		Connection dtmConn = sqlDoubleConnection.getDtmConnection(); 
        )
        {
        	denScriptExecutor.dbConnection = denConn;
        	dtmScriptExecutor.dbConnection = dtmConn;
        	
    		Pattern patternPromptLine = Pattern.compile("^prompt |^prompt$");
    		Pattern patternCommentLine = Pattern.compile("^--|^--$");
    		Pattern patternStartDen = Pattern.compile("^call den ");
    		Pattern patternStartDtm = Pattern.compile("^call dtm ");
	        try(BufferedReader br = new BufferedReader(new FileReader(sSrcDir + File.separator + sMainScript))) 
	        {
	            for(String line; (line = br.readLine()) != null; ) 
	            {
	            	Matcher matcherPromptLine = patternPromptLine.matcher(line);
	        		if(matcherPromptLine.find())
	        		{
	        			println(line.substring(matcherPromptLine.end()));
	        		}
	        		else 
	        		{
	                	Matcher matcherCommentLine = patternCommentLine.matcher(line);
	            		if(!matcherCommentLine.find())
	            		{
		                	Matcher matcherStartDen = patternStartDen.matcher(line);
		                	if(matcherStartDen.find())
	                        {
			        			println("");
		                		File scriptDen = new File(sSrcDir + File.separator + line.substring(matcherStartDen.end()));
	                        	denScriptExecutor.executeScript(this, scriptDen);
		                	}
		                	else
		                	{
			                	Matcher matcherStartDtm = patternStartDtm.matcher(line);
			                	if(matcherStartDtm.find())
		                        {
				        			println("");
			                		File scriptDtm = new File(sSrcDir + File.separator + line.substring(matcherStartDtm.end()));
		                        	dtmScriptExecutor.executeScript(this, scriptDtm);
			                	}
			                	else
			                	{
			                		//if(!line.isBlank())
				                	if(!line.isEmpty())
			                		{
			                            printlnErr("Unknowm command \"" + line + "\"");
			                            exitWithError();
			                		}
			                	}
		                	}
	            		}
	        		}
	            }
    		}
            // Close Connection to database
        }
        catch (SQLException e) {
            //println("Database Error : " + e.getMessage());
            throw new RuntimeException("Database Error", e);
            //bExitWithError = true;
        } 
        catch (IOException e) {
            //e.printStackTrace();
            throw new RuntimeException("IOException", e);
        }
        if(!bQuiet) println("Database closed ... ");
	}
	
	public static void main(String[] args) {
		Executable executable = new DenDtmCreator();
		executable.doMain(args);
	}

}
