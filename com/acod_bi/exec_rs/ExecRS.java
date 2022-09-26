package com.acod_bi.exec_rs;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.acod_bi.Executable;
import com.acod_bi.SQLConnection;
import com.acod_bi.SQLScriptExecutor;

/** 
 * ACOD Execute a request set
 */
public class ExecRS extends Executable {

	protected SQLConnection sqlConnection = new SQLConnection(this);
	
	String sRSScript;
	boolean bShowSQL;
	boolean bTimingOn;
	
	protected void setParams() {
		super.setParams();
		
	    addParam("showsql", "Show queries", false, null);
	    addParam("s", "Full SQL file path (with request set)", false, "script full path");
	    sqlConnection.executableSetParams();
		addParam("t", "show ellapsed time", false, null);
	}
	
	protected void setVariablesFromParameters() {
		super.setVariablesFromParameters();
		
		bShowSQL = hasParam("showsql");
		
		sRSScript = getParamValue("s");
	    if(sRSScript == null)
	    {
	    	exitWithParametersError("Missing parameter -s <file script>");
	    }
		File rsScript = new File(sRSScript);
		if(!rsScript.isFile())
		{
			printlnErr("Invalid main script : \"" + sRSScript + "\"");
	    	exitWithError();
		}

	    sqlConnection.executableSetVariablesFromParameters();
	    
		bTimingOn = hasParam("t");
		
	}
	
	protected void showParameters() {
		if(!bQuiet) {
			println("Main script : \"" + sRSScript + "\"");
		}
		sqlConnection.executableShowParameters();
		super.showParameters();
        if(bVerbose)
        {
	        if(bShowSQL) println("Show SQL");
        }
	}
	
	protected String getActionName() {
		return "execute request set";
	}
	
	protected SQLScriptExecutor buildSQLScriptExecutor()
	{
        SQLScriptExecutor newScriptExecutor = new SQLScriptExecutor();
        newScriptExecutor.bQuiet = this.bQuiet;
        newScriptExecutor.bShowSQL = this.bShowSQL;
        newScriptExecutor.bTimingOn = this.bTimingOn;
        
        newScriptExecutor.bShowData=false;
        newScriptExecutor.bLogData=true;
        
        newScriptExecutor.setPatternQueryEnd(";$");
        newScriptExecutor.setExecMessage("Executing request set script ");
        
		return newScriptExecutor;
	}
	
	protected void doAction() {
		sqlConnection.loadDriver();
		
        SQLScriptExecutor sqlScriptExecutor = buildSQLScriptExecutor();
        
    	// Create Connection to database
        try(Connection denConn = sqlConnection.getConnection();)
        {
        	sqlScriptExecutor.dbConnection = denConn;
        	
    		File rsScript = new File(sRSScript);
        	sqlScriptExecutor.executeScript(this, rsScript);
        	
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
		ExecRS executable = new ExecRS();
		executable.doMain(args);
	}
}
