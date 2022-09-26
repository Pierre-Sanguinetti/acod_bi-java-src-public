package com.acod_bi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * ACOD Execute an SQL script
 */
public class SQLScriptExecutor /*implements AutoCloseable*/ {
	
	public boolean bQuiet = false;
	public boolean bShowSQL = false;
	public boolean bShowData = true;
	public boolean bLogData = true;
	public boolean bTimingOn = false;
	public Connection dbConnection = null;
	
	private Pattern patternPromptLine = Pattern.compile("^prompt |^prompt$");
	public void setPatternPromptLine(String sNewPatternPromptLine){patternPromptLine = Pattern.compile(sNewPatternPromptLine);}
	
	private Pattern patternCommentLine = Pattern.compile("(?i)^--|^DEFINE |^DEF |^VARIABLE |^exec DBMS_OUTPUT\\.|^--$(?-i)");
	public void setPatternCommentLine(String sNewPatternCommentLine){patternCommentLine = Pattern.compile(sNewPatternCommentLine);}
	
    private Pattern patternQueryEnd = null;
    public void setPatternQueryEnd(String sNewPatternQueryEnd){patternQueryEnd = Pattern.compile(sNewPatternQueryEnd);}
	
	private Pattern patternSetVar = Pattern.compile("^exec :(?<varname>vv[1-3]) := '(?<stringvalue>(.)*)';$");
	private Pattern patternRefVar = Pattern.compile("(?<varref>:vv[1-3])([^a-zA-Z_$]|$)");
	
	private Pattern patternStartFile = Pattern.compile("^start[ \\t]+\"(?<filename>(.)*)\"[ \\t]*$");
	public void setPatternStartFile(String sNewPatternStartFile){patternStartFile = Pattern.compile(sNewPatternStartFile);}
	
    private String sExecMessage = "Executing script ";
    public void setExecMessage(String sNewExecMessage){sExecMessage = sNewExecMessage;}
	
	public void executeScript(Executable executable, File sqlFile)  throws SQLException, IOException
	{
        String sQueryText = null;
        Vector<String> queryParams = new Vector<String>();  
		executeScript(executable, sqlFile, sQueryText, queryParams);
	}
	
	public void executeScript(Executable executable, File sqlFile, String sQueryText, Vector<String> queryParams)  throws SQLException, IOException
	{
		executable.println(sExecMessage + "\"" + sqlFile + "\"");
		if(!sqlFile.isFile())
			throw new FileNotFoundException("Script file \"" + sqlFile.getPath() + "\" not found");
        try(BufferedReader br = new BufferedReader(new FileReader(sqlFile))) 
        {
            for(String line; (line = br.readLine()) != null; ) 
            {
            	boolean bLineDone = false;
        		if(sQueryText == null)
        		{
                	Matcher matcherSetVar = patternSetVar.matcher(line);
            		if(matcherSetVar.find())
            		{
            			queryParams.add(matcherSetVar.group("stringvalue"));
            			bLineDone = true;
            		}
            		else
            		{
    	            	Matcher matcherPromptLine = patternPromptLine.matcher(line);
    	        		if(matcherPromptLine.find())
    	        		{
    	        			executable.println(line.substring(matcherPromptLine.end()));
                			bLineDone = true;
    	        		}
    	        		else 
    	        		{
    	                	Matcher matcherCommentLine = patternCommentLine.matcher(line);
    	            		if(matcherCommentLine.find())
    	            		{
                    			bLineDone = true;
    	            		}
    	            		else
    	            		{
        	                	Matcher matcherStartFile = patternStartFile.matcher(line);
        	            		if(matcherStartFile.find())
        	            		{
        	            			String sNewFileName = matcherStartFile.group("filename");
        	            			File newSqlFile =new File(sNewFileName);
        	            			executeScript(executable, newSqlFile, sQueryText, queryParams);
                        			bLineDone = true;
        	            		}
    	            		}
    	        		}
            		}
        		}
        		if(!bLineDone)
        		{
                	Matcher matcherQueryEnd = patternQueryEnd.matcher(line);
                	if(matcherQueryEnd.find())
                	{
                		if(sQueryText == null)
                    		sQueryText = line.substring(0, matcherQueryEnd.start());
                		else
                			if(matcherQueryEnd.start()>0)
                				sQueryText += System.lineSeparator() + line.substring(0, matcherQueryEnd.start());
                        // Remplacement des variables par ?
                		// Ne prend pas en compte les chaines contenant ':vv[1-3]'  
                		sQueryText = patternRefVar.matcher(sQueryText).replaceAll("?");
                		
                		Instant begin, end;
                		begin = Instant.now();
                		SQLExec.execQuery(executable, dbConnection, sQueryText, queryParams, bShowSQL, bQuiet, bShowData, bLogData);
                		end = Instant.now();
	        			executable.print("Ellapsed:");
	        			executable.println(Duration.between(begin, end).toString());
                		
                        sQueryText = null;
                        queryParams.clear();
                	}
                	else
                	{
                		if(sQueryText == null)
                		{
                			if(!line.isEmpty())
                				sQueryText = line;
                		}
                		else
                    		sQueryText += System.lineSeparator() + line;
                	}
        		}
            }
        }
	}
}
