package com.acod_bi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * ACOD Execute an SQL script
 */
public class SQLScriptExecutor /*implements AutoCloseable*/ {
	
    public SQLScriptExecutor(Executable executable, Connection connection){ this.executable=executable; this.connection=connection; };
    
	public boolean bQuiet = false;
	public boolean bShowSQL = false;
	public boolean bShowData = true;
	public boolean bLogData = true;
	public boolean bTimingOn = false;
	public Connection connection;
	public Executable executable; 
	
	private SQLExec sqlExec=null;
	
	public SQLExec getSqlExec() {
	    if(sqlExec == null)
	        sqlExec = new SQLExec(executable, connection);
        return sqlExec;
    }

    public void setSqlExec(SQLExec sqlExec) {
        this.sqlExec = sqlExec;
    }

    public String getDefaultPatternPromptLine() { return "^((prompt)|(--p))([ \\t]|$)"; };
	private Pattern patternPromptLine = Pattern.compile(getDefaultPatternPromptLine());
	public void setPatternPromptLine(String sPatternPromptLine){patternPromptLine = Pattern.compile(sPatternPromptLine);}
	
    //-----public String getDefaultPatternCommentLine() { return "(?i)^--|^DEFINE |^DEF |^VARIABLE |^exec DBMS_OUTPUT\\.|^--$(?-i)"; };
	//-----public String getDefaultPatternCommentLine() { return "^(--.*)|[ \\t]*$"; };
	public String getDefaultPatternCommentLine() { return "(^[ \\t]*(--.*)$)|(^[ \\t]*$)"; };
	private Pattern patternCommentLine = Pattern.compile(getDefaultPatternCommentLine());
	public void setPatternCommentLine(String sPatternCommentLine){patternCommentLine = Pattern.compile(sPatternCommentLine);}
	
    public String getDefaultPatternQueryEnd() { return "(^/$)|(;[ \t]*$)"; };
    private Pattern patternQueryEnd = Pattern.compile(getDefaultPatternQueryEnd());
    public void setPatternQueryEnd(String sPatternQueryEnd){patternQueryEnd = Pattern.compile(sPatternQueryEnd);}

	// pattern d'affectation de valeur a une variable en une seule ligne
    //-----public String getDefaultPatternSetVarLine() { return "^exec :(?<varname>vv[1-3]) := '(?<stringvalue>(.)*)';$"; };
    // ^[ \t]*@set[ \t]+(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)[ \t]*=[ \t]*'(?<stringvalue>(.)*)'[ \t]*;?[ \t]*$
    public String getDefaultPatternSetVarByValue() { return "^[ \\t]*@set[ \\t]+(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)[ \\t]*=[ \\t]*'(?<stringvalue>(.)*)'[ \\t]*;?[ \\t]*$"; };
    private Pattern patternSetVarByValue = Pattern.compile(getDefaultPatternSetVarByValue());
    public void setPatternSetVarByValue(String sPatternSetVarByValue){patternSetVarByValue = Pattern.compile(sPatternSetVarByValue);}
    
    // ^[ \t]*@set[ \t]+(?<varname2>[a-zA-Z_][a-zA-Z0-9_]*)[ \t]*=[ \t]*\((?<queryvalue>(.)*)\)[ \t]*;[ \t]*?$
    public String getDefaultPatternSetVarByQuery() { return "^[ \\t]*@set[ \\t]+(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)[ \\t]*=[ \\t]*\\((?<queryvalue>(.)*)\\)[ \\t]*;?[ \\t]*$"; };
    private Pattern patternSetVarByQuery = Pattern.compile(getDefaultPatternSetVarByQuery());
    public void setPatternSetVarByQuery(String sPatternSetVarByQuery){patternSetVarByQuery = Pattern.compile(sPatternSetVarByQuery);}

    //----- private Pattern patternRefVar = Pattern.compile("(?<varref>:vv[1-3])([^a-zA-Z_$]|$)");
    // \$\{(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)\}
    public String getDefaultPatternRefVar() { return "\\$\\{(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)\\}"; };
    private Pattern patternRefVar = Pattern.compile(getDefaultPatternRefVar());
    public void setPatternRefVar(String sPatternRefVar){patternRefVar = Pattern.compile(sPatternRefVar);}

	private Pattern patternStartFile = Pattern.compile("^start[ \\t]+\"(?<filename>(.)*)\"[ \\t]*$");
	public void setPatternStartFile(String sNewPatternStartFile){patternStartFile = Pattern.compile(sNewPatternStartFile);}
	
    private String sExecMessage = "Executing script ";
    public void setExecMessage(String sNewExecMessage){sExecMessage = sNewExecMessage;}
	
	public void executeScript(File sqlFile)  throws SQLException, IOException
	{
        String sQueryText = null;
        Map<String, String> queryParams = new LinkedHashMap<String, String>();
		executeScript(sqlFile, sQueryText, queryParams);
	}
	
	public void executeScript(File sqlFile, String sQueryText, Map<String, String> queryParams)  throws SQLException, IOException
	{
		executable.println(sExecMessage + "\"" + sqlFile + "\"");
		if(!sqlFile.isFile())
			throw new FileNotFoundException("Script file \"" + sqlFile.getPath() + "\" not found");
        try(BufferedReader br = new BufferedReader(new FileReader(sqlFile))) 
        {
            int lineNo = 0;
            for(String line; (line = br.readLine()) != null; ) 
            {
                ++lineNo;
                try
                {
                	// bLineDone indique si la ligne a ete consommee
                	boolean bLineDone = false;
                	// Si on n'a pas commence la construction d'une query sur plusieurs lignes on essaie de consommer la ligne seule
            		if(sQueryText == null)
            		{
                    	Matcher matcherSetVarByValue = patternSetVarByValue.matcher(line);
                		if(matcherSetVarByValue.find())
                		{
                		    String varName = matcherSetVarByValue.group("varname");
                		    String stringValue = matcherSetVarByValue.group("stringvalue");
                            if(!bQuiet)
                            {
                                executable.println("@SET " + varName + "='" + stringValue + "'");
                            }
                		    queryParams.put(varName, stringValue);
                			bLineDone = true;
                		}
                		else
                		{
                            Matcher matcherSetVarByQuery = patternSetVarByQuery.matcher(line);
                            if(matcherSetVarByQuery.find())
                            {
                                String varName = matcherSetVarByQuery.group("varname");
                                String sql = matcherSetVarByQuery.group("queryvalue");
                                if(bShowSQL) 
                                {
                                    executable.print("SET query:");
                                    executable.println(sql + ";");
                                }
                                String stringValue;
                                try (PreparedStatement statement = connection.prepareStatement(sql))
                                {
                                    statement.execute();
                                    ResultSet resultSet = statement.getResultSet();
                                    if(resultSet == null)
                                    {
                                        throw new SQLException("No result set for SET query");
                                    }
                                    if(!resultSet.next())
                                    {
                                        throw new SQLException("No data for SET query");
                                    }
                                    stringValue = resultSet.getString(1);
                                    if(resultSet.wasNull())
                                    {
                                        throw new SQLException("NULL value not supported for @SET command");
                                    }
                                }
                                catch (SQLException e) {
                                    throw new SQLException("Error in query : " + sql, e);
                                }
                                if(!bQuiet)
                                {
                                    executable.println("@SET " + varName + "='" + stringValue + "'");
                                }
                                queryParams.put(varName, stringValue);
                                bLineDone = true;
                                /*
                                PreparedStatement statementSetVarByQuery = dbConnection.prepareStatement(querySetVarByQuery);
                                //ResultSet resultSetVarByQuery = statementSetVarByQuery.executeQuery();
                                statementSetVarByQuery.execute();
                                ResultSet resultSetVarByQuery = statementSetVarByQuery.getResultSet();
                                resultSetVarByQuery.next(); 
                                String sValue = resultSetVarByQuery.getString(1);
                                queryParams.put(matcherSetVarByQuery.group("varname"), sValue);
                                */
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
                	            			executeScript(newSqlFile, sQueryText, queryParams);
                                			bLineDone = true;
                	            		}
            	            		}
            	        		}
                            }
                		}
            		}
                	// Si la ligne n'a pas ete consommee seule elle est traitee comme faisant partie d'une query d'une ou plusieurs lignes 
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
                    		
                    		Instant begin, end;
                    		begin = Instant.now();
                    		getSqlExec().execQuery(new QueryWithParameters(sQueryText, queryParams, patternRefVar), bShowSQL, bQuiet, bShowData, bLogData);
                    		end = Instant.now();
    	        			executable.print("Ellapsed:");
    	        			executable.println(Duration.between(begin, end).toString());
                    		
                            sQueryText = null;
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
                catch (Exception e){
                    throw new RuntimeException("Error in file \"" + sqlFile + "\" line " + String.valueOf(lineNo) , e);
                }                
            }
        }
	}
}
