package com.acod_bi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/** 
 * ACOD Extract or execute SQL statement 
 */
public class SQLExec {
    
    Executable executable;
    Connection connection;
    
    SQLExec(Executable executable, Connection connection){ this.executable=executable; this.connection=connection; };
    
    //static private Pattern patternSelect = Pattern.compile("^[ \\t]*(?i)SELECT(?-i)[ \\t$]");
    
	void extractQuery2CSV(File dataFile, String sStatement, boolean bShowQuery, boolean bQuiet) throws SQLException, IOException {
		if(bShowQuery) executable.println("-- Extract to \"" + dataFile.getPath() + "\"");
		if(bShowQuery) executable.println(sStatement+";");
    	// Open file
        if(!bQuiet) executable.println("Opening file " + dataFile.getPath() + " ... ");
        try(BufferedWriter br = new BufferedWriter(new FileWriter(dataFile))){
            // Prepare and execute statement
            PreparedStatement statement = connection.prepareStatement(sStatement);
            statement.execute();
            ResultSet result = statement.getResultSet();
            if (result != null) {
                ResultSetMetaData metadata = result.getMetaData();
                int iNbCols = metadata.getColumnCount();

            	// Write Header
                for (int iCol = 1; iCol <= iNbCols; iCol++) {
                	if(iCol>1)
        	            br.write(";");
    	            br.write(metadata.getColumnName(iCol));
                }
	            br.write(System.getProperty("line.separator"));

                // Iterate over the rows returned
                while (result.next()) {
                    // Iterate over the columns of the row
                    for (int iCol = 1; iCol <= iNbCols; iCol++) {
                    	if(iCol>1)
            	            br.write(";");
        	            if (result.getObject(iCol) != null)
                    		br.write(result.getObject(iCol).toString());
                    }
    	            br.write(System.getProperty("line.separator"));
                }
            } else {
                throw new RuntimeException("No ResultSet");
            }
        }
        catch (SQLException e) {
            throw new SQLException("Error in query : " + sStatement, e);
        } 
	}
	
	public int maxNbTry(QueryWithParameters queryWithParameters)
	{
	    return 1;
	}

    // execQuery with parameters
    public void execQuery(QueryWithParameters queryWithParameters, 
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData) throws SQLException {
        if(bShowQuery) 
        {
            //executable.println("--------------------------------------------------------------------------------");
            executable.println("Query:");
            //-----executable.println(sStatement + ";");
            executable.println(queryWithParameters.getQueryStatementAndParamsLog());
            //executable.println("--------------------------------------------------------------------------------");
        }
        
        // Prepare and execute statement
        try (PreparedStatement statement = connection.prepareStatement(queryWithParameters.anonymousStatement))
        {
            int iMaxNbTry = maxNbTry(queryWithParameters);
            int nbTry = 0;
            boolean isExecuteDone = false;
            queryWithParameters.setPreparedStatementParameters(statement);
            while(!isExecuteDone)
            {
                ++nbTry;
                try {
                    statement.execute();
                    if(!bQuiet) executable.println("Query executed");
                    ResultSet resultSet = statement.getResultSet();
                    execQueryCheckResultSet1(queryWithParameters, bShowQuery, bQuiet, bShowData, bLogData, resultSet);
                    execQueryShowResultSet(queryWithParameters, bShowQuery, bQuiet, bShowData, bLogData, resultSet);
                    execQueryCheckResultSet2(queryWithParameters, bShowQuery, bQuiet, bShowData, bLogData, resultSet);
                    isExecuteDone = true;
                }
                catch (SQLException e) {
                    if(nbTry>=iMaxNbTry)
                        throw e;
                    else
                    {
                        executable.println("Error in query - " + e.getMessage());
                        executable.println("Trying again");
                    }
                        
                }
            }
        }
        catch (SQLException e) {
            throw new SQLException("Error in query : " + queryWithParameters.getQueryStatementAndParamsLog(), e);
        }
    }
    
    // Verification du resultSet avant son affichage
    protected void execQueryCheckResultSet1(QueryWithParameters queryWithParameters,  
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData, ResultSet resultSet) throws SQLException 
    {
    }
    
    // Verification du resultSet apres son affichage
    protected void execQueryCheckResultSet2(QueryWithParameters queryWithParameters,  
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData, ResultSet resultSet) throws SQLException 
    {
    }
    
    // execQueryShowResultSet with anonymous string parameters
    protected void execQueryShowResultSet(QueryWithParameters queryWithParameters,  
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData, ResultSet resultSet) throws SQLException 
    {
        if ((resultSet != null)&&(!bQuiet)) 
        {
            ResultSetMetaData metadata = resultSet.getMetaData();
            if (metadata == null)
            {
                throw new SQLException("ResultSetMetaData is null");
            }
            else
            {
                int iNbCols = metadata.getColumnCount();
    
                // Write Header
                for (int iCol = 1; iCol <= iNbCols; iCol++) {
                    if(iCol>1)
                        executable.print("|", bShowData, bLogData);
                    executable.print(metadata.getColumnName(iCol), bShowData, bLogData);
                }
                executable.println("", bShowData, bLogData);
    
                // Iterate over the rows returned
                while (resultSet.next()) {
                    // Iterate over the columns of the row
                    for (int iCol = 1; iCol <= iNbCols; iCol++) {
                        if(iCol>1)
                            executable.print("|", bShowData, bLogData);
                        if (resultSet.getObject(iCol) != null)
                            executable.print(resultSet.getObject(iCol).toString(), bShowData, bLogData);
                    }
                    executable.println("", bShowData, bLogData);
                }
                if(!bQuiet) executable.println("Result set displayed", bShowData, bLogData);
            }
        }
    }
    
    // Les methodes statiques sont conservees pour des raisons historiques
    
    public static void extractQuery2CSV(Executable executable, Connection connection, File dataFile, String sStatement, boolean bShowQuery, boolean bQuiet) throws SQLException, IOException {
        SQLExec o = new SQLExec(executable, connection);
        o.extractQuery2CSV(dataFile, sStatement, bShowQuery, bQuiet);
    }

    // execQuery with parameters
    public static void execQuery(Executable executable, Connection connection, QueryWithParameters queryWithParameters, 
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData) throws SQLException {
        SQLExec o = new SQLExec(executable, connection);
        o.execQuery(queryWithParameters, bShowQuery, bQuiet, bShowData, bLogData);
    }
    
    // execQueryShowResultSet with parameters
    protected static void execQueryShowResultSet(Executable executable, Connection connection, QueryWithParameters queryWithParameters,  
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData, ResultSet resultSet) throws SQLException 
    {
        SQLExec o = new SQLExec(executable, connection);
        o.execQueryShowResultSet(queryWithParameters, bShowQuery, bQuiet, bShowData, bLogData, resultSet);
    }
    
}
