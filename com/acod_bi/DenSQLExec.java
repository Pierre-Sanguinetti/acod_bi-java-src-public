/**
 * 
 */
package com.acod_bi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * ACOD Extract or execute SQL statement in Denodo 
 */
public class DenSQLExec extends SQLExec {

    //private Pattern patternCreateRemoteTable = Pattern.compile("(?i)^[ \\t]*SELECT[ \\t\\n].*[ \\t\\n]FROM[ \\t\\n]+CREATE_REMOTE_TABLE\\(\\)[ \\t\\n]+(?-i)");
    private Pattern patternCreateRemoteTable = Pattern.compile("(?i).*[ \\t\\n,]+CREATE_REMOTE_TABLE[ \\t\\n]*\\((?-i)");
    
    public DenSQLExec(Executable executable, Connection connection) {
        super(executable, connection);
    }
    
    public boolean isQueryCreateRemoteTable(QueryWithParameters queryWithParameters)
    {
        return patternCreateRemoteTable.matcher(queryWithParameters.anonymousStatement).find();
    }
    
    public int maxNbTry(QueryWithParameters queryWithParameters)
    {
        int iMaxNbTry; 
        if(isQueryCreateRemoteTable(queryWithParameters))
            iMaxNbTry = 3;
        else
            iMaxNbTry = super.maxNbTry(queryWithParameters);
        return iMaxNbTry;
    }

    protected void execQueryCheckResultSet2(QueryWithParameters queryWithParameters,  
            boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData, ResultSet resultSet) throws SQLException 
    {
        if(isQueryCreateRemoteTable(queryWithParameters))
        {
            if(resultSet == null)
            {
                throw new SQLException("No ResultSet for Denodo CREATE_REMOTE_TABLE statement");
            }
            else
            {
                ResultSetMetaData metadata = resultSet.getMetaData();
                if (metadata == null)
                {
                    throw new SQLException("No ResultSetMetaData for Denodo CREATE_REMOTE_TABLE statement");
                }
                else
                {
                    int iNbCols = metadata.getColumnCount();
                    int errorColumnIndex = 0;
                    for (int iCol = 1; iCol <= iNbCols; iCol++) {
                        if(metadata.getColumnName(iCol).compareToIgnoreCase("error") == 0)
                        {
                            errorColumnIndex = iCol;
                        }
                    }
                    if(errorColumnIndex == 0)
                    {
                        executable.println("WARNING: You should display error column in Denodo CREATE_REMOTE_TABLE statement");
                    }
                    else
                    {
                        // Iterate over the rows returned
                        while (resultSet.next()) {
                            boolean error = resultSet.getBoolean("error");
                            if(error)
                            {
                                throw new SQLException("error in Denodo CREATE_REMOTE_TABLE statement result set, line " + String.valueOf(resultSet.getRow()));
                            }
                                
                        }
                    }
                }
            }
        }
    }
    
    

}
