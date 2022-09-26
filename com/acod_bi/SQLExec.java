package com.acod_bi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Vector;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/** 
 * ACOD Extract or execute SQL statement 
 */
public class SQLExec {

	public static void extractQuery2CSV(Executable executable, Connection connection, File dataFile, String sStatement, boolean bShowQuery, boolean bQuiet) throws SQLException, IOException {
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

	public static void execQuery(Executable executable, Connection connection, String sStatement, Vector<String> queryParams, 
			boolean bShowQuery, boolean bQuiet, boolean bShowData, boolean bLogData) throws SQLException {
		if(bShowQuery) {
			//executable.println("--------------------------------------------------------------------------------");
			executable.println("Query:");
			executable.println(sStatement + ";");
			//executable.println("--------------------------------------------------------------------------------");
		}
		
        // Prepare and execute statement
		try (PreparedStatement statement = connection.prepareStatement(sStatement)){
	        if(queryParams != null)
	        {
	        	int iParamPos = 1; 
	        	for (String sParamValue : queryParams) {
	        		statement.setString(iParamPos, sParamValue);
	        		++iParamPos;
	        	}	        	
	        }
	        statement.execute();
			if(!bQuiet) executable.println("Query executed");
	        ResultSet result = statement.getResultSet();
	        if ((result != null)&&(!bQuiet)) 
	        {
	            ResultSetMetaData metadata = result.getMetaData();
	            int iNbCols = metadata.getColumnCount();
	
	        	// Write Header
	            for (int iCol = 1; iCol <= iNbCols; iCol++) {
	            	if(iCol>1)
	            		executable.print("|", bShowData, bLogData);
	            	executable.print(metadata.getColumnName(iCol), bShowData, bLogData);
	            }
	            executable.println("", bShowData, bLogData);
	
	            // Iterate over the rows returned
	            while (result.next()) {
	                // Iterate over the columns of the row
	                for (int iCol = 1; iCol <= iNbCols; iCol++) {
	                	if(iCol>1)
	                		executable.print("|", bShowData, bLogData);
	    	            if (result.getObject(iCol) != null)
	    	            	executable.print(result.getObject(iCol).toString(), bShowData, bLogData);
	                }
		            executable.println("", bShowData, bLogData);
	            }
	            if(!bQuiet) executable.println("Result set displayed", bShowData, bLogData);
	        }
		}
        catch (SQLException e) {
            throw new SQLException("Error in query : " + sStatement, e);
        } 
	}
	
}
