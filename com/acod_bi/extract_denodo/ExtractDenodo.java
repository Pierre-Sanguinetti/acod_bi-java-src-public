package com.acod_bi.extract_denodo;

import java.sql.Connection;
import java.sql.SQLException;
import java.io.File;
import java.io.IOException;

import com.acod_bi.Executable;
import com.acod_bi.SQLConnection;
import com.acod_bi.SQLExec;

/** 
 * ACOD Extract source meta-data from Denodo catalog
 */
public class ExtractDenodo extends Executable {

	protected String getActionName() {
		return "meta-data extraction";
	}
	
	protected SQLConnection sqlConnection = new SQLConnection(this);
	
	boolean bShowSQL;
	String sDestDir;
	String sViewSelection;
	
	// Overload with specific options
	protected void setParams() {
		super.setParams();
		
		sqlConnection.executableSetParams();

	    addParam("showsql", "Show extract queries", false, null);
	    addParam("dd", "Destination directory where files are generated", false, "destination directory");
	    addParam("vs", "Denodo query retrieving datamart source views (view database name, view name)", false, "view selection query");
	}
	
	// Overload with specific parameters
	protected void setVariablesFromParameters() {
		sqlConnection.executableSetVariablesFromParameters();
		if(!sqlConnection.sRdbmsId.equals("den"))
		{
			this.printlnErr("Use this application only for Denodo");
			Executable.exitWithError();
		}
		
		bShowSQL = hasParam("showsql");
		sDestDir = getParamValue("dd", ".");
		sViewSelection = getParamValue("vs");

		if(sViewSelection == null)
		{
			String sLogDbName;
			if(sqlConnection.sDbUrl.lastIndexOf("/") == -1)
				sLogDbName = "null";
			else
				sLogDbName = "'" + sqlConnection.sDbUrl.substring(sqlConnection.sDbUrl.lastIndexOf("/")+1) + "'";
	    	sViewSelection = new String("SELECT database_name, name "
	    			+ "FROM GET_VIEWS() "
	    			+ "WHERE input_database_name=" + sLogDbName
	    			+ " ORDER BY database_name, name");
		}
	}
	
	// Overload with specific parameters and options
	protected void showParameters() {
		super.showParameters();
		sqlConnection.executableShowParameters();
		if(!bQuiet) {
			println("Destination directory: \"" + sDestDir + "\"");
		}
        if(bVerbose)
        {
	        println("View selection : \"" + sViewSelection + "\"");
	        if(bShowSQL) println("Show SQL");
        }
	}
	
	// Overload with specific action
	protected void doAction() {
        // JDBC Driver load
		sqlConnection.loadDriver();

    	// Create Connection to database
        try(Connection connection = sqlConnection.getConnection())
        {
        	////////////////////////////////////////////////////////////////////////////////
        	// NTAB File
        	////////////////////////////////////////////////////////////////////////////////
        	if(!bQuiet) println("Extracting NTAB meta-data ... ");
        	SQLExec.extractQuery2CSV(this, connection, 
        		new File(sDestDir + "\\exc_ntab.csv"), 
        		"WITH t_view_selection AS(\r\n"
        		+ sViewSelection + "\r\n"
        		+ ")\r\n"
        		+ "SELECT t_view_selection.database_name || '.' || t_view_selection.name \"NTAB_CODE\", MAX(t_view_stat.rows_number) \"NTAB_NB_ROWS\"\r\n"
        		+ "FROM t_view_selection\r\n"
        		+ "LEFT OUTER JOIN GET_VIEW_STATISTICS() t_view_stat\r\n"
        		+ "   ON t_view_stat.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_view_stat.input_name=t_view_selection.name\r\n"
        		+ "GROUP BY t_view_selection.database_name, t_view_selection.name\r\n"
        		+ "ORDER BY t_view_selection.database_name, t_view_selection.name", 
        		bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NCOL File
        	////////////////////////////////////////////////////////////////////////////////
            if(!bQuiet) println("Extracting NCOL meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_ncol.csv"),
        		"WITH t_view_selection AS(\r\n"
        		+ sViewSelection + "\r\n"
        		+ ")\r\n"
        		+ "SELECT\r\n"
        		+ "   t_view_selection.database_name || '.' || t_view_selection.name \"NTAB_CODE\", \r\n"
        		+ "   t_view_col.column_name \"NCOL_CODE\", \r\n"
        		+ "   t_view_col.ordinal_position \"NCOL_ORDER_NO\", \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN column_sql_type IN ('BIT', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN 'N'\r\n"
        		+ "      WHEN column_sql_type IN ('FLOAT', 'REAL', 'DOUBLE') THEN 'F'\r\n"
        		+ "      WHEN column_sql_type IN ('NUMERIC', 'DECIMAL') THEN 'N'\r\n"
        		+ "      WHEN column_sql_type IN ('CHAR', 'NCHAR') THEN 'A'\r\n"
        		+ "      WHEN column_sql_type IN ('VARCHAR', 'LONGVARCHAR', 'NVARCHAR', 'LONGNVARCHAR') THEN 'VA'\r\n"
        		+ "      WHEN column_sql_type IN ('DATE', 'TIME', 'TIMESTAMP', 'TIME_WITH_TIMEZONE', 'TIMESTAMP_WITH_TIMEZONE') THEN 'DT'\r\n"
        		+ "      ELSE 'UN' -- Unsupported\r\n"
        		+ "   END \"NCOL_DATA_TYPE\", \r\n"
        		+ "   '<dwh_column>' \"NCOL_CONVERT2DTM\", \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN column_sql_type IN ('CHAR', 'NCHAR') THEN column_size\r\n"
        		+ "      WHEN column_sql_type IN ('VARCHAR', 'LONGVARCHAR', 'NVARCHAR', 'LONGNVARCHAR') THEN column_size\r\n"
        		+ "      ELSE NULL\r\n"
        		+ "   END \"NCOL_MAX_NB_CHARS\", \r\n"
        		+ "   CASE \r\n"
        		+ "      WHEN rows_number = 0 THEN NULL\r\n"
        		+ "      WHEN (rows_number-null_values) = 0 THEN 0\r\n"
        		+ "      WHEN column_sql_type IN ('CHAR', 'NCHAR') THEN column_size\r\n"
        		+ "      WHEN column_sql_type IN ('VARCHAR', 'LONGVARCHAR') THEN \r\n"
        		+ "         CASE \r\n"
        		+ "            WHEN column_size <= 253 THEN ((((average_size-0.5)*rows_number)-null_values)/(rows_number-null_values))-1.0\r\n"
        		+ "            ELSE ((((average_size-0.5)*rows_number)-null_values)/(rows_number-null_values))-3.0\r\n"
        		+ "         END\r\n"
        		+ "      WHEN column_sql_type IN ('NVARCHAR', 'LONGNVARCHAR') THEN (((((average_size-0.5)*rows_number)-null_values)/(rows_number-null_values))-1.0)/2.0\r\n"
        		+ "      ELSE NULL\r\n"
        		+ "   END \"NCOL_AVG_NB_CHARS\", \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN column_sql_type IN ('BIT', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN column_size\r\n"
        		+ "      WHEN column_sql_type IN ('FLOAT', 'REAL', 'DOUBLE') THEN column_size\r\n"
        		+ "      WHEN column_sql_type IN ('NUMERIC', 'DECIMAL') THEN column_size\r\n"
        		+ "      ELSE NULL\r\n"
        		+ "   END \"NCOL_MAX_NB_DIGITS\", \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN rows_number = 0 THEN NULL\r\n"
        		+ "      WHEN rows_number-null_values = 0 THEN 0\r\n"
        		+ "      WHEN (column_sql_type IN ('BIT', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') \r\n"
        		+ "         OR column_sql_type IN ('FLOAT', 'REAL', 'DOUBLE')\r\n"
        		+ "         OR column_sql_type IN ('NUMERIC', 'DECIMAL')) THEN\r\n"
        		+ "         CASE\r\n"
        		+ "            WHEN (((((rows_number*(average_size-0.5))-(null_values*1.0))/(rows_number-null_values))-2.5)*2.0) < 1.0 THEN 1.0\r\n"
        		+ "            WHEN (((((rows_number*(average_size-0.5))-(null_values*1.0))/(rows_number-null_values))-2.5)*2.0) > 38.0 THEN 38.0\r\n"
        		+ "            ELSE (((((rows_number*(average_size-0.5))-(null_values*1.0))/(rows_number-null_values))-2.5)*2.0)\r\n"
        		+ "         END\r\n"
        		+ "      ELSE NULL\r\n"
        		+ "   END \"NCOL_AVG_NB_DIGITS\", \r\n"
        		+ "   column_decimals \"NCOL_SCALE\", \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN column_is_nullable THEN 0\r\n"
        		+ "      ELSE 1\r\n"
        		+ "   END \"NCOL_MANDATORY\",\r\n"
        		+ "   distinct_values \"NCOL_NB_DISTINCT_VALUES\", \r\n"
        		+ "   null_values \"NCOL_NB_NULLS\"\r\n"
        		+ "FROM t_view_selection\r\n"
        		+ "INNER JOIN GET_VIEW_COLUMNS() t_view_col\r\n"
        		+ "   ON t_view_col.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_view_col.input_view_name=t_view_selection.name\r\n"
        		+ "   AND t_view_col.view_name=t_view_selection.name\r\n"
        		+ "LEFT OUTER JOIN GET_VIEW_STATISTICS() t_view_stat\r\n"
        		+ "   ON t_view_stat.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_view_stat.input_name=t_view_selection.name\r\n"
        		+ "   AND t_view_stat.field_name=t_view_col.column_name\r\n"
        		+ "ORDER BY \r\n"
        		+ "   t_view_selection.database_name, \r\n"
        		+ "   t_view_selection.name, \r\n"
        		+ "   t_view_col.ordinal_position", 
        		bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NUK File
        	////////////////////////////////////////////////////////////////////////////////
            if(!bQuiet) println("Extracting NUK meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nuk.csv"),
        		"WITH t_view_selection AS(\r\n"
                + sViewSelection + "\r\n"
        		+ ")\r\n"
        		+ "SELECT\r\n"
        		+ "   t_pk.database_name || '.' || t_pk.primary_key_name \"NUK_CODE\", \r\n"
        		+ "   t_view_selection.database_name || '.' || t_view_selection.name \"NTAB_CODE\", \r\n"
        		+ "   1 \"NUK_PRIMARY\"\r\n"
        		+ "FROM t_view_selection\r\n"
        		+ "INNER JOIN GET_PRIMARY_KEYS() t_pk\r\n"
        		+ "   ON t_pk.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_pk.input_view_name=t_view_selection.name\r\n"
        		+ "GROUP BY \r\n"
        		+ "   t_view_selection.database_name, \r\n"
        		+ "   t_view_selection.name, \r\n"
        		+ "   t_pk.database_name, \r\n"
        		+ "   t_pk.primary_key_name\r\n"
        		+ "UNION ALL\r\n"
        		+ "SELECT\r\n"
        		+ "   t_ind.database_name || '.' || t_ind.index_name \"NUK_CODE\", \r\n"
        		+ "   t_view_selection.database_name || '.' || t_view_selection.name \"NTAB_CODE\", \r\n"
        		+ "   0 \"NUK_PRIMARY\"\r\n"
        		+ "FROM t_view_selection\r\n"
        		+ "INNER JOIN GET_VIEW_INDEXES () t_ind\r\n"
        		+ "   ON t_ind.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_ind.input_view_name=t_view_selection.name\r\n"
        		+ "-- NOT EXISTS does not work\r\n"
        		+ "LEFT OUTER JOIN GET_PRIMARY_KEYS() t_pk2\r\n"
        		+ "   ON t_pk2.input_database_name=t_ind.input_database_name\r\n"
        		+ "   AND t_pk2.input_view_name=t_ind.view_name\r\n"
        		+ "   AND t_pk2.primary_key_name=t_ind.index_name\r\n"
        		+ "   AND t_pk2.column_name=t_ind.column_name\r\n"
        		+ "WHERE unique\r\n"
        		+ "AND t_pk2.primary_key_name IS NULL\r\n"
        		+ "GROUP BY \r\n"
        		+ "   t_view_selection.database_name, \r\n"
        		+ "   t_view_selection.name, \r\n"
        		+ "   t_ind.database_name, \r\n"
        		+ "   t_ind.index_name\r\n"
        		+ "ORDER BY \r\n"
        		+ "   \"NTAB_CODE\", \r\n"
        		+ "   \"NUK_CODE\"", 
        		bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NUK_NCOL File
        	////////////////////////////////////////////////////////////////////////////////
            if(!bQuiet) println("Extracting NUK_NCOL meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nuk_ncol.csv"),
        		"WITH t_view_selection AS(\r\n"
                + sViewSelection + "\r\n"
        		+ ")\r\n"
        		+ "SELECT\r\n"
        		+ "   t_ind.database_name || '.' || t_ind.index_name \"NUK_CODE\", \r\n"
        		+ "   t_view_selection.database_name || '.' || t_view_selection.name \"NTAB_CODE\", \r\n"
        		+ "   t_ind.column_name \"NCOL_CODE\", \r\n"
        		+ "   t_ind.ordinal_position \"NUK_NCOL_ORDER_NO\"\r\n"
        		+ "FROM t_view_selection\r\n"
        		+ "INNER JOIN GET_VIEW_INDEXES () t_ind\r\n"
        		+ "   ON t_ind.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_ind.input_view_name=t_view_selection.name\r\n"
        		+ "WHERE t_ind.unique\r\n"
        		+ "UNION ALL\r\n"
        		+ "SELECT\r\n"
        		+ "   t_pk.database_name || '.' || t_pk.primary_key_name \"NUK_CODE\", \r\n"
        		+ "   t_view_selection.database_name || '.' || t_view_selection.name \"NTAB_CODE\", \r\n"
        		+ "   t_pk.column_name \"NCOL_CODE\", \r\n"
        		+ "   ROWNUM() \"NUK_NCOL_ORDER_NO\"\r\n"
        		+ "FROM t_view_selection\r\n"
        		+ "INNER JOIN GET_PRIMARY_KEYS() t_pk\r\n"
        		+ "   ON t_pk.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_pk.input_view_name=t_view_selection.name\r\n"
        		+ "LEFT OUTER JOIN GET_VIEW_INDEXES() t_ind2\r\n"
        		+ "   ON t_pk.input_database_name=t_ind2.input_database_name\r\n"
        		+ "   AND t_pk.input_view_name=t_ind2.view_name\r\n"
        		+ "   AND t_pk.primary_key_name=t_ind2.index_name\r\n"
        		+ "   AND t_pk.column_name=t_ind2.column_name\r\n"
        		+ "WHERE t_ind2.index_name IS NULL\r\n"
        		+ "ORDER BY \r\n"
        		+ "   \"NTAB_CODE\", \r\n"
        		+ "   \"NUK_CODE\", \r\n"
        		+ "   \"NUK_NCOL_ORDER_NO\"", 
        		bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NFK File
        	////////////////////////////////////////////////////////////////////////////////
            if(!bQuiet) println("Extracting NFK meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nfk.csv"),
        		"WITH t_view_selection AS(\r\n"
                + sViewSelection + "\r\n"
        		+ ")\r\n"
        		+ "SELECT \r\n"
        		+ "   t_fk.fk_database_name || '.' || t_fk.fk_name \"NFK_CODE\", \r\n"
        		+ "   t_fk.fk_database_name || '.' || t_fk.fk_view_name \"NTAB_CODE_CHILD\", \r\n"
        		+ "   t_fk.pk_database_name || '.' || t_fk.pk_view_name \"NTAB_CODE_PARENT\", \r\n"
        		+ "   NULL \"NFK_NB_DISTINCT_VALUES\"\r\n"
        		+ "FROM t_view_selection \r\n"
        		+ "INNER JOIN GET_FOREIGN_KEYS () t_fk\r\n"
        		+ "   ON t_fk.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_fk.input_view_name=t_view_selection.name\r\n"
        		+ "GROUP BY \r\n"
        		+ "   t_fk.fk_database_name, t_fk.fk_name, t_fk.fk_view_name, \r\n"
        		+ "   t_fk.pk_database_name, t_fk.pk_view_name\r\n"
        		+ "ORDER BY \r\n"
        		+ "   \"NTAB_CODE_CHILD\", \r\n"
        		+ "   \"NTAB_CODE_PARENT\", \r\n"
        		+ "   \"NFK_CODE\"", 
        		bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NFK_NCOL File
        	////////////////////////////////////////////////////////////////////////////////
            if(!bQuiet) println("Extracting NFK_NCOL meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nfk_ncol.csv"),
        		"WITH t_view_selection AS(\r\n"
        	    + sViewSelection + "\r\n"
        		+ ")\r\n"
        		+ "SELECT \r\n"
        		+ "   t_fk.fk_database_name || '.' || t_fk.fk_view_name \"NTAB_CODE_CHILD\", \r\n"
        		+ "   t_fk.fk_column_name \"NCOL_CODE_CHILD\", \r\n"
        		+ "   t_fk.fk_database_name || '.' || t_fk.fk_name \"NFK_CODE\", \r\n"
        		+ "   t_fk.pk_database_name || '.' || t_fk.pk_view_name \"NTAB_CODE_PARENT\", \r\n"
        		+ "   t_fk.pk_column_name \"NCOL_CODE_PARENT\"\r\n"
        		+ "FROM t_view_selection \r\n"
        		+ "INNER JOIN GET_FOREIGN_KEYS () t_fk\r\n"
        		+ "   ON t_fk.input_database_name=t_view_selection.database_name\r\n"
        		+ "   AND t_fk.input_view_name=t_view_selection.name\r\n"
        		+ "ORDER BY \r\n"
        		+ "   \"NTAB_CODE_CHILD\", \r\n"
        		+ "   \"NTAB_CODE_PARENT\", \r\n"
        		+ "   \"NFK_CODE\", \r\n"
        		+ "   \"NCOL_CODE_CHILD\"", 
        		bShowSQL, bQuiet);
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
		Executable executable = new ExtractDenodo(); 
		executable.doMain(args);
	}
}
