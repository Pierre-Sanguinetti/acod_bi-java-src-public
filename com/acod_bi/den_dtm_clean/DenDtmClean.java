package com.acod_bi.den_dtm_clean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.acod_bi.Executable;
import com.acod_bi.SQLDoubleConnection;
import com.acod_bi.SQLExec;

/** 
 * ACOD Drop all composants of a Denodo datamart
 */
public class DenDtmClean extends Executable {

	protected SQLDoubleConnection sqlDoubleConnection = new SQLDoubleConnection(this);
	
	boolean bShowSQL;
	String sDatamartBVPrefix;
	String sDatamartTablePrefix;
	String sDatamartTableSuffix;
	
	protected void setParams() {
		super.setParams();
		
	    addParam("showsql", "Show queries", false, null);
	    addParam("dbvp", "Datamart Base Views Prefix", false, "Datamart Base Views Prefix");
	    addParam("dtabp", "Datamart Tables Prefix", false, "Datamart Tables Prefix");
	    addParam("dtabs", "Datamart Tables Suffix", false, "Datamart Tables Suffix");
	    
	    sqlDoubleConnection.executableSetParams();
	}
	
	protected void setVariablesFromParameters() {
		super.setVariablesFromParameters();
		
		bShowSQL = hasParam("showsql");
		sDatamartBVPrefix = getParamValue("dbvp");
		sDatamartTablePrefix = getParamValue("dtabp");;
		sDatamartTableSuffix = getParamValue("dtabs");;;
		
		sqlDoubleConnection.executableSetVariablesFromParameters();
	}
	
	protected void showParameters() {
		if(!bQuiet) {
			if(sDatamartBVPrefix != null)
				println("Datamart BV prefix: \"" + sDatamartBVPrefix + "\"");
			else
				println("Datamart BV prefix:<none>");
			sqlDoubleConnection.executableShowParameters();
		}
		super.showParameters();
        if(bVerbose)
        {
	        if(bShowSQL) println("Show SQL");
        }
	}
	
	protected String getActionName() {
		return "Create datamart";
	}
	
	protected void dropObject(Connection conn, String sObjectType, String sObjectName, String sDropStatement)  
	{
		if(sDropStatement==null)
			sDropStatement = "DROP " + sObjectType.toUpperCase() + " " + sObjectName;
		
    	if(!bQuiet)
    	{
			println("Drop " + sObjectType + " " + sObjectName);
    	}
    	try {
    		SQLExec.execQuery(this, conn, sDropStatement, null, bShowSQL, bQuiet, true, true);
    	}
        catch (SQLException e) {
            throw new RuntimeException("Database error", e);
        } 
	}
	
	protected void dropObjectsFromRequest(Connection conn, String sObjectType, String sTitle, String sGetObjectsStatement)
	{
		if(sTitle==null)
			sTitle = "Drop datamart " + sObjectType + "(s)"; 
    	
		if(!bQuiet)
    	{
			println("--------------------------------------------------------------------------------");
			println(sTitle);
			println("--------------------------------------------------------------------------------");
    	}
		if(bShowSQL) {
			println("Query:");
			println(sGetObjectsStatement + ";");
		}
        // Prepare and execute statement
		try (PreparedStatement getObjectsStatement = conn.prepareStatement(sGetObjectsStatement)){
			getObjectsStatement.execute();
	        ResultSet getObjectsResultSet = getObjectsStatement.getResultSet();
	        if ((getObjectsResultSet != null)) 
	        {
	            ResultSetMetaData metadata = getObjectsResultSet.getMetaData();
	            int iNbCols = metadata.getColumnCount();
	            String sObjectName;
	            String sDropStatement = null;
	            while (getObjectsResultSet.next()) {
    	        	sObjectName = getObjectsResultSet.getString(1);
    	        	if(iNbCols >= 2)
    	        		sDropStatement = getObjectsResultSet.getString(2);
    	        	dropObject(conn, sObjectType, sObjectName, sDropStatement);
	            }
	        }
		}
        catch (SQLException e) {
        	throw new RuntimeException("Database error in query : " + sGetObjectsStatement, e);
        }
	}
	
	protected void dropDenBaseView(Connection denConn, String sObjectName)  
	{
    	if(!bQuiet)
    	{
			println("Drop view " + sObjectName);
    	}
    	try {
    		SQLExec.execQuery(this, denConn, "DROP VIEW " + sObjectName, null, bShowSQL, bQuiet, true, true);
    		SQLExec.execQuery(this, denConn, "DROP WRAPPER JDBC " + sObjectName, null, bShowSQL, bQuiet, true, true);
    	}
        catch (SQLException e) {
            throw new RuntimeException("Database error", e);
        } 
	}
	
	protected void dropDenBaseViewsFromRequest(Connection denConn, String sTitle, String sGetObjectsStatement)
	{
    	if(!bQuiet)
    	{
			println("--------------------------------------------------------------------------------");
			println(sTitle);
			println("--------------------------------------------------------------------------------");
    	}
		if(bShowSQL) {
			println("Query:");
			println(sGetObjectsStatement + ";");
		}
        // Prepare and execute statement
		try (PreparedStatement getObjectsstatement = denConn.prepareStatement(sGetObjectsStatement)){
			getObjectsstatement.execute();
	        ResultSet getObjectsResultSet = getObjectsstatement.getResultSet();
	        if ((getObjectsResultSet != null)) 
	        {
	            while (getObjectsResultSet.next()) {
    	        	String sObjectName = getObjectsResultSet.getObject(1).toString();
    	        	dropDenBaseView(denConn, sObjectName);
	            }
	        }
		}
        catch (SQLException e) {
        	throw new RuntimeException("Database error in query : " + sGetObjectsStatement, e);
        }
	}
	
	protected void doAction() {
		sqlDoubleConnection.loadDriver();
		
    	// Create Connection to database
        try(
        		Connection denConn = sqlDoubleConnection.getDenConnection(); 
        		Connection dtmConn = sqlDoubleConnection.getDtmConnection(); 
        )
        {
        	String sGetObjectsStatement;
        	
        	// drop datamart summaries
        	sGetObjectsStatement = "select name from GET_VIEWS()\r\n"
        			+ "WHERE input_database_name=GETSESSION('database')\r\n"
        			+ "  AND folder='/01 - connectivity/02 - base views/acod-bi-internal'\r\n"
        			+ "  AND view_type=4";
        	dropDenBaseViewsFromRequest(denConn, "Drop datamart summaries", sGetObjectsStatement);
        	
        	// drop report views
        	sGetObjectsStatement = "select name from GET_VIEWS()\r\n"
        			+ "WHERE input_database_name=GETSESSION('database')\r\n";
        	if(sDatamartBVPrefix != null)
        		sGetObjectsStatement += "  AND substr(name, 1, len('" + sDatamartBVPrefix + "')) ='" + sDatamartBVPrefix + "'\r\n";  
			sGetObjectsStatement +=  "  AND folder='/04 - report views'";
        	dropDenBaseViewsFromRequest(denConn, "Drop report views", sGetObjectsStatement);
        	
        	// drop table views
        	sGetObjectsStatement = "select name from GET_VIEWS()\r\n"
        			+ "WHERE input_database_name=GETSESSION('database')\r\n"
        			+ "AND folder='/01 - connectivity/02 - base views/acod-bi-internal'\r\n"
        			+ "AND view_type=0";
        	dropDenBaseViewsFromRequest(denConn, "Drop table views", sGetObjectsStatement);
        	
        	if(sqlDoubleConnection.sDtmRdbmsId.equals("ora"))
        	{
            	// Drop datamart views (FV AV)
            	sGetObjectsStatement = 
            		"SELECT OBJECT_NAME\r\n"
					+ "FROM USER_OBJECTS\r\n"
					+ "WHERE REGEXP_LIKE(OBJECT_NAME, '^" + sDatamartTablePrefix + "(FV_|AV_)')\r\n"
					+ "AND REGEXP_LIKE(OBJECT_NAME, '" + sDatamartTableSuffix + "$')\r\n"
					+ "AND OBJECT_TYPE = 'VIEW'";
            	dropObjectsFromRequest(dtmConn, "View", "Drop datamart views (FV AV)", sGetObjectsStatement);
            	
            	// Drop datamart fact tables (FT, FM)
            	sGetObjectsStatement = 
					"SELECT TABLE_NAME\r\n"
					+ "FROM USER_TABLES\r\n"
					+ "WHERE REGEXP_LIKE(USER_TABLES.TABLE_NAME, '^" + sDatamartTablePrefix + "(FM_|FT_)')\r\n"
					+ "AND REGEXP_LIKE(USER_TABLES.TABLE_NAME, '" + sDatamartTableSuffix + "$')";
            	dropObjectsFromRequest(dtmConn, "Table", "Drop datamart fact tables (FT, FM)", sGetObjectsStatement);

            	// Drop datamart Dimensions (DI)
            	sGetObjectsStatement = 
					"SELECT DIMENSION_NAME\r\n"
					+ "FROM USER_DIMENSIONS\r\n"
					+ "WHERE REGEXP_LIKE(DIMENSION_NAME, '^" + sDatamartTablePrefix + "DI_')\r\n"
					+ "AND REGEXP_LIKE(DIMENSION_NAME, '" + sDatamartTableSuffix + "$')";
            	dropObjectsFromRequest(dtmConn, "Dimension", "Drop datamart Dimensions (DI)", sGetObjectsStatement);
            	
            	// Drop datamart Dimension tables (DM, DT, LT)
            	sGetObjectsStatement = 
					"SELECT TABLE_NAME\r\n"
					+ "FROM USER_TABLES\r\n"
					+ "WHERE REGEXP_LIKE(TABLE_NAME, '^" + sDatamartTablePrefix + "(DT_|DM_|DX_|LT_)')\r\n"
					+ "AND REGEXP_LIKE(TABLE_NAME, '" + sDatamartTableSuffix + "$')";
            	dropObjectsFromRequest(dtmConn, "Table", "Drop datamart Dimension tables (DM, DT, LT)", sGetObjectsStatement);
        	}
        	else
        	{
                throw new RuntimeException("Unhandled DtmRdbmsId " + sqlDoubleConnection.sDtmRdbmsId);
        	}
        // Close Connection to database
        }
        catch (SQLException e) {
            //println("Database Error : " + e.getMessage());
            throw new RuntimeException("Database Error", e);
            //bExitWithError = true;
        } 
        if(!bQuiet) println("Database closed ... ");
	}
	
	public static void main(String[] args) {
		Executable executable = new DenDtmClean();
		executable.doMain(args);
	}

}
