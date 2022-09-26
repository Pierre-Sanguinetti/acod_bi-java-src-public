package com.acod_bi.extract_oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.io.File;
import java.io.IOException;

import com.acod_bi.Executable;
import com.acod_bi.SQLConnection;
import com.acod_bi.SQLExec;

import java.util.Arrays;

public class ExtractOracle extends Executable {

	protected String getActionName() {
		return "meta-data extraction";
	}
	
	protected SQLConnection sqlConnection = new SQLConnection(this);
	
	boolean bShowSQL;
	String sDestDir;
	
	// Overload with specific options
	protected void setParams() {
		super.setParams();

		sqlConnection.executableSetParams();
		
	    addParam("showsql", "Show extract queries", false, null);
	    addParam("dd", "Destination directory where files are generated", false, "destination directory");
	}
	
	// Overload with specific parameters 
	protected void setVariablesFromParameters() {
		sqlConnection.executableSetVariablesFromParameters();
		if(!sqlConnection.sRdbmsId.equals("ora"))
		{
			this.printlnErr("Use this application only for Oracle");
			Executable.exitWithError();
		}
		
		bShowSQL = hasParam("showsql");
		sDestDir = getParamValue("dd", ".");
	}
	
	// Overload with specific parameters and options 
	protected void showParameters() {
		sqlConnection.executableShowParameters();
		if(!bQuiet) {
			println("Destination directory: \"" + sDestDir + "\"");
		}
		super.showParameters();
        if(bVerbose)
        {
	        if(bShowSQL) println("Show SQL");
        }
	}
	
	/*private*/ static double columnAvgNbChars(
		   double fNumRows,
		   String sDataType,
		   double fCharLen,
		   double fAvgColLen,
		   double fNumNulls,
		   double fNumDistincts
		)
		{
		   double fResult;
		   
		   if(fNumRows == 0)
		      fResult = -1; // NULL
		   else if(fNumRows-fNumNulls == 0)
		      fResult = 0;
		   else if(sDataType.equals("CHAR"))
		      fResult = fCharLen;
		   else if(sDataType.equals("NCHAR"))
		      fResult = fCharLen;
		   else if(sDataType.equals("VARCHAR"))
		      if(fCharLen <= 253)
		         fResult = ((((fAvgColLen-0.5)*fNumRows)-fNumNulls)/(fNumRows-fNumNulls))-1.0;
		      else
		         fResult = ((((fAvgColLen-0.5)*fNumRows)-fNumNulls)/(fNumRows-fNumNulls))-3.0;
		   else if(sDataType.equals("VARCHAR2"))
		      if(fCharLen <= 253)
		         fResult = ((((fAvgColLen-0.5)*fNumRows)-fNumNulls)/(fNumRows-fNumNulls))-1.0;
		      else
		         fResult = ((((fAvgColLen-0.5)*fNumRows)-fNumNulls)/(fNumRows-fNumNulls))-3.0;
		   else if(sDataType.equals("NVARCHAR2"))
		      fResult = (((((fAvgColLen-0.5)*fNumRows)-fNumNulls)/(fNumRows-fNumNulls))-1.0)/2.0;
		   else
		      fResult = -1; // NULL
		   return fResult;
	}

	/*private*/ static double columnAvgNbDigits(
		   double fNumRows,
		   String sDataType,
		   double fAvgColLen,
		   double fNumNulls,
		   double fNumDistincts
		   )
	{
		double fResult;
	   if( fNumRows == 0)
	      fResult = -1; //NULL
	   else if(fNumRows-fNumNulls == 0)
	      fResult = 0;
	   else if(Arrays.asList("NUMBER", "FLOAT").contains(sDataType))
	   {
		   double fAvgColLenNotNull;
	         fAvgColLenNotNull = ((fNumRows*(fAvgColLen-0.5))-(fNumNulls*1.0))/(fNumRows-fNumNulls);
	         fResult = (fAvgColLenNotNull-2.5)*2.0;
	         if( fResult < 1.0)
	            fResult = 1.0;
	         else if( fResult > 38.0)
	            fResult = 38.0;
	   }
	   else
	      fResult = -1;//NULL
	   return fResult;
	}
	
	// Overload with specific action
	protected void doAction() {
        // JDBC Driver load
		sqlConnection.loadDriver();
		
    	// Connecting to database
        try(Connection connection = sqlConnection.getConnection()){
        	////////////////////////////////////////////////////////////////////////////////
        	// NTAB File
        	////////////////////////////////////////////////////////////////////////////////
            println("Extracting NTAB meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_ntab.csv"), 
        		"SELECT TABLE_NAME NTAB_CODE, NUM_ROWS NTAB_NB_ROWS\r\n"
        		+ "   FROM USER_TABLES DWOC_TABLES\r\n"
        		+ "   WHERE SUBSTR(TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ "   ORDER BY TABLE_NAME", 
				bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NCOL File
        	////////////////////////////////////////////////////////////////////////////////
            println("Extracting NCOL meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_ncol.csv"),
        		//-----"SELECT NUM_ROWS, DATA_TYPE, CHAR_LENGTH, AVG_COL_LEN, NUM_NULLS, NUM_DISTINCT, \r\n"
        		"SELECT \r\n"
        		+ "   DWOC_TAB_COLUMNS.TABLE_NAME NTAB_CODE, \r\n"
        		+ "   DWOC_TAB_COLUMNS.COLUMN_NAME NCOL_CODE, \r\n"
        		+ "   DWOC_TAB_COLUMNS.COLUMN_ID NCOL_ORDER_NO, \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'CHAR' THEN 'A'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'NCHAR' THEN 'A'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'VARCHAR' THEN 'VA'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'VARCHAR2' THEN 'VA'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'NVARCHAR2' THEN 'VA'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'DATE' THEN 'DT'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE LIKE 'TIMESTAMP(%)' THEN 'DT'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'NUMBER' THEN 'N'\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'FLOAT' THEN 'F'\r\n"
        		+ "      -- WHEN DWOC_TAB_COLUMNS.DATA_TYPE = 'CLOB' THEN 'VA' -- Non implemente. Il faudra prendre en compte NUM_DISTINCT=0 et AVG_COL_LEN non significatif (une version degradee pourrait etre NUM_DISTINCT = NUM_ROWS et AVG_COL_LEN = 4000)\r\n"
        		+ "      ELSE 'UN' -- Unsupported\r\n"
        		+ "   END NCOL_DATA_TYPE, \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN DWOC_TAB_COLUMNS.DATA_TYPE LIKE 'TIMESTAMP(%)' THEN 'CAST(<dwh_column> AS DATE)'\r\n"
        		+ "      ELSE '<dwh_column>'\r\n"
        		+ "   END NCOL_CONVERT2DTM, \r\n"
        		+ "   DECODE(DWOC_TAB_COLUMNS.DATA_TYPE,\r\n"
        		+ "      'CHAR', DWOC_TAB_COLUMNS.CHAR_LENGTH,\r\n"
        		+ "      'NCHAR', DWOC_TAB_COLUMNS.CHAR_LENGTH,\r\n"
        		+ "      'VARCHAR', DWOC_TAB_COLUMNS.CHAR_LENGTH,\r\n"
        		+ "      'VARCHAR2', DWOC_TAB_COLUMNS.CHAR_LENGTH,\r\n"
        		+ "      'NVARCHAR2', DWOC_TAB_COLUMNS.CHAR_LENGTH,\r\n"
        		+ "      NULL) NCOL_MAX_NB_CHARS, \r\n"
        		//+ "   NULL NCOL_AVG_NB_CHARS, \r\n"
        		+ "      CASE \r\n"
        		+ "         WHEN NUM_ROWS = 0 THEN NULL\r\n"
        		+ "         WHEN (NUM_ROWS-NUM_NULLS) = 0 THEN 0\r\n"
        		+ "         WHEN DATA_TYPE = 'CHAR' THEN CHAR_LENGTH\r\n"
        		+ "         WHEN DATA_TYPE = 'NCHAR' THEN CHAR_LENGTH\r\n"
        		+ "         WHEN DATA_TYPE = 'VARCHAR' THEN\r\n"
        		+ "            CASE \r\n"
        		+ "               WHEN CHAR_LENGTH <= 253 THEN ((((AVG_COL_LEN-0.5)*NUM_ROWS)-NUM_NULLS)/(NUM_ROWS-NUM_NULLS))-1.0\r\n"
        		+ "               ELSE ((((AVG_COL_LEN-0.5)*NUM_ROWS)-NUM_NULLS)/(NUM_ROWS-NUM_NULLS))-3.0\r\n"
        		+ "            END\r\n"
        		+ "         WHEN DATA_TYPE = 'VARCHAR2' THEN\r\n"
        		+ "            CASE\r\n"
        		+ "               WHEN CHAR_LENGTH <= 253 THEN ((((AVG_COL_LEN-0.5)*NUM_ROWS)-NUM_NULLS)/(NUM_ROWS-NUM_NULLS))-1.0\r\n"
        		+ "               ELSE ((((AVG_COL_LEN-0.5)*NUM_ROWS)-NUM_NULLS)/(NUM_ROWS-NUM_NULLS))-3.0\r\n"
        		+ "            END\r\n"
        		+ "         WHEN DATA_TYPE = 'NVARCHAR2' THEN (((((AVG_COL_LEN-0.5)*NUM_ROWS)-NUM_NULLS)/(NUM_ROWS-NUM_NULLS))-1.0)/2.0\r\n"
        		+ "         ELSE NULL\r\n"
        		+ "      END NCOL_AVG_NB_CHARS, \r\n"
        		+ "   DECODE(DWOC_TAB_COLUMNS.DATA_TYPE,\r\n"
        		+ "      'NUMBER', DWOC_TAB_COLUMNS.DATA_PRECISION,\r\n"
        		+ "      'FLOAT', DWOC_TAB_COLUMNS.DATA_PRECISION,\r\n"
        		+ "      NULL) NCOL_MAX_NB_DIGITS, \r\n"
        		+ "   CASE\r\n"
        		+ "      WHEN NUM_ROWS = 0 THEN NULL\r\n"
        		+ "      WHEN NUM_ROWS-NUM_NULLS = 0 THEN 0\r\n"
        		+ "      WHEN DATA_TYPE IN ('NUMBER', 'FLOAT') THEN\r\n"
        		+ "         CASE\r\n"
        		+ "            WHEN (((((NUM_ROWS*(AVG_COL_LEN-0.5))-(NUM_NULLS*1.0))/(NUM_ROWS-NUM_NULLS))-2.5)*2.0) < 1.0 THEN 1.0\r\n"
        		+ "            WHEN (((((NUM_ROWS*(AVG_COL_LEN-0.5))-(NUM_NULLS*1.0))/(NUM_ROWS-NUM_NULLS))-2.5)*2.0) > 38.0 THEN 38.0\r\n"
        		+ "            ELSE (((((NUM_ROWS*(AVG_COL_LEN-0.5))-(NUM_NULLS*1.0))/(NUM_ROWS-NUM_NULLS))-2.5)*2.0)\r\n"
        		+ "         END\r\n"
        		+ "      ELSE NULL\r\n"
        		+ "   END NCOL_AVG_NB_DIGITS, \r\n"
        		+ "   DWOC_TAB_COLUMNS.DATA_SCALE NCOL_SCALE, \r\n"
        		+ "   DECODE(DWOC_TAB_COLUMNS.NULLABLE, 'Y', 0, 'N', 1) NCOL_MANDATORY, \r\n"
        		+ "   DWOC_TAB_COLUMNS.NUM_DISTINCT NCOL_NB_DISTINCT_VALUES, \r\n"
        		+ "   DWOC_TAB_COLUMNS.NUM_NULLS NCOL_NB_NULLS\r\n"
        		+ "FROM USER_TAB_COLUMNS DWOC_TAB_COLUMNS\r\n"
        		+ "INNER JOIN USER_TABLES DWOC_TABLES \r\n"
        		+ "   ON DWOC_TABLES.TABLE_NAME = DWOC_TAB_COLUMNS.TABLE_NAME\r\n"
        		+ "WHERE SUBSTR(DWOC_TABLES.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ "ORDER BY DWOC_TAB_COLUMNS.TABLE_NAME, DWOC_TAB_COLUMNS.COLUMN_ID", 
				bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NUK File
        	////////////////////////////////////////////////////////////////////////////////
            println("Extracting NUK meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nuk.csv"),
        		"SELECT \r\n"
        		+ "   NUK_CODE, \r\n"
        		+ "   TABLE_NAME NTAB_CODE, \r\n"
        		+ "   (SELECT COUNT(*) FROM DUAL WHERE EXISTS(\r\n"
        		+ "      SELECT 1 FROM USER_CONSTRAINTS DWOC_CONSTRAINTS\r\n"
        		+ "         WHERE DWOC_CONSTRAINTS.CONSTRAINT_NAME=NUK_CODE\r\n"
        		+ "         AND DWOC_CONSTRAINTS.CONSTRAINT_TYPE='P'\r\n"
        		+ "      )\r\n"
        		+ "   ) NUK_PRIMARY\r\n"
        		+ "FROM (\r\n"
        		+ "   SELECT DWOC_INDEXES.INDEX_NAME NUK_CODE, DWOC_INDEXES.TABLE_NAME\r\n"
        		+ "   FROM USER_INDEXES DWOC_INDEXES\r\n"
        		+ "   WHERE DWOC_INDEXES.UNIQUENESS = 'UNIQUE'\r\n"
        		+ "      AND DWOC_INDEXES.INDEX_TYPE <> 'LOB'\r\n"
        		+ "      AND SUBSTR(DWOC_INDEXES.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ "   UNION\r\n"
        		+ "   SELECT DWOC_CONSTRAINTS.CONSTRAINT_NAME NUK_CODE, DWOC_CONSTRAINTS.TABLE_NAME\r\n"
        		+ "   FROM USER_CONSTRAINTS DWOC_CONSTRAINTS\r\n"
        		+ "   WHERE DWOC_CONSTRAINTS.CONSTRAINT_TYPE IN ('U', 'P')\r\n"
        		+ "      AND SUBSTR(DWOC_CONSTRAINTS.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ ")\r\n"
        		+ "ORDER BY TABLE_NAME, NUK_CODE", 
				bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NUK_NCOL File
        	////////////////////////////////////////////////////////////////////////////////
            println("Extracting NUK_NCOL meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nuk_ncol.csv"),
        		"SELECT NUK_CODE, NTAB_CODE, NCOL_CODE, NUK_NCOL_ORDER_NO\r\n"
        		+ "FROM(\r\n"
        		+ "   SELECT DWOC_INDEXES.INDEX_NAME NUK_CODE, DWOC_INDEXES.TABLE_NAME NTAB_CODE, DWOC_IND_COLUMNS.COLUMN_NAME NCOL_CODE, DWOC_IND_COLUMNS.COLUMN_POSITION NUK_NCOL_ORDER_NO\r\n"
        		+ "   FROM USER_INDEXES DWOC_INDEXES\r\n"
        		+ "   INNER JOIN USER_IND_COLUMNS DWOC_IND_COLUMNS ON DWOC_IND_COLUMNS.INDEX_NAME = DWOC_INDEXES.INDEX_NAME\r\n"
        		+ "   WHERE DWOC_INDEXES.UNIQUENESS = 'UNIQUE'\r\n"
        		+ "      AND SUBSTR(DWOC_INDEXES.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ "   UNION\r\n"
        		+ "   SELECT DWOC_CONSTRAINTS.CONSTRAINT_NAME NUK_CODE, DWOC_CONSTRAINTS.TABLE_NAME NTAB_CODE, DWOC_CONS_COLUMNS.COLUMN_NAME NCOL_CODE, DWOC_CONS_COLUMNS.POSITION NUK_NCOL_ORDER_NO\r\n"
        		+ "   FROM USER_CONSTRAINTS DWOC_CONSTRAINTS\r\n"
        		+ "   INNER JOIN USER_CONS_COLUMNS DWOC_CONS_COLUMNS ON DWOC_CONS_COLUMNS.CONSTRAINT_NAME = DWOC_CONSTRAINTS.CONSTRAINT_NAME\r\n"
        		+ "   WHERE DWOC_CONSTRAINTS.CONSTRAINT_TYPE IN ('U', 'P')\r\n"
        		+ "      AND SUBSTR(DWOC_CONSTRAINTS.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ ")\r\n"
        		+ "ORDER BY NTAB_CODE, NUK_CODE, NUK_NCOL_ORDER_NO", 
    			bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NFK File
        	////////////////////////////////////////////////////////////////////////////////
            println("Extracting NFK meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nfk.csv"),
        		"SELECT\r\n"
        		+ "   DWOC_CONSTRAINTS.CONSTRAINT_NAME NFK_CODE, \r\n"
        		+ "   DWOC_CONSTRAINTS.TABLE_NAME NTAB_CODE_CHILD, \r\n"
        		+ "   DWOC_CONSTRAINTS_PARENT.TABLE_NAME NTAB_CODE_PARENT, \r\n"
        		+ "   CASE \r\n"
        		+ "      WHEN (SELECT COUNT(*) FROM USER_CONS_COLUMNS WHERE USER_CONS_COLUMNS.CONSTRAINT_NAME = DWOC_CONSTRAINTS.CONSTRAINT_NAME) = 1 \r\n"
        		+ "         THEN (SELECT NUM_DISTINCT FROM USER_TAB_COLUMNS, USER_CONS_COLUMNS WHERE USER_CONS_COLUMNS.CONSTRAINT_NAME = DWOC_CONSTRAINTS.CONSTRAINT_NAME\r\n"
        		+ "               AND USER_TAB_COLUMNS.TABLE_NAME = USER_CONS_COLUMNS.TABLE_NAME\r\n"
        		+ "               AND USER_TAB_COLUMNS.COLUMN_NAME = USER_CONS_COLUMNS.COLUMN_NAME)\r\n"
        		+ "      ELSE\r\n"
        		+ "         -- Indexe correspondant à la FK => nombre de valeurs de clés de l'index\r\n"
        		+ "         (SELECT MIN(DISTINCT_KEYS)\r\n"
        		+ "            FROM USER_CONSTRAINTS,\r\n"
        		+ "               USER_INDEXES\r\n"
        		+ "            WHERE USER_CONSTRAINTS.CONSTRAINT_NAME = DWOC_CONSTRAINTS.CONSTRAINT_NAME\r\n"
        		+ "               AND USER_INDEXES.TABLE_NAME = USER_CONSTRAINTS.TABLE_NAME\r\n"
        		+ "               AND NOT EXISTS(\r\n"
        		+ "                  SELECT USER_CONS_COLUMNS.COLUMN_NAME\r\n"
        		+ "                     FROM USER_CONS_COLUMNS\r\n"
        		+ "                     WHERE USER_CONS_COLUMNS.CONSTRAINT_NAME = USER_CONSTRAINTS.CONSTRAINT_NAME\r\n"
        		+ "                  MINUS\r\n"
        		+ "                  SELECT USER_IND_COLUMNS.COLUMN_NAME\r\n"
        		+ "                     FROM USER_IND_COLUMNS\r\n"
        		+ "                     WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME\r\n"
        		+ "                  )\r\n"
        		+ "               AND NOT EXISTS(\r\n"
        		+ "                  SELECT USER_IND_COLUMNS.COLUMN_NAME\r\n"
        		+ "                     FROM USER_IND_COLUMNS\r\n"
        		+ "                     WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME\r\n"
        		+ "                  MINUS\r\n"
        		+ "                  SELECT USER_CONS_COLUMNS.COLUMN_NAME\r\n"
        		+ "                     FROM USER_CONS_COLUMNS\r\n"
        		+ "                     WHERE USER_CONS_COLUMNS.CONSTRAINT_NAME = USER_CONSTRAINTS.CONSTRAINT_NAME\r\n"
        		+ "                  )\r\n"
        		+ "               -- Il ne doit pas y  avoir de colonne de l'index avec des nuls sinon Oracle décomptera des clés\r\n"
        		+ "               -- pour des n-uplet avec des nulls dans certaines colonnes\r\n"
        		+ "               AND NOT EXISTS(\r\n"
        		+ "                  SELECT 1\r\n"
        		+ "                     FROM USER_IND_COLUMNS, USER_TAB_COLUMNS\r\n"
        		+ "                     WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME\r\n"
        		+ "                     AND USER_TAB_COLUMNS.TABLE_NAME = USER_INDEXES.TABLE_NAME\r\n"
        		+ "                     AND USER_TAB_COLUMNS.COLUMN_NAME = USER_IND_COLUMNS.COLUMN_NAME\r\n"
        		+ "                     AND (USER_TAB_COLUMNS.NUM_NULLS IS NULL OR USER_TAB_COLUMNS.NUM_NULLS > 0)\r\n"
        		+ "                  )\r\n"
        		+ "         )\r\n"
        		+ "   END NFK_NB_DISTINCT_VALUES\r\n"
        		+ "FROM\r\n"
        		+ "   USER_CONSTRAINTS DWOC_CONSTRAINTS\r\n"
        		+ "   INNER JOIN USER_CONSTRAINTS DWOC_CONSTRAINTS_PARENT\r\n"
        		+ "      ON DWOC_CONSTRAINTS_PARENT.CONSTRAINT_NAME = DWOC_CONSTRAINTS.R_CONSTRAINT_NAME\r\n"
        		+ "WHERE\r\n"
        		+ "   DWOC_CONSTRAINTS.CONSTRAINT_TYPE = 'R'\r\n"
        		+ "   AND SUBSTR(DWOC_CONSTRAINTS.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ "ORDER BY \r\n"
        		+ "   DWOC_CONSTRAINTS.TABLE_NAME,\r\n"
        		+ "   DWOC_CONSTRAINTS.CONSTRAINT_NAME", 
				bShowSQL, bQuiet);
            
        	////////////////////////////////////////////////////////////////////////////////
        	// NFK_NCOL File
        	////////////////////////////////////////////////////////////////////////////////
            println("Extracting NFK_NCOL meta-data ... ");
            SQLExec.extractQuery2CSV(this, connection, 
            	new File(sDestDir + "\\exc_nfk_ncol.csv"),
        		"SELECT\r\n"
        		+ "   DWOC_CONSTRAINTS.TABLE_NAME NTAB_CODE_CHILD, \r\n"
        		+ "   DWOC_CONS_COLUMNS.COLUMN_NAME NCOL_CODE_CHILD, \r\n"
        		+ "   DWOC_CONSTRAINTS.CONSTRAINT_NAME NFK_CODE, \r\n"
        		+ "   DWOC_CONSTRAINTS_PARENT.TABLE_NAME NTAB_CODE_PARENT, \r\n"
        		+ "   DWOC_CONS_COLUMNS_PARENT.COLUMN_NAME NCOL_CODE_PARENT\r\n"
        		+ "FROM USER_CONSTRAINTS DWOC_CONSTRAINTS\r\n"
        		+ "INNER JOIN USER_CONS_COLUMNS DWOC_CONS_COLUMNS\r\n"
        		+ "   ON DWOC_CONS_COLUMNS.CONSTRAINT_NAME = DWOC_CONSTRAINTS.CONSTRAINT_NAME\r\n"
        		+ "INNER JOIN USER_CONSTRAINTS DWOC_CONSTRAINTS_PARENT\r\n"
        		+ "   ON DWOC_CONSTRAINTS_PARENT.CONSTRAINT_NAME = DWOC_CONSTRAINTS.R_CONSTRAINT_NAME\r\n"
        		+ "INNER JOIN USER_CONS_COLUMNS DWOC_CONS_COLUMNS_PARENT\r\n"
        		+ "   ON DWOC_CONS_COLUMNS_PARENT.CONSTRAINT_NAME = DWOC_CONSTRAINTS_PARENT.CONSTRAINT_NAME\r\n"
        		+ "   AND DWOC_CONS_COLUMNS.POSITION = DWOC_CONS_COLUMNS_PARENT.POSITION\r\n"
        		+ "WHERE SUBSTR(DWOC_CONSTRAINTS.TABLE_NAME, 1, 4) <> 'BIN$'\r\n"
        		+ "ORDER BY \r\n"
        		+ "   DWOC_CONSTRAINTS.TABLE_NAME,\r\n"
        		+ "   DWOC_CONSTRAINTS.CONSTRAINT_NAME,\r\n"
        		+ "   DWOC_CONS_COLUMNS.COLUMN_NAME", 
				bShowSQL, bQuiet);
        } catch (SQLException e) {
            //println("Database Error : " + e.getMessage());
            throw new RuntimeException("Database Error", e);
            //bExitWithError = true;
        }
        catch (IOException e) {
            //e.printStackTrace();
            throw new RuntimeException("IOException", e);
        }
        println("Database closed ... ");
		
	}
	
	public static void main(String[] args) {
		Executable executable = new ExtractOracle(); 
		executable.doMain(args);
	}
}
