package com.acod_bi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** 
 * ACOD Link between connection parameters from executable parameter and SQL connection
 */
public class SQLConnection implements ExecutableChild {
	public SQLConnection()
	{
	}
	
    public void setExecutable(Executable executable) {
        executable.addExecutableChild(this);
    }
    
	public String sRdbmsId;
	public String sDriverClassName;
	public String sRdbmsName;
	
	public String sDbUrl;
	public String sUser;
	public String sPassword;

	public void executableSetParams(Executable executable) {
		executable.addParam("c", "Database url", false, "database url");
		executable.addParam("u", "Database user", false, "user");
		executable.addParam("p", "Database password", false, "password");
	}
	
	public void executableSetVariablesFromParameters(Executable executable) {
	    sDbUrl = executable.getParamValue("c");
	    if(sDbUrl == null)
	    {
	    	executable.exitWithParametersError("Missing parameter -c <database url>");
	    }
	    sUser = executable.getParamValue("u");
	    sPassword = executable.getParamValue("p");
	    
		if((sRdbmsId==null)||(sDriverClassName==null)||(sRdbmsName==null))
		{
			if(sDbUrl.toLowerCase().startsWith("jdbc:oracle:"))
			{
				if(sRdbmsId==null) 
					sRdbmsId = "ora";
				if(sDriverClassName==null) 
					sDriverClassName = "oracle.jdbc.OracleDriver";
				if(sRdbmsName==null) 
					sRdbmsName = "Oracle";
			}
			else if(sDbUrl.toLowerCase().startsWith("jdbc:vdb:"))
			{
				if(sRdbmsId==null) 
					sRdbmsId = "den";
				if(sDriverClassName==null) 
					sDriverClassName = "com.denodo.vdp.jdbc.Driver";
				if(sRdbmsName==null) 
					sRdbmsName = "Denodo";
			}
			else 
			{
				executable.printlnErr("Unhandled protocol in jdbc url " + sDbUrl);
				Executable.exitWithError();
			}
		}
	}
	
	public void executableShowParameters(Executable executable) {
		if(!executable.bQuiet) {
			if(sPassword == null)
				executable.println("Database Url : ###");
			else
				executable.println("Database Url : " + sDbUrl);
			if(sUser != null)
				executable.println("Database User: " + sUser);
			if(sPassword != null)
				executable.println("Database Password: ###");
		}
	}
	
	public void loadDriver(Executable executable) {
        if(!executable.bQuiet) executable.println("Loading " + sRdbmsName + " Driver ... ");
        try {
            Class.forName(sDriverClassName);
        } catch (Exception e) {
            throw new RuntimeException("Error loading " + sRdbmsName + " Driver ... ", e);
        	//-----executable.printlnErr("Error loading " + sRdbmsName + " Driver ... " + e.getMessage());
        	//-----Executable.exitWithError();
        }
	}
	
    public Connection getConnection(Executable executable) throws SQLException {
    	// Connecting to database
        if(!executable.bQuiet) executable.println("Connecting to database ... ");
    	return DriverManager.getConnection(sDbUrl, sUser, sPassword);
        }
	
}
