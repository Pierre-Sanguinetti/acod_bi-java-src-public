package com.acod_bi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** 
 * ACOD Link between connection parameters from executable parameter and double SQL connection (Denodo and direct connection to target database)
 */
public class SQLDoubleConnection implements ExecutableChild {
	public SQLDoubleConnection()
	{
	}
	
	public String sDenRdbmsId = "den";
	public String sDenDriverClassName = "com.denodo.vdp.jdbc.Driver";
	public String sDenRdbmsName = "Denodo";
	
	public String sDenDbUrl;
	public String sDenUser;
	public String sDenPassword;
	
	public String sRdbmsId;
	public String sRdbDriverClassName;
	public String sRdbmsName;

	public String sRdbUrl;
	public String sRdbUser;
	public String sRdbPassword;
	
    public void setExecutable(Executable executable) {
        executable.addExecutableChild(this);
    }
    
	public void executableSetParams(Executable executable) {
		executable.addParam("dc", "Denodo database url", true, "Denodo database url");
		executable.addParam("du", "Denodo user", false, "Denodo user");
		executable.addParam("dp", "Denodo password", false, "Denodo password");
		executable.addParam("tc", "Target database url", true, "Target database url");
		executable.addParam("tu", "Target user", false, "Target user");
		executable.addParam("tp", "Target password", false, "Target password"); 
	}
	
	public void executableSetVariablesFromParameters(Executable executable) {
	    sDenDbUrl = executable.getParamValue("dc");
	    if(sDenDbUrl == null)
	    {
	    	executable.exitWithParametersError("Missing parameter -dc");
	    }
	    sDenUser = executable.getParamValue("du");
	    sDenPassword = executable.getParamValue("dp");
	    
	    sRdbUrl = executable.getParamValue("tc");
	    if(sRdbUrl == null)
	    {
	    	executable.exitWithParametersError("Missing parameter -tc");
	    }
		sRdbUser = executable.getParamValue("tu");
		sRdbPassword = executable.getParamValue("tp");
		
		if((sRdbmsId==null)||(sRdbDriverClassName==null)||(sRdbmsName==null))
		{
			if(sRdbUrl.toLowerCase().startsWith("jdbc:oracle:"))
			{
				if(sRdbmsId==null) 
					sRdbmsId = "ora";
				if(sRdbDriverClassName==null) 
					sRdbDriverClassName = "oracle.jdbc.OracleDriver";
				if(sRdbmsName==null) 
					sRdbmsName = "Oracle";
			}
			else 
			{
			    if(sRdbUrl.toLowerCase().startsWith("jdbc:postgresql:"))
	            {
	                if(sRdbmsId==null) 
	                    sRdbmsId = "pg";
	                if(sRdbDriverClassName==null) 
	                    sRdbDriverClassName = "org.postgresql.Driver";
	                if(sRdbmsName==null) 
	                    sRdbmsName = "PostgreSQL";
	            }
			    else
			    {
    				executable.printlnErr("Unhandled protocol in jdbc url " + sRdbUrl);
    				Executable.exitWithError();
			    }
			}
		}
	}
	
	public void executableShowParameters(Executable executable) {
		if(!executable.bQuiet) {
			if(sDenPassword == null)
				executable.println("Denodo Database Url : ###");
			else
				executable.println("Denodo Database Url : " + sDenDbUrl);
			if(sDenUser != null)
				executable.println("Denodo User: " + sDenUser);
			if(sDenPassword != null)
				executable.println("Denodo Password: ###");
			
			executable.println("RDBMS : " + sRdbmsName);
			if(sRdbPassword == null)
				executable.println("RDB Url : ###");
			else
				executable.println("RDB Url : " + sRdbUrl);
			if(sRdbUser != null)
				executable.println("RDB User: " + sRdbUser);
			if(sRdbPassword != null)
				executable.println("RDB Password: ###");
		}
	}
	
	public void loadDriver(Executable executable) {
        if(!executable.bQuiet) executable.println("Loading " + sDenRdbmsName + " Driver ... ");
        try {
            Class.forName(sDenDriverClassName);
        } catch (Exception e) {
        	executable.printlnErr("Error loading " + sDenRdbmsName + " Driver ... " + e.getMessage());
			Executable.exitWithError();
        }

        if(!executable.bQuiet) executable.println("Loading " + sRdbmsName + " Driver ... ");
        try {
            Class.forName(sRdbDriverClassName);
        } catch (Exception e) {
        	executable.printlnErr("Error loading " + sRdbmsName + " Driver ... " + e.getMessage());
        	Executable.exitWithError();
        }
	}
	
    public Connection getDenConnection(Executable executable) throws SQLException {
        if(!executable.bQuiet) executable.println("Connecting to Denodo database ... ");
    	return DriverManager.getConnection(sDenDbUrl, sDenUser, sDenPassword);
        }
	
    public Connection getDtmConnection(Executable executable) throws SQLException {
        if(!executable.bQuiet) executable.println("Connecting directly to relationnal database ... ");
    	return DriverManager.getConnection(sRdbUrl, sRdbUser, sRdbPassword);
        }
	
}
