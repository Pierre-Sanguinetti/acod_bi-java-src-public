package com.acod_bi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** 
 * ACOD Link between connection parameters from executable parameter and double SQL connection (Denodo and direct connection to datamart database)
 */
public class SQLDoubleConnection {
	public SQLDoubleConnection(){}
	
	public SQLDoubleConnection(Executable newExecutable)
	{
		SetExecutable(newExecutable);
	}
	
	protected Executable executable;
	public void SetExecutable(Executable newExecutable) { executable = newExecutable; }
	
	public String sDenRdbmsId = "den";
	public String sDenDriverClassName = "com.denodo.vdp.jdbc.Driver";
	public String sDenRdbmsName = "Denodo";
	
	public String sDenDbUrl;
	public String sDenUser;
	public String sDenPassword;
	
	public String sDtmRdbmsId;
	public String sDtmDriverClassName;
	public String sDtmRdbmsName;

	public String sDtmDbUrl;
	public String sDtmUser;
	public String sDtmPassword;
	
	public void executableSetParams() {
		executable.addParam("dc", "Denodo database url", true, "Denodo database url");
		executable.addParam("du", "Denodo user", false, "Denodo user");
		executable.addParam("dp", "Denodo password", false, "Denodo password");
		executable.addParam("tc", "Target database url", true, "Target database url");
		executable.addParam("tu", "Target user", false, "Target user");
		executable.addParam("tp", "Target password", false, "Target password"); 
	}
	
	public void executableSetVariablesFromParameters() {
	    sDenDbUrl = executable.getParamValue("dc");
	    if(sDenDbUrl == null)
	    {
	    	executable.exitWithParametersError("Missing parameter -dc");
	    }
	    sDenUser = executable.getParamValue("du");
	    sDenPassword = executable.getParamValue("dp");
	    
	    sDtmDbUrl = executable.getParamValue("tc");
	    if(sDtmDbUrl == null)
	    {
	    	executable.exitWithParametersError("Missing parameter -tc");
	    }
		sDtmUser = executable.getParamValue("tu");
		sDtmPassword = executable.getParamValue("tp");
		
		if((sDtmRdbmsId==null)||(sDtmDriverClassName==null)||(sDtmRdbmsName==null))
		{
			if(sDtmDbUrl.toLowerCase().startsWith("jdbc:oracle:"))
			{
				if(sDtmRdbmsId==null) 
					sDtmRdbmsId = "ora";
				if(sDtmDriverClassName==null) 
					sDtmDriverClassName = "oracle.jdbc.OracleDriver";
				if(sDtmRdbmsName==null) 
					sDtmRdbmsName = "Oracle";
			}
			else 
			{
				executable.printlnErr("Unhandled protocol in jdbc url " + sDtmDbUrl);
				Executable.exitWithError();
			}
		}
	}
	
	public void executableShowParameters() {
		if(!executable.bQuiet) {
			if(sDenPassword == null)
				executable.println("Denodo Database Url : ###");
			else
				executable.println("Denodo Database Url : " + sDenDbUrl);
			if(sDenUser != null)
				executable.println("Denodo User: " + sDenUser);
			if(sDenPassword != null)
				executable.println("Denodo Password: ###");
			
			executable.println("Datamart RDBMS : " + sDtmRdbmsName);
			if(sDtmPassword == null)
				executable.println("Datamart Database Url : ###");
			else
				executable.println("Datamart Database Url : " + sDtmDbUrl);
			if(sDtmUser != null)
				executable.println("Datamart User: " + sDtmUser);
			if(sDtmPassword != null)
				executable.println("Datamart Password: ###");
		}
	}
	
	public void loadDriver() {
        if(!executable.bQuiet) executable.println("Loading " + sDenRdbmsName + " Driver ... ");
        try {
            Class.forName(sDenDriverClassName);
        } catch (Exception e) {
        	executable.printlnErr("Error loading " + sDenRdbmsName + " Driver ... " + e.getMessage());
			Executable.exitWithError();
        }

        if(!executable.bQuiet) executable.println("Loading " + sDtmRdbmsName + " Driver ... ");
        try {
            Class.forName(sDtmDriverClassName);
        } catch (Exception e) {
        	executable.printlnErr("Error loading " + sDtmRdbmsName + " Driver ... " + e.getMessage());
        	Executable.exitWithError();
        }
	}
	
    public Connection getDenConnection() throws SQLException {
        if(!executable.bQuiet) executable.println("Connecting to Denodo database ... ");
    	return DriverManager.getConnection(sDenDbUrl, sDenUser, sDenPassword);
        }
	
    public Connection getDtmConnection() throws SQLException {
        if(!executable.bQuiet) executable.println("Connecting directly to datamart database ... ");
    	return DriverManager.getConnection(sDtmDbUrl, sDtmUser, sDtmPassword);
        }
	
}
