package com.acod_bi;

/** 
 * ACOD Link between parameters from executable and other components
 */
public interface ExecutableChild {
    public void setExecutable(Executable executable);
    // example : public void setExecutable(Executable executable) {executable.addExecutableChild(this); }
	
	public void executableSetParams(Executable executable); 
		// to overload
		// example : executable.addParam("c", "Database url", false, "database url");
	
	
	public void executableSetVariablesFromParameters(Executable executable);
		// to overload
		// example : sDbUrl = executable.getParamValue("c");
	
	
	public void executableShowParameters(Executable executable);
		// to overload
		// example : if(!executable.bQuiet) {if(sUser != null) executable.println("Database User: " + sUser);}
	
}
