package com.acod_bi;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * ACOD Query avec parametres nommes ou anonymes
 * Ne fonctionne qu'avec des paremetres de type String 
 */
public class QueryWithParameters 
{
    String anonymousStatement;
    public String getAnonymousStatement() {
        return anonymousStatement;
    }

    Vector<String> anonymousQueryParams;
    
    QueryWithParameters(String anonymousStatement, Vector<String> anonymousQueryParams)
    {
        this.anonymousStatement = anonymousStatement;
        this.anonymousQueryParams = anonymousQueryParams;
    }
    
    /**
    * Construit une Query avec parametres anonymes a partir d'une query avec parametre nommes references par ${nom_parametre} 
    * 
    */
    public QueryWithParameters(String namedStatement, Map<String, String> namedQueryParams, Pattern patternRefVar)
    {
        if(patternRefVar == null)
            // \${(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)}
            patternRefVar = Pattern.compile("\\$\\{(?<varname>[a-zA-Z_][a-zA-Z0-9_]*)\\}");
            
        anonymousQueryParams = new Vector<String>();
        Matcher matcher = patternRefVar.matcher(namedStatement);
        
        String varName;
        String stringValue;
        int iPos = 0;
        while(matcher.find(iPos))
        {
            varName = matcher.group("varname");
            stringValue = namedQueryParams.get(varName);
            anonymousQueryParams.add(stringValue);
            iPos = matcher.end();
        }
        
        anonymousStatement = patternRefVar.matcher(namedStatement).replaceAll("?");
    }
    
    /**
    * Retourne une String decrivant la query et ses parametres a utiliser dans les logs
    * 
    */
    public String getQueryStatementAndParamsLog()
    {
        String queryStatementAndParamsLog;
        if((anonymousQueryParams == null) || anonymousQueryParams.isEmpty())
        {
            queryStatementAndParamsLog = anonymousStatement + ";";
            queryStatementAndParamsLog = queryStatementAndParamsLog + "\nno parameters";
        }
        else
        {
            queryStatementAndParamsLog = anonymousStatement + ";"; 
            int iParamPos = 1; 
            for (String sParamValue : anonymousQueryParams) {
                queryStatementAndParamsLog = queryStatementAndParamsLog + "\n@p" + String.valueOf(iParamPos) + "='" + sParamValue + "'";
                ++iParamPos;
            }               
        }
        return queryStatementAndParamsLog;
    }
    
    /**
    * Positionne les parametres pour un statement prepare au prealable
    * 
    */
    public void setPreparedStatementParameters(PreparedStatement preparedStatement) throws SQLException
    {
        if(anonymousQueryParams != null)
        {
            int iParamPos = 1; 
            for (String sParamValue : anonymousQueryParams) {
                preparedStatement.setString(iParamPos, sParamValue);
                ++iParamPos;
            }               
        }
    }
}