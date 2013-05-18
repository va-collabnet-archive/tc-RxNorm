package gov.va.rxnorm.rrf;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RXNSAT
{
	public String rxcui, rxaui, stype, code, atui, satui, atn, sab, atv, suppress, cvf;

	public RXNSAT(ResultSet rs) throws SQLException
	{
		rxcui = rs.getString("RXCUI");
		rxaui = rs.getString("RXAUI");
		stype = rs.getString("STYPE");
		code = rs.getString("CODE");
		atui = rs.getString("ATUI");
		satui = rs.getString("SATUI");
		atn = rs.getString("ATN");
		sab = rs.getString("SAB");
		atv = rs.getString("ATV");
		suppress = rs.getString("SUPPRESS");
		cvf = rs.getString("CVF");
	}
}
