package gov.va.rxnorm.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Attributes;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Attributes extends BPT_Attributes
{
	public PT_Attributes()
	{
		addProperty("RXAUI", null, "Unique identifier for atom (RxNorm Atom Id)");  //loaded as an attribute and a id
		addProperty("SAUI", null, "Source asserted atom identifier");
		addProperty("SCUI", null, "Source asserted concept identifier");
		addProperty("SAB", null, "Source Vocabulary");
		addProperty("CODE", null, "\"Most useful\" source asserted identifier (if the source vocabulary has more than one identifier)" 
				+ ", or a RxNorm-generated source entry identifier (if the source vocabulary has none.)");
		addProperty("SUPPRESS");
		addProperty("tty_class");
		addProperty("STYPE", null, "The name of the column in RXNCONSO.RRF or RXNREL.RRF that contains the identifier to which the attribute is attached, e.g., CUI, AUI.");
		addProperty("ATUI", null, "Unique identifier for attribute");
		addProperty("SATUI", null, "Source asserted attribute identifier (optional - present if it exists)");
		addProperty("UMLSAUI");  //This property should be in RXNDOC, but it is currently missing - bug in the data
	}
}
