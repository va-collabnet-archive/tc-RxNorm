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
		addProperty("STYPE1", null, "The name of the column in RXNCONSO.RRF that contains the identifier used for the first concept or first atom in source of the relationship (e.g., 'AUI' or 'CUI')");
		addProperty("STYPE2", null, "The name of the column in RXNCONSO.RRF that contains the identifier used for the second concept or second atom in the source of the relationship (e.g., 'AUI' or 'CUI')");
		addProperty("SATUI", null, "Source asserted attribute identifier (optional - present if it exists)");
		addProperty("STN", "Semantic Type tree number", null);
		addProperty("STY", "Semantic Type", null);
		addProperty("CVF", null, "Content View Flag. Bit field used to flag rows included in Content View.");//note - this is undocumented in RxNorm - used on the STY table - description comes from UMLS
		addProperty("URI");
		addProperty("RG", null, "Machine generated and unverified indicator");
		addProperty("Generic rel type", null, "Generic rel type for this relationship");
	}
}
