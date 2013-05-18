package gov.va.rxnorm.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_SAB_Metadata extends PropertyType
{
	public PT_SAB_Metadata()
	{
		//from http://www.nlm.nih.gov/research/umls/rxnorm/docs/2013/rxnorm_doco_full_2013-2.html#s12_8
		super("Source Vocabulary Metadata");
		addProperty("VCUI","CUI","CUI of the versioned SRC concept for a source");
		addProperty("RCUI","Root CUI","CUI of the root SRC concept for a source");
		addProperty("VSAB","Versioned Source Abbreviation","The versioned source abbreviation for a source, e.g., NDDF_2004_11_03");
		addProperty("RSAB","Root Source Abbreviation","The root source abbreviation, for a source e.g. NDDF");
		addProperty("SON","Official Name","The official name for a source");
		addProperty("SF","Source Family","The Source Family for a source");
		addProperty("SVER","Version","The source version, e.g., 2001");
		addProperty("VSTART","Meta Start Date","The date a source became active, e.g., 2001_04_03");
		addProperty("VEND","Meta End Date","The date a source ceased to be active, e.g., 2001_05_10");
		addProperty("IMETA","Meta Insert Version","The version of the Metathesaurus a source first appeared, e.g., 2001AB");
		addProperty("RMETA","Meta Remove Version","The version of the Metathesaurus a source was removed, e.g., 2001AC");
		addProperty("SLC","Source License Contact","The source license contact information. A semi-colon separated list containing the following fields: Name; Title; Organization; Address 1; Address 2; City; State or Province; Country; Zip or Postal Code; Telephone; Contact Fax; Email; URL");
		addProperty("SCC","Source Content Contact","The source content contact information. A semi-colon separated list containing the following fields: Name; Title; Organization; Address 1; Address 2; City; State or Province; Country; Zip or Postal Code; Telephone; Contact Fax; Email; URL");
		addProperty("SRL","Source Restriction Level","0,1,2,3,4 - explained in the License Agreement.");
		addProperty("TFR","Term Frequency","The number of terms for this source in RXNCONSO.RRF, e.g., 12343 (not implemented yet)");
		addProperty("CFR","CUI Frequency","The number of CUIs associated with this source, e.g., 10234 (not implemented yet)");
		addProperty("CXTY","Context Type","The type of relationship label (Section 2.4.2 of UMLS Reference Manual)");
		addProperty("TTYL","Term Type List","Term type list from source, e.g., MH,EN,PM,TQ");
		addProperty("ATNL","Attribute Name List","The attribute name list, e.g., MUI,RN,TH,...");
		addProperty("LAT","Language","The language of the terms in the source");
		addProperty("CENC","Character Encoding","Character set as specified by the IANA official names for character assignments http://www.iana.org/assignments/character-sets");
		addProperty("CURVER","Current Version","A Y or N flag indicating whether or not this row corresponds to the current version of the named source");
		addProperty("SABIN","Source in Subset","A Y or N flag indicating whether or not this row is represented in the current MetamorphoSys subset. Initially always Y where CURVER is Y, but later is recomputed by MetamorphoSys.");
		addProperty("SSN","Source short name","The short name of a source as used by the NLM Knowledge Source Server.");
		addProperty("SCIT","Source citation","Citation information for a source. A semi-colon separated list containing the following fields: Author(s); Author(s) address; Author(s) organization; Editor(s); Title; Content Designator; Medium Designator; Edition; Place of Publication; Publisher; Date of Publication/copyright; Date of revision; Location; Extent; Series; Availability Statement (URL); Language; Notes");	
	}
}
