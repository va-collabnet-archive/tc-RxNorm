package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion.BaseContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ConceptCreationNotificationListener;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.BaseConverter;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.propertyTypes.PT_Refsets;
import gov.va.rxnorm.propertyTypes.PT_Attributes;
import gov.va.rxnorm.propertyTypes.PT_IDs;
import gov.va.rxnorm.rrf.RXNCONSO;
import gov.va.rxnorm.rrf.RXNREL;
import gov.va.rxnorm.rrf.RXNSAT;
import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.refex.type_string.TkRefsetStrMember;
import org.ihtsdo.tk.dto.concept.component.relationship.TkRelationship;

public class RxNormConverter extends BaseConverter
{
	private final boolean liteLoad = true;
	
	private HashMap<String, String> loadedRels_ = new HashMap<>();
	private HashMap<String, String> skippedRels_ = new HashMap<>();

	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	private EConcept cpcRefsetConcept_;

	public RxNormConverter(File outputDirectory, String loaderVersion, String releaseVersion, RRFDatabaseHandle db) throws Exception
	{
		super("RXNORM", "RxNorm", db, "RXN", outputDirectory, false, new PT_IDs(), new PT_Attributes());
		db_ = db;
		
		try
		{
			allRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.ALL.getProperty());
			cpcRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CPC.getProperty());
			allCUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CUI_CONCEPTS.getProperty());
			allAUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.AUI_CONCEPTS.getProperty());

			// Add version data to allRefsetConcept
			eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion, BaseContentVersion.LOADER_VERSION.getProperty().getUUID(), false);
			eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, BaseContentVersion.RELEASE.getProperty().getUUID(), false);
			
			//Disable the masterUUID debug map now that the metadata is populated, not enough memory on most systems to maintain it for everything else.
			ConverterUUID.disableUUIDMap_ = true;
			int cuiCounter = 0;

			Statement statement = db_.getConnection().createStatement();
			//TODO SIZELIMIT - remove SAB restriction
			ResultSet rs = statement.executeQuery("select RXCUI, LAT, RXAUI, SAUI, SCUI, SAB, TTY, CODE, STR, SUPPRESS, CVF from RXNCONSO " 
					+ (liteLoad ? "where SAB='RXNORM' " : "") + "order by RXCUI" );
			ArrayList<RXNCONSO> conceptData = new ArrayList<>();
			while (rs.next())
			{
				RXNCONSO current = new RXNCONSO(rs);
				if (conceptData.size() > 0 && !conceptData.get(0).rxcui.equals(current.rxcui))
				{
					processCUIRows(conceptData);
					ConsoleUtil.showProgress();
					cuiCounter++;
					if (cuiCounter % 10000 == 0)
					{
						ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
					}
					conceptData.clear();
				}
				conceptData.add(current);
			}
			rs.close();
			statement.close();

			// process last
			processCUIRows(conceptData);
			
			checkRelationships();

			eConcepts_.storeRefsetConcepts(ptRefsets_, dos_);
			db_.shutdown();
			finish();
		}
		finally
		{
			try
			{
				db_.shutdown();
			}
			catch (SQLException e)
			{
				ConsoleUtil.printErrorln("Error closing source DB: " + e);
			}
		}
	}

	

	private void processCUIRows(ArrayList<RXNCONSO> conceptData) throws IOException, SQLException
	{
		EConcept cuiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("RXCUI" + conceptData.get(0).rxcui, true));
		eConcepts_.addAdditionalIds(cuiConcept, conceptData.get(0).rxcui, ptIds_.getProperty("RXCUI").getUUID(), false);

		ArrayList<ValuePropertyPairWithSAB> descriptions = new ArrayList<>();
		
		for (RXNCONSO rowData : conceptData)
		{
			EConcept auiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("RXAUI" + rowData.rxaui, true));
			eConcepts_.addAdditionalIds(auiConcept, rowData.rxaui, ptIds_.getProperty("RXAUI").getUUID(), false);
			
			if (rowData.saui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.saui, ptAttributes_.getProperty("SAUI").getUUID(), false);
			}
			if (rowData.scui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.scui, ptAttributes_.getProperty("SCUI").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSABs_.getProperty(rowData.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());

			if (rowData.code != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.code, ptAttributes_.getProperty("CODE").getUUID(), false);
			}

			eConcepts_.addUuidAnnotation(auiConcept, ptSuppress_.getProperty(rowData.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS")
					.getUUID());
			
			if (rowData.cvf != null)
			{
				if (rowData.cvf.equals("4096"))
				{
					eConcepts_.addRefsetMember(cpcRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNCONSO cvf column '" + rowData.cvf + "'");
				}
			}
			// TODO handle language.
			if (!rowData.lat.equals("ENG"))
			{
				ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
			}
			
			eConcepts_.addDescription(auiConcept, rowData.str, DescriptionType.FSN, true, ptDescriptions_.getProperty(rowData.tty).getUUID(), 
					ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
			
			//used for sorting description to find one for the CUI concept
			descriptions.add(new ValuePropertyPairWithSAB(rowData.str, ptDescriptions_.getProperty(rowData.tty), rowData.sab));
			
			//Add attributes
			processConceptAttributes(auiConcept, rowData.rxcui, rowData.rxaui);
			
			eConcepts_.addRelationship(auiConcept, cuiConcept.getPrimordialUuid());
			
			eConcepts_.addRefsetMember(allRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(allAUIRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			addRelationships(auiConcept, rowData.rxaui, false);
			auiConcept.writeExternal(dos_);
		}
		
		//Pick the 'best' description to use on the cui concept
		Collections.sort(descriptions);
		eConcepts_.addDescription(cuiConcept, descriptions.get(0).getValue(), DescriptionType.FSN, true, descriptions.get(0).getProperty().getUUID(), 
				ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
		
		//there are no attributes in rxnorm without an AUI.
		
		//add semantic types
		processSemanticTypes(cuiConcept, conceptData.get(0).rxcui);
		
		addRelationships(cuiConcept, conceptData.get(0).rxcui, true);

		eConcepts_.addRefsetMember(allRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		eConcepts_.addRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		cuiConcept.writeExternal(dos_);
	}
	
	private void processConceptAttributes(EConcept concept, String rxcui, String rxaui) throws SQLException
	{
		//TODO SIZELIMIT - remove SAB restriction
		PreparedStatement ps = db_.getConnection().prepareStatement("select * from RXNSAT where RXCUI = ? and RXAUI = ?" 
				+ (liteLoad ? " and (SAB='RXNORM' or ATN='NDC')" : ""));
		ps.setString(1, rxcui);
		ps.setString(2, rxaui);
		ResultSet rs = ps.executeQuery();
		
		ArrayList<RXNSAT> rowData = new ArrayList<>();
		while (rs.next())
		{
			rowData.add(new RXNSAT(rs));
		}
		rs.close();
		ps.close();
		
		processRXNSAT(concept.getConceptAttributes(), rowData);
	}
	
	private void processRXNSAT(TkComponent<?> itemToAnnotate, List<RXNSAT> rxnsatRows)
	{
		for (RXNSAT row : rxnsatRows)
		{
			//for some reason, ATUI isn't always provided - don't know why.  fallback on randomly generated in those cases.
			TkRefsetStrMember attribute = eConcepts_.addStringAnnotation(itemToAnnotate, 
					(row.atui == null ? null : ConverterUUID.createNamespaceUUIDFromString("ATUI" + row.atui)), row.atv, 
					ptAttributes_.getProperty(row.atn).getUUID(), false, null);
			
			if (row.stype != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(row.stype).getUUID(), ptAttributes_.getProperty("STYPE").getUUID());
			}
			
			if (row.code != null)
			{
				eConcepts_.addStringAnnotation(attribute, row.code, ptAttributes_.getProperty("CODE").getUUID(), false);
			}
			if (row.atui != null)
			{
				eConcepts_.addStringAnnotation(attribute, row.atui, ptAttributes_.getProperty("ATUI").getUUID(), false);
			}
			if (row.satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, row.satui, ptAttributes_.getProperty("SATUI").getUUID(), false);
			}
			eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(row.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
			if (row.suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(row.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (row.cvf != null)
			{
				if (row.cvf.equals("4096"))
				{
					eConcepts_.addRefsetMember(cpcRefsetConcept_, attribute.getPrimordialComponentUuid(), null, true, null);
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + row.cvf + "'");
				}
			}
		}
	}
	
	private void processSemanticTypes(EConcept concept, String rxcui) throws SQLException
	{
		PreparedStatement ps = db_.getConnection().prepareStatement("select TUI, ATUI from RXNSTY where RXCUI = ?");
		ps.setString(1, rxcui);
		ResultSet rs = ps.executeQuery();
		
		while (rs.next())
		{
			if (rs.getString("ATUI") != null)
			{
				throw new RuntimeException("Unexpected ATUI value");
			}
			eConcepts_.addUuidAnnotation(concept, semanticTypes_.get(rs.getString("TUI")), ptAttributes_.getProperty("STY").getUUID());
		}
		rs.close();
		ps.close();
	}
	
	/**
	 * @param isCUI - true for CUI, false for AUI
	 * @throws SQLException
	 */
	private void addRelationships(EConcept concept, String id2, boolean isCUI) throws SQLException
	{
		//TODO SIZELIMIT - remove SAB restriction
		Statement s = db_.getConnection().createStatement();
		ResultSet rs = s.executeQuery("Select RXCUI1, RXAUI1, STYPE1, REL, STYPE2, RELA, RUI, SAB, RG, SUPPRESS, CVF from RXNREL where " 
				+ (isCUI ? "RXCUI2" : "RXAUI2") + "='" + id2 + "'" + (liteLoad ? " and SAB='RXNORM'" : ""));
				
		while (rs.next())
		{
			RXNREL rel = new RXNREL(rs);
			
			if (isRelPrimary(rel.rel, rel.rela))
			{
				UUID targetConcept = ConverterUUID.createNamespaceUUIDFromString((isCUI ? "RXCUI" + rel.rxcui1 : "RXAUI" + rel.rxaui1), true);
				TkRelationship r = eConcepts_.addRelationship(concept, (rel.rui != null ? ConverterUUID.createNamespaceUUIDFromString("RUI:" + rel.rui) : null),
						targetConcept, ptRelationships_.getProperty(rel.rel).getUUID(), null, null, null);
				
				annotateRelationship(r, rel);
				updateSanityCheckLoadedData(rel.rel, rel.rela, id2, (isCUI ? rel.rxcui1 : rel.rxaui1), (isCUI ? "CUI" : "AUI"));
			}
			else
			{
				updateSanityCheckSkipData(rel.rel, rel.rela, id2, (isCUI ? rel.rxcui1 : rel.rxaui1), (isCUI ? "CUI" : "AUI"));
			}
		}
		s.close();
		rs.close();
	}
	
	private void updateSanityCheckLoadedData(String rel, String rela, String source, String target, String type)
	{
		loadedRels_.put(type + ":" + source, rel + ":" + rela + ":" + target);
		skippedRels_.remove(type + ":" + source);
	}
	
	private void updateSanityCheckSkipData(String rel, String rela, String source, String target, String type)
	{
		//Get the primary rel name, add it to the skip list
		String primary = nameToRel_.get(rel).getFSNName();
		String primaryExtended = null;
		if (rela != null)
		{
			primaryExtended = nameToRel_.get(rela).getFSNName();
		}
		//also reverse the cui2 / cui1
		skippedRels_.put(type + ":" + target, primary + ":" + primaryExtended + ":" + source);
	}
	
	private void checkRelationships()
	{
		//if the inverse relationships all worked properly, loaded and skipped should be copies of each other.
		
		for (String s : loadedRels_.keySet())
		{
			skippedRels_.remove(s);
		}
		
		if (skippedRels_.size() > 0)
		{
			ConsoleUtil.printErrorln("Relationship design error - " +  skippedRels_.size() + " were skipped that should have been loaded");
			for (Entry<String, String> x : skippedRels_.entrySet())
			{
				ConsoleUtil.printErrorln(x.getKey() + "->" + x.getValue());
			}
		}
		else
		{
			ConsoleUtil.println("Yea! - no missing relationships!");
		}
	}
	
	private void annotateRelationship(TkRelationship r, RXNREL relData) throws SQLException
	{
		eConcepts_.addStringAnnotation(r, relData.stype1, ptAttributes_.getProperty("STYPE1").getUUID(), false);
		eConcepts_.addStringAnnotation(r, relData.stype2, ptAttributes_.getProperty("STYPE2").getUUID(), false);
		if (relData.rela != null)
		{
			eConcepts_.addUuidAnnotation(r, ptRelationshipQualifiers_.getProperty(relData.rela).getUUID(), ptAttributes_.getProperty("RELA Label").getUUID());
		}
		if (relData.rui != null)
		{
			eConcepts_.addAdditionalIds(r, relData.rui, ptIds_.getProperty("RUI").getUUID());
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("select * from RXNSAT where STYPE='RUI' and RXAUI='" + relData.rui + "'");
			ArrayList<RXNSAT> rowData = new ArrayList<>();
			while (rs.next())
			{
				rowData.add(new RXNSAT(rs));
			}
			rs.close();
			s.close();
			
			processRXNSAT(r, rowData);
		}
		eConcepts_.addUuidAnnotation(r, ptSABs_.getProperty(relData.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
		if (relData.rg != null)
		{
			eConcepts_.addStringAnnotation(r, relData.rg, ptAttributes_.getProperty("RG").getUUID(), false);
		}
		if (relData.suppress != null)
		{
			eConcepts_.addUuidAnnotation(r, ptSuppress_.getProperty(relData.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
		}
		if (relData.cvf != null)
		{
			if (relData.cvf.equals("4096"))
			{
				eConcepts_.addRefsetMember(cpcRefsetConcept_, r.getPrimordialComponentUuid(), null, true, null);
			}
			else
			{
				throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + relData.cvf + "'");
			}
		}
	}
	
	private boolean isRelPrimary(String relName, String relaName)
	{
		if (relaName != null)
		{
			return nameToRel_.get(relaName).getFSNName().equals(relaName);
		}
		else
		{
			return nameToRel_.get(relName).getFSNName().equals(relName);
		}
	}
	
	protected void makeDescriptionType(String fsnName, String preferredName, final Set<String> tty_classes)
	{
		// The current possible classes are:
		// preferred
		// obsolete
		// entry_term
		// hierarchical
		// synonym
		// attribute
		// abbreviation
		// expanded
		// other

		// TODO - Question - do we want to do any other ranking based on the SAB?  Currently, only rank RXNORM sabs higher...  
		int descriptionRanking;

		//Note - ValuePropertyPairWithSAB overrides the sorting based on these values to kick RXNORM sabs to the top, where 
		//they will get used as FSN.
		if (fsnName.equals("FN") && tty_classes.contains("preferred"))
		{
			descriptionRanking = BPT_Descriptions.FSN;
		}
		else if (fsnName.equals("FN"))
		{
			descriptionRanking = BPT_Descriptions.FSN + 1;
		}
		// preferred gets applied with others as well, in some cases. Don't want 'preferred' 'obsolete' at the top.
		//Just preferred, and we make it the top synonym.
		else if (tty_classes.contains("preferred") && tty_classes.size() == 1)
		{
			descriptionRanking = BPT_Descriptions.SYNONYM;
		}
		else if (tty_classes.contains("entry_term"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 1;
		}
		else if (tty_classes.contains("synonym"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 2;
		}
		else if (tty_classes.contains("expanded"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 3;
		}
		else if (tty_classes.contains("abbreviation"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 4;
		}
		else if (tty_classes.contains("attribute"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 5;
		}
		else if (tty_classes.contains("hierarchical"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 6;
		}
		else if (tty_classes.contains("other"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 7;
		}
		else if (tty_classes.contains("obsolete"))
		{
			descriptionRanking = BPT_Descriptions.SYNONYM + 8;
		}
		else
		{
			throw new RuntimeException("Unexpected class type");
		}
		Property p = ptDescriptions_.addProperty(fsnName, preferredName, null, false, descriptionRanking);
		p.registerConceptCreationListener(new ConceptCreationNotificationListener()
		{
			@Override
			public void conceptCreated(Property property, EConcept concept)
			{
				for (String tty_class : tty_classes)
				{
					eConcepts_.addStringAnnotation(concept, tty_class, ptAttributes_.getProperty("tty_class").getUUID(), false);
				}
			}
		});
	}

	@Override
	protected void allDescriptionsCreated()
	{
		//noop
	}
}
