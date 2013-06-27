package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.BaseConverter;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.propertyTypes.PT_Refsets;
import gov.va.rxnorm.propertyTypes.PT_Attributes;
import gov.va.rxnorm.propertyTypes.PT_IDs;
import gov.va.rxnorm.rrf.RXNCONSO;
import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.refex.type_string.TkRefsetStrMember;

public class RxNormConverter extends BaseConverter
{
	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	private EConcept cpcRefsetConcept_;
	
	private PreparedStatement semanticTypeStatement, conSat, cuiRelStatementForward, auiRelStatementForward, cuiRelStatementBackward, auiRelStatementBackward;

	public RxNormConverter(File outputDirectory, String loaderVersion, String releaseVersion, RRFDatabaseHandle db) throws Exception
	{
		super("RXNORM", "RxNorm", "RxNorm", db, "RXN", outputDirectory, false, new PT_IDs(), new PT_Attributes());
		db_ = db;
				
		try
		{
			allRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.ALL.getPropertyName());
			cpcRefsetConcept_ = ptRefsets_.getConcept("Current Prescribable Content");
			allCUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CUI_CONCEPTS.getPropertyName());
			allAUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.AUI_CONCEPTS.getPropertyName());

			// Add version data to allRefsetConcept
			eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion,  ptContentVersion_.LOADER_VERSION.getUUID(), false);
			eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, ptContentVersion_.RELEASE.getUUID(), false);
			
			semanticTypeStatement = db_.getConnection().prepareStatement("select TUI, ATUI, CVF from RXNSTY where RXCUI = ?");
			conSat = db_.getConnection().prepareStatement("select * from RXNSAT where RXCUI = ? and RXAUI = ? and (SAB='RXNORM' or ATN='NDC')");
			cuiRelStatementForward = db_.getConnection().prepareStatement("SELECT * from RXNREL where RXCUI2 = ? and RXAUI2 is null and SAB='RXNORM'");
			auiRelStatementForward = db_.getConnection().prepareStatement("SELECT * from RXNREL where RXCUI2 = ? and RXAUI2 = ? and SAB='RXNORM'");
			cuiRelStatementBackward = db_.getConnection().prepareStatement("SELECT * from RXNREL where RXCUI1 = ? and RXAUI1 is null and SAB='RXNORM'");
			auiRelStatementBackward = db_.getConnection().prepareStatement("SELECT * from RXNREL where RXCUI1 = ? and RXAUI1 = ? and SAB='RXNORM'");
			
			//Disable the masterUUID debug map now that the metadata is populated, not enough memory on most systems to maintain it for everything else.
			ConverterUUID.disableUUIDMap_ = true;
			int cuiCounter = 0;

			Statement statement = db_.getConnection().createStatement();
			ResultSet rs = statement.executeQuery("select RXCUI, LAT, RXAUI, SAUI, SCUI, SAB, TTY, CODE, STR, SUPPRESS, CVF from RXNCONSO " 
					+ "where SAB='RXNORM' order by RXCUI" );
			ArrayList<RXNCONSO> conceptData = new ArrayList<>();
			while (rs.next())
			{
				RXNCONSO current = new RXNCONSO(rs);
				if (conceptData.size() > 0 && !conceptData.get(0).rxcui.equals(current.rxcui))
				{
					processCUIRows(conceptData);
					if (cuiCounter % 100 == 0)
					{
						ConsoleUtil.showProgress();
					}
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

			semanticTypeStatement.close();
			conSat.close();
			cuiRelStatementForward.close();
			cuiRelStatementBackward.close();
			auiRelStatementBackward.close();
			auiRelStatementForward.close();
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
		EConcept cuiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("CUI" + conceptData.get(0).rxcui, true));
		eConcepts_.addAdditionalIds(cuiConcept, conceptData.get(0).rxcui, ptIds_.getProperty("RXCUI").getUUID(), false);

		ArrayList<ValuePropertyPairWithSAB> descriptions = new ArrayList<>();
		
		for (RXNCONSO rowData : conceptData)
		{
			EConcept auiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("AUI" + rowData.rxaui, true));
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
			conSat.clearParameters();
			conSat.setString(1, rowData.rxcui);
			conSat.setString(2, rowData.rxaui);
			ResultSet rs = conSat.executeQuery();
			processSAT(auiConcept.getConceptAttributes(), rs);
			
			eConcepts_.addRelationship(auiConcept, cuiConcept.getPrimordialUuid());
			
			eConcepts_.addRefsetMember(allRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(allAUIRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			auiRelStatementForward.clearParameters();
			auiRelStatementForward.setString(1, conceptData.get(0).rxcui);
			auiRelStatementForward.setString(2, rowData.rxaui);
			addRelationships(auiConcept, auiRelStatementForward.executeQuery(), true);
			
			auiRelStatementBackward.clearParameters();
			auiRelStatementBackward.setString(1, conceptData.get(0).rxcui);
			auiRelStatementBackward.setString(2, rowData.rxaui);
			addRelationships(auiConcept, auiRelStatementBackward.executeQuery(), false);
			auiConcept.writeExternal(dos_);
		}
		
		//Pick the 'best' description to use on the cui concept
		Collections.sort(descriptions);
		eConcepts_.addDescription(cuiConcept, descriptions.get(0).getValue(), DescriptionType.FSN, true, descriptions.get(0).getProperty().getUUID(), 
				ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
		
		//there are no attributes in rxnorm without an AUI.
		
		//add semantic types
		semanticTypeStatement.clearParameters();
		semanticTypeStatement.setString(1, conceptData.get(0).rxcui);
		ResultSet rs = semanticTypeStatement.executeQuery();
		processSemanticTypes(cuiConcept, rs);
		
		cuiRelStatementForward.clearParameters();
		cuiRelStatementForward.setString(1, conceptData.get(0).rxcui);
		addRelationships(cuiConcept, cuiRelStatementForward.executeQuery(), true);
		
		cuiRelStatementBackward.clearParameters();
		cuiRelStatementBackward.setString(1, conceptData.get(0).rxcui);
		addRelationships(cuiConcept, cuiRelStatementBackward.executeQuery(), false);

		eConcepts_.addRefsetMember(allRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		eConcepts_.addRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		cuiConcept.writeExternal(dos_);
	}
	
	protected Property makeDescriptionType(String fsnName, String preferredName, final Set<String> tty_classes)
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
		return ptDescriptions_.addProperty(fsnName, preferredName, null, false, descriptionRanking);
	}

	@Override
	protected void allDescriptionsCreated()
	{
		//noop
	}

	@Override
	protected void loadCustomMetaData() throws Exception
	{
		//noop
	}

	@Override
	protected void addCustomRefsets() throws Exception
	{
		ptRefsets_.addProperty("Current Prescribable Content");
	}

	@Override
	protected void processSAT(TkComponent<?> itemToAnnotate, ResultSet rs) throws SQLException
	{
		while (rs.next())
		{
			//String rxcui = rs.getString("RXCUI");
			//String rxaui = rs.getString("RXAUI");
			String stype = rs.getString("STYPE");
			String code = rs.getString("CODE");
			String atui = rs.getString("ATUI");
			String satui = rs.getString("SATUI");
			String atn = rs.getString("ATN");
			String sab = rs.getString("SAB");
			String atv = rs.getString("ATV");
			String suppress = rs.getString("SUPPRESS");
			String cvf = rs.getString("CVF");
			
			//for some reason, ATUI isn't always provided - don't know why.  fallback on randomly generated in those cases.
			TkRefsetStrMember attribute = eConcepts_.addStringAnnotation(itemToAnnotate, 
					(atui == null ? null : ConverterUUID.createNamespaceUUIDFromString("ATUI" + atui)), atv, 
					ptAttributes_.getProperty(atn).getUUID(), false, null);
			
			if (stype != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(stype).getUUID(), ptAttributes_.getProperty("STYPE").getUUID());
			}
			
			if (code != null)
			{
				eConcepts_.addStringAnnotation(attribute, code, ptAttributes_.getProperty("CODE").getUUID(), false);
			}
			if (atui != null)
			{
				eConcepts_.addStringAnnotation(attribute, atui, ptAttributes_.getProperty("ATUI").getUUID(), false);
			}
			if (satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, satui, ptAttributes_.getProperty("SATUI").getUUID(), false);
			}
			eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (cvf != null)
			{
				if (cvf.equals("4096"))
				{
					eConcepts_.addRefsetMember(cpcRefsetConcept_, attribute.getPrimordialComponentUuid(), null, true, null);
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + cvf + "'");
				}
			}
		}
		rs.close();
	}
}
