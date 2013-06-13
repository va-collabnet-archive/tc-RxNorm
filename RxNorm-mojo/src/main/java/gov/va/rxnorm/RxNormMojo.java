package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion.BaseContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Relations;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ConceptCreationNotificationListener;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import gov.va.rxnorm.propertyTypes.PT_Attributes;
import gov.va.rxnorm.propertyTypes.PT_IDs;
import gov.va.rxnorm.propertyTypes.PT_Refsets;
import gov.va.rxnorm.propertyTypes.PT_Relationship_Metadata;
import gov.va.rxnorm.propertyTypes.PT_SAB_Metadata;
import gov.va.rxnorm.rrf.RXNCONSO;
import gov.va.rxnorm.rrf.RXNREL;
import gov.va.rxnorm.rrf.RXNSAT;
import gov.va.rxnorm.rrf.Relationship;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.dwfa.cement.ArchitectonicAuxiliary;
import org.dwfa.util.id.Type3UuidFactory;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.refex.type_string.TkRefsetStrMember;
import org.ihtsdo.tk.dto.concept.component.relationship.TkRelationship;

/**
 * Goal to build RxNorm
 * 
 * @goal buildRxNorm
 * 
 * @phase process-sources
 */
public class RxNormMojo extends AbstractMojo
{
	
	private final boolean liteLoad = true;
	
	private String problemListNamespaceSeed_ = "gov.va.med.term.RxNorm";
	private String terminologyName_ = "RxNorm";
	private EConceptUtility eConcepts_;
	private DataOutputStream dos_;
	private RRFDatabaseHandle db_;
	UUID metaDataRoot_;

	private PropertyType ptLanguages_;
	private PropertyType ptSABs_;
	private PropertyType ptDescriptions_;
	private PropertyType ptAttributes_;
	private PropertyType ptIds_;
	private PropertyType ptSuppress_;
	private BPT_Refsets ptRefsets_;
	private PropertyType ptContentVersion_;
	private PropertyType ptSourceRestrictionLevels_;
	private PropertyType ptSTypes_;
	private PropertyType ptRelationships_;
	private PropertyType ptRelationshipQualifiers_;
	
	private HashMap<String, UUID> semanticTypes_ = new HashMap<>();
	private HashMap<String, Relationship> nameToRel_ = new HashMap<>();
	
	private HashMap<String, String> loadedRels_ = new HashMap<>();
	private HashMap<String, String> skippedRels_ = new HashMap<>();

	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	private EConcept cpcRefsetConcept_;

	/**
	 * Where RxNorm source files are
	 * 
	 * @parameter
	 * @required
	 */
	private File srcDataPath;

	/**
	 * Location of the file.
	 * 
	 * @parameter expression="${project.build.directory}"
	 * @required
	 */
	private File outputDirectory;

	/**
	 * Loader version number
	 * Use parent because project.version pulls in the version of the data file, which I don't want.
	 * 
	 * @parameter expression="${project.parent.version}"
	 * @required
	 */
	private String loaderVersion;

	/**
	 * Content version number
	 * 
	 * @parameter expression="${project.version}"
	 * @required
	 */
	private String releaseVersion;

	public void execute() throws MojoExecutionException
	{
		try
		{
			outputDirectory.mkdir();

			loadDatabase();

			File binaryOutputFile = new File(outputDirectory, "RxNorm.jbin");

			dos_ = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(binaryOutputFile)));
			eConcepts_ = new EConceptUtility(problemListNamespaceSeed_, "RxNorm Path", dos_);

			UUID archRoot = ArchitectonicAuxiliary.Concept.ARCHITECTONIC_ROOT_CONCEPT.getPrimoridalUid();
			metaDataRoot_ = ConverterUUID.createNamespaceUUIDFromString("metadata");
			eConcepts_.createAndStoreMetaDataConcept(metaDataRoot_, "RxNorm Metadata", false, archRoot, dos_);

			loadMetaData();

			ConsoleUtil.println("Metadata Statistics");
			for (String s : eConcepts_.getLoadStats().getSummary())
			{
				ConsoleUtil.println(s);
			}

			eConcepts_.clearLoadStats();

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
			dos_.close();
			db_.shutdown();

			ConsoleUtil.println("Load Statistics");
			for (String s : eConcepts_.getLoadStats().getSummary())
			{
				ConsoleUtil.println(s);
			}

			// this could be removed from final release. Just added to help debug editor problems.
			ConsoleUtil.println("Dumping UUID Debug File");
			ConverterUUID.dump(new File(outputDirectory, "RxNormUuidDebugMap.txt"));
			ConsoleUtil.writeOutputToFile(new File(outputDirectory, "ConsoleOutput.txt").toPath());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MojoExecutionException("Failure during export ", e);
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

	private void loadDatabase() throws Exception
	{
		// Set up the DB for loading the temp data
		db_ = new RRFDatabaseHandle();
		File dbFile = new File(outputDirectory, "rrfDB.h2.db");
		boolean createdNew = db_.createOrOpenDatabase(new File(outputDirectory, "rrfDB"));

		if (!createdNew)
		{
			ConsoleUtil.println("Using existing database.  To load from scratch, delete the file '" + dbFile.getAbsolutePath() + ".*'");
		}
		else
		{
			// RxNorm doesn't give us the UMLS tables that define the table definitions, so I put them into an XML file.
			List<TableDefinition> tables = db_.loadTableDefinitionsFromXML(RxNormMojo.class.getResourceAsStream("/RxNormTableDefinitions.xml"));

			// Read the RRF file directly from the source zip file
			ZipFile zf = null;
			for (File f : srcDataPath.listFiles())
			{
				if (f.getName().toLowerCase().startsWith("rxnorm_full_") && f.getName().toLowerCase().endsWith(".zip"))
				{
					zf = new ZipFile(f);
					break;
				}
			}
			if (zf == null)
			{
				throw new MojoExecutionException("Can't find source zip file");
			}

			for (TableDefinition td : tables)
			{
				ZipEntry ze = zf.getEntry("rrf/" + td.getTableName() + ".RRF");
				if (ze == null)
				{
					throw new MojoExecutionException("Can't find the file 'rrf/" + td.getTableName() + ".RRF' in the zip file");
				}

				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new InputStreamReader(zf.getInputStream(ze), "UTF-8"))));
			}
			zf.close();

			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxcui_index ON RXNCONSO (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_rxcui_aui_index ON RXNSAT (RXCUI, RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_aui_index ON RXNSAT (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_rxcui_index ON RXNSTY (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_tui_index ON RXNSTY (TUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxcui2_index ON RXNREL (RXCUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxaui2_index ON RXNREL (RXAUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rela_index ON RXNREL (RELA)");
			s.close();
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
	
	private void loadMetaData() throws Exception
	{
		ptIds_ = new PT_IDs();
		ptRefsets_ = new PT_Refsets(terminologyName_);
		ptContentVersion_ = new BPT_ContentVersion();
		final PropertyType sourceMetadata = new PT_SAB_Metadata();
		final PropertyType relationshipMetadata = new PT_Relationship_Metadata();

		eConcepts_.loadMetaDataItems(Arrays.asList(ptIds_, ptRefsets_, ptContentVersion_, sourceMetadata, relationshipMetadata), metaDataRoot_, dos_);
		
		ptAttributes_ = new PT_Attributes();
		//dynamically add more attributes from RXNDOC
		{
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT VALUE, TYPE, EXPL from RXNDOC where DOCKEY = 'ATN'");
			while (rs.next())
			{
				String abbreviation = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String expansion = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within RXNDOC: '" + type + "'");
				}

				String preferredName = null;
				String description = null;
				if (expansion.length() > 30)
				{
					description = expansion;
				}
				else
				{
					preferredName = expansion;
				}
				
				ptAttributes_.addProperty(abbreviation, preferredName, description);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptAttributes_, metaDataRoot_, dos_);
		
		//STYPE values
		ptSTypes_= new PropertyType("STYPEs"){};
		{
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT DISTINCT VALUE, TYPE, EXPL FROM RXNDOC where DOCKEY like 'STYPE%'");
			while (rs.next())
			{
				String sType = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String name = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within RXNDOC: '" + type + "'");
				}				
				
				ptSTypes_.addProperty(sType, name, null);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptSTypes_, metaDataRoot_, dos_);
		
		//SUPPRESS values
		ptSuppress_= new PropertyType("Suppress") {};
		{
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT VALUE, TYPE, EXPL FROM RXNDOC where DOCKEY='SUPPRESS'");
			while (rs.next())
			{
				String value = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String name = rs.getString("EXPL");
				
				if (value == null)
				{
					//there is a null entry, don't care about it.
					continue;
				}

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within RXNDOC: '" + type + "'");
				}				
				
				ptSuppress_.addProperty(value, name, null);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptSuppress_, metaDataRoot_, dos_);
		
		// Handle the languages
		{
			ptLanguages_ = new PropertyType("Languages"){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT * from RXNDOC where DOCKEY = 'LAT'");
			while (rs.next())
			{
				String abbreviation = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String expansion = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the language data within RXNDOC: '" + type + "'");
				}

				Property p = ptLanguages_.addProperty(abbreviation, expansion, null);

				if (abbreviation.equals("ENG") || abbreviation.equals("SPA"))
				{
					// use official WB SCT languages
					if (abbreviation.equals("ENG"))
					{
						p.setWBPropertyType(UUID.fromString("c0836284-f631-3c86-8cfc-56a67814efab"));
					}
					else if (abbreviation.equals("SPA"))
					{
						p.setWBPropertyType(UUID.fromString("03615ef2-aa56-336d-89c5-a1b5c4cee8f6"));
					}
					else
					{
						throw new RuntimeException("oops");
					}
				}
			}
			rs.close();
			s.close();
			eConcepts_.loadMetaDataItems(ptLanguages_, metaDataRoot_, dos_);
		}
		
		// And Source Restriction Levels
		{
			ptSourceRestrictionLevels_ = new PropertyType("Source Restriction Levels"){};
			PreparedStatement ps = db_.getConnection().prepareStatement("SELECT VALUE, TYPE, EXPL from RXNDOC where DOCKEY=? ORDER BY VALUE");
			ps.setString(1, "SRL");
			ResultSet rs = ps.executeQuery();
			
			String value = null;
			String description = null;
			String uri = null;
			
			//Two entries per SRL, read two rows, create an entry.
			
			while (rs.next())
			{
				String type = rs.getString("TYPE");
				String expl = rs.getString("EXPL");
				
				if (type.equals("expanded_form"))
				{
					description = expl;
				}
				else if (type.equals("uri"))
				{
					uri = expl;
				}
				else
				{
					throw new RuntimeException("oops");
				}
					
				
				if (value == null)
				{
					value = rs.getString("VALUE");
				}
				else
				{
					if (!value.equals(rs.getString("VALUE")))
					{
						throw new RuntimeException("oops");
					}
					
					if (description == null || uri == null)
					{
						throw new RuntimeException("oops");
					}
					
					Property p = ptSourceRestrictionLevels_.addProperty(value, null, description);
					final String temp = uri;
					p.registerConceptCreationListener(new ConceptCreationNotificationListener()
					{
						@Override
						public void conceptCreated(Property property, EConcept concept)
						{
							eConcepts_.addStringAnnotation(concept, temp, ptAttributes_.getProperty("URI").getUUID(), false);
						}
					});
					type = null;
					expl = null;
					value = null;
				}
			}
			rs.close();
			ps.close();

			eConcepts_.loadMetaDataItems(ptSourceRestrictionLevels_, metaDataRoot_, dos_);
		}

		// And Source vocabularies
		{
			ptSABs_ = new PropertyType("Source Vocabularies"){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT RSAB, SON from RXNSAB");
			while (rs.next())
			{
				String rsab = rs.getString("RSAB");
				String son = rs.getString("SON");

				Property p = ptSABs_.addProperty(rsab, son, null);
				p.registerConceptCreationListener(new ConceptCreationNotificationListener()
				{
					@Override
					public void conceptCreated(Property property, EConcept concept)
					{
						try
						{
							PreparedStatement getMetadata = db_.getConnection().prepareStatement("Select * from RXNSAB where RSAB = ?");

							//lookup the other columns for the row with this newly added RSAB terminology
							getMetadata.setString(1, property.getSourcePropertyNameFSN());
							ResultSet rs = getMetadata.executeQuery();
							if (rs.next())  //should be only one result
							{
								for (Property metadataProperty : sourceMetadata.getProperties())
								{
									String columnName = metadataProperty.getSourcePropertyNameFSN();
									String columnValue = rs.getString(columnName);
									if (columnName.equals("SRL"))
									{
										eConcepts_.addUuidAnnotation(concept, ptSourceRestrictionLevels_.getProperty(columnValue).getUUID(),
												metadataProperty.getUUID());
									}
									else
									{
										eConcepts_.addStringAnnotation(concept, columnValue, metadataProperty.getUUID(), false);
									}
								}
							}
							if (rs.next())
							{
								throw new RuntimeException("oops!");
							}
							rs.close();
							getMetadata.close();
						}
						catch (SQLException e)
						{
							throw new RuntimeException("Error loading RXNSAB", e);
						}
					}
				});
			}
			rs.close();
			s.close();

			eConcepts_.loadMetaDataItems(ptSABs_, metaDataRoot_, dos_);
		}

		// And Descriptions
		{
			ptDescriptions_ = new BPT_Descriptions(terminologyName_);
			Statement s = db_.getConnection().createStatement();
			ResultSet usedDescTypes = s.executeQuery("select distinct TTY from RXNCONSO");

			PreparedStatement ps = db_.getConnection().prepareStatement("select TYPE, EXPL from RXNDOC where DOCKEY='TTY' and VALUE=?");

			while (usedDescTypes.next())
			{
				String tty = usedDescTypes.getString(1);
				ps.setString(1, tty);
				ResultSet descInfo = ps.executeQuery();

				String expandedForm = null;
				HashSet<String> classes = new HashSet<>();

				while (descInfo.next())
				{
					String type = descInfo.getString("TYPE");
					String expl = descInfo.getString("EXPL");
					if (type.equals("expanded_form"))
					{
						if (expandedForm != null)
						{
							throw new RuntimeException("Expected name to be null!");
						}
						expandedForm = expl;
					}
					else if (type.equals("tty_class"))
					{
						classes.add(expl);
					}
					else
					{
						throw new RuntimeException("Unexpected type in RXNDOC for '" + tty + "'");
					}
				}
				descInfo.close();
				ps.clearParameters();
				makeDescriptionType(tty, expandedForm, classes);
			}
			usedDescTypes.close();
			s.close();
			ps.close();

			eConcepts_.loadMetaDataItems(ptDescriptions_, metaDataRoot_, dos_);
		}
		
		// And semantic types
		{
			PropertyType ptSemanticTypes = new PropertyType("Semantic Types"){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT distinct TUI, STN, STY from RXNSTY");
			while (rs.next())
			{
				final String tui = rs.getString("TUI");
				final String stn = rs.getString("STN");
				String sty = rs.getString("STY");

				Property p = ptSemanticTypes.addProperty(sty);
				semanticTypes_.put(tui, p.getUUID());
				p.registerConceptCreationListener(new ConceptCreationNotificationListener()
				{
					@Override
					public void conceptCreated(Property property, EConcept concept)
					{
						eConcepts_.addAdditionalIds(concept, tui, ptIds_.getProperty("TUI").getUUID(), false);
						eConcepts_.addStringAnnotation(concept, stn, ptAttributes_.getProperty("STN").getUUID(), false);
					}
				});
			}
			rs.close();
			s.close();

			eConcepts_.loadMetaDataItems(ptSemanticTypes, metaDataRoot_, dos_);
		}
		
		loadRelationshipMetadata(relationshipMetadata);
	}
	
	private void loadRelationshipMetadata(final PropertyType relationshipMetadata) throws Exception
	{
		//Both of these get added as extra attributes on the relationship definition
		HashMap<String, String> snomedCTRelaMappings = new HashMap<>(); //Maps something like 'has_specimen_source_morphology' to '118168003'
		HashMap<String, String> snomedCTRelMappings = new HashMap<>();  //Maps something like '118168003' to 'RO'
		
		nameToRel_ = new HashMap<>();
		
		Statement s = db_.getConnection().createStatement();
		//get the inverses of first, before the expanded forms
		ResultSet rs = s.executeQuery("SELECT DOCKEY, VALUE, TYPE, EXPL FROM RXNDOC where DOCKEY ='REL' or DOCKEY = 'RELA' order by TYPE DESC ");
		while (rs.next())
		{
			String dockey = rs.getString("DOCKEY");
			String value = rs.getString("VALUE");
			String type = rs.getString("TYPE");
			String expl = rs.getString("EXPL");
			if (value == null)
			{
				continue;  //don't need this one
			}
			
			if (type.equals("snomedct_rela_mapping"))
			{
				snomedCTRelaMappings.put(expl,  value);
			}
			else if (type.equals("snomedct_rel_mapping"))
			{
				snomedCTRelMappings.put(value, expl);
			}
			else
			{
				Relationship rel = nameToRel_.get(value);
				if (rel == null)
				{
					if (type.endsWith("_inverse"))
					{
						rel = nameToRel_.get(expl);
						if (rel == null)
						{
							rel = new Relationship(dockey.equals("RELA"));
							nameToRel_.put(value, rel);
							nameToRel_.put(expl, rel);
						}
						else
						{
							throw new RuntimeException("shouldn't happen due to query order");
						}
					}
					else
					{
						//only cases where there is no inverse
						rel = new Relationship(dockey.equals("RELA"));
						nameToRel_.put(value, rel);
					}
				}
				
				if (type.equals("expanded_form"))
				{
					rel.addNiceName(value, expl);
				}
				else if (type.equals("rela_inverse") || type.equals("rel_inverse"))
				{
					rel.addRelInverse(value, expl);
				}
				else
				{
					throw new RuntimeException("Oops");
				}
			}
		}
		
		rs.close();
		s.close();
		
		for (Entry<String, String> x : snomedCTRelaMappings.entrySet())
		{
			nameToRel_.get(x.getKey()).addSnomedCode(x.getKey(), x.getValue());
			String relType = snomedCTRelMappings.remove(x.getValue());
			if (relType != null)
			{
				nameToRel_.get(x.getKey()).addRelType(x.getKey(), relType);
			}
		}
		
		if (snomedCTRelMappings.size() > 0)
		{
			throw new RuntimeException("oops");
		}
		
		ptRelationships_ = new BPT_Relations(terminologyName_) {};
		ptRelationshipQualifiers_ = new PropertyType("Relationship Qualifiers") {};
		
		HashSet<Relationship> uniqueRels = new HashSet<>(nameToRel_.values());
		for (final Relationship r : uniqueRels)
		{
			r.setSwap(db_.getConnection());
			
			Property p;
			if (r.getIsRela())
			{
				p = ptRelationshipQualifiers_.addProperty(r.getFSNName(), r.getNiceName(), null);
			}
			else
			{
				p = ptRelationships_.addProperty(r.getFSNName(), r.getNiceName(), null);
			}
			
			p.registerConceptCreationListener(new ConceptCreationNotificationListener()
			{
				@Override
				public void conceptCreated(Property property, EConcept concept)
				{
					if (r.getInverseFSNName() != null)
					{
						eConcepts_.addDescription(concept, r.getInverseFSNName(), DescriptionType.FSN, false, null, 
								relationshipMetadata.getProperty("Inverse FSN").getUUID(), false);
					}
					
					if (r.getInverseNiceName() != null)
					{
						eConcepts_.addDescription(concept, r.getInverseNiceName(), DescriptionType.SYNONYM, true, null, 
								relationshipMetadata.getProperty("Inverse Name").getUUID(), false);
					}
					
					if (r.getRelType() != null)
					{
						Relationship generalRel = nameToRel_.get(r.getRelType());
						
						eConcepts_.addUuidAnnotation(concept, ptRelationships_.getProperty(generalRel.getFSNName()).getUUID(), 
								relationshipMetadata.getProperty("General Rel Type").getUUID());
					}
					
					if (r.getInverseRelType() != null)
					{
						Relationship generalRel = nameToRel_.get(r.getInverseRelType());
						
						eConcepts_.addUuidAnnotation(concept, ptRelationships_.getProperty(generalRel.getFSNName()).getUUID(), 
								relationshipMetadata.getProperty("Inverse General Rel Type").getUUID());
					}
					
					if (r.getRelSnomedCode() != null)
					{
						eConcepts_.addUuidAnnotation(concept, Type3UuidFactory.fromSNOMED(r.getRelSnomedCode()), 
								relationshipMetadata.getProperty("Snomed Code").getUUID());
					}
					
					if (r.getInverseRelSnomedCode() != null)
					{
						eConcepts_.addUuidAnnotation(concept, Type3UuidFactory.fromSNOMED(r.getInverseRelSnomedCode()), 
								relationshipMetadata.getProperty("Inverse Snomed Code").getUUID());
					}
				}
			});
		}
		
		eConcepts_.loadMetaDataItems(ptRelationships_, metaDataRoot_, dos_);
		eConcepts_.loadMetaDataItems(ptRelationshipQualifiers_, metaDataRoot_, dos_);
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
	
	private void makeDescriptionType(String fsnName, String preferredName, final Set<String> tty_classes)
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
	
	public static void main(String[] args) throws MojoExecutionException
	{
		RxNormMojo mojo = new RxNormMojo();
		mojo.outputDirectory = new File("../RxNorm-econcept/target");
		mojo.srcDataPath = new File("../RxNorm-econcept/target/generated-resources/src/");
		mojo.execute();
	}
}
