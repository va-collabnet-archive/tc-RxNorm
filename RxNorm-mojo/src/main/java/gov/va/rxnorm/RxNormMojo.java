package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.BaseConverter;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.ValuePropertyPairWithAttributes;
import gov.va.oia.terminology.converters.umlsUtils.rrf.REL;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import gov.va.rxnorm.propertyTypes.PT_Attributes;
import gov.va.rxnorm.propertyTypes.PT_IDs;
import gov.va.rxnorm.rrf.RXNCONSO;
import java.io.BufferedReader;
import java.io.File;
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
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.description.TkDescription;
import org.ihtsdo.tk.dto.concept.component.refex.type_string.TkRefsetStrMember;
import org.ihtsdo.tk.dto.concept.component.refex.type_uuid.TkRefexUuidMember;
import org.ihtsdo.tk.dto.concept.component.relationship.TkRelationship;

/**
 * Goal to build RxNorm
 * 
 * @goal buildRxNorm
 * 
 * @phase process-sources
 */
public class RxNormMojo extends BaseConverter implements Mojo
{
	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	private EConcept cpcRefsetConcept_;
	public static final String cpcRefsetConceptKey_ = "Current Prescribable Content";
	
	private PreparedStatement semanticTypeStatement, conSat, cuiRelStatementForward, auiRelStatementForward, cuiRelStatementBackward, auiRelStatementBackward;
	
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
			
			init(outputDirectory, "RxNorm", "RXN", new PT_IDs(), new PT_Attributes(), Arrays.asList(new String[] {"RXNORM"}), null);
			
			allRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.ALL.getSourcePropertyNameFSN());
			allCUIRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.CUI_CONCEPTS.getSourcePropertyNameFSN());
			allAUIRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.TERM_CONCEPTS.getSourcePropertyNameFSN());
			
			cpcRefsetConcept_ = ptRefsets_.get("RXNORM").getConcept(cpcRefsetConceptKey_);

			// Add version data to allRefsetConcept
			eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion,  ptContentVersion_.LOADER_VERSION.getUUID(), false);
			eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, ptContentVersion_.RELEASE.getUUID(), false);
			
			semanticTypeStatement = db_.getConnection().prepareStatement("select TUI, ATUI, CVF from RXNSTY where RXCUI = ?");
			conSat = db_.getConnection().prepareStatement("select * from RXNSAT where RXCUI = ? and RXAUI = ? and (SAB='RXNORM' or ATN='NDC')");

			//UMLS and RXNORM do different things with rels - UMLS never has null CUI's, while RxNorm always has null CUI's (when AUI is specified)
			//Also need to join back to MRCONSO to make sure that the target concept is one that we will load with the SAB filter in place.
			cuiRelStatementForward = db_.getConnection().prepareStatement("SELECT r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from RXNREL as r, RXNCONSO "
					+ "WHERE RXCUI2 = ? and RXAUI2 is null and r.SAB='RXNORM' and r.RXCUI1 = RXNCONSO.RXCUI and RXNCONSO.SAB='RXNORM'");

			cuiRelStatementBackward = db_.getConnection().prepareStatement("SELECT r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from RXNREL as r, RXNCONSO "
					+ "WHERE RXCUI1 = ? and RXAUI1 is null and r.SAB='RXNORM' and r.RXCUI2 = RXNCONSO.RXCUI and RXNCONSO.SAB='RXNORM'");

			auiRelStatementForward = db_.getConnection().prepareStatement("SELECT r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF, RXNCONSO.SAB as targetSAB, RXNCONSO.CODE as targetCODE, "
					+ "RXNCONSO.RXCUI as targetCUI from RXNREL as r, RXNCONSO "
					+ " WHERE r.RXCUI2 is null and r.RXAUI2=? and r.SAB='RXNORM' and r.RXAUI1 = RXNCONSO.RXAUI and RXNCONSO.SAB='RXNORM'");

			auiRelStatementBackward = db_.getConnection().prepareStatement("SELECT r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF, RXNCONSO.SAB as targetSAB, RXNCONSO.CODE as targetCODE, "
					+ "RXNCONSO.RXCUI as targetCUI from RXNREL as r, RXNCONSO "
					+ " WHERE r.RXCUI1 is null and r.RXAUI1=? and r.SAB='RXNORM' and r.RXAUI2 = RXNCONSO.RXAUI and RXNCONSO.SAB='RXNORM'");
			
			//Set this to true if you are trying to load on a limited-memory system...
			//ConverterUUID.disableUUIDMap_ = false;
			int cuiCounter = 0;

			Statement statement = db_.getConnection().createStatement();
			ResultSet rs = statement.executeQuery("select RXCUI, LAT, RXAUI, SAUI, SCUI, SAB, TTY, CODE, STR, SUPPRESS, CVF from RXNCONSO " 
					+ "where SAB='RXNORM' order by RXCUI" );
			HashMap<String, ArrayList<RXNCONSO>> conceptData = new HashMap<>();
			while (rs.next())
			{
				RXNCONSO current = new RXNCONSO(rs);
				if (conceptData.size() > 0 && !conceptData.values().iterator().next().get(0).rxcui.equals(current.rxcui))
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
				
				String groupKey = current.sab + "-" + current.code;
				
				ArrayList<RXNCONSO> codeConcepts = conceptData.get(groupKey);
				if (codeConcepts == null)
				{
					codeConcepts = new ArrayList<>();
					conceptData.put(groupKey, codeConcepts);
				}
				
				codeConcepts.add(current);
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
			finish(outputDirectory);
			
			db_.shutdown();
		}
		catch (Exception e)
		{
			throw new MojoExecutionException("Failure during conversion", e);
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

				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new InputStreamReader(zf.getInputStream(ze), "UTF-8"))), null);
			}
			zf.close();

			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxcui_index ON RXNCONSO (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxaui_index ON RXNCONSO (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_rxcui_aui_index ON RXNSAT (RXCUI, RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_aui_index ON RXNSAT (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_rxcui_index ON RXNSTY (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_tui_index ON RXNSTY (TUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxcui2_index ON RXNREL (RXCUI2, RXAUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxaui2_index ON RXNREL (RXCUI1, RXAUI1)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rela_rel_index ON RXNREL (RELA, REL)");  //helps with rel metadata
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_sab_index ON RXNREL (SAB)");  //helps with rel metadata
			s.close();
		}
	}

	private void processCUIRows(HashMap<String, ArrayList<RXNCONSO>> conceptData) throws IOException, SQLException
	{
		String rxCui = conceptData.values().iterator().next().get(0).rxcui;
		
		EConcept cuiConcept = eConcepts_.createConcept(createCUIConceptUUID(rxCui));
		eConcepts_.addAdditionalIds(cuiConcept, rxCui, ptIds_.getProperty("RXCUI").getUUID(), false);

		ArrayList<ValuePropertyPairWithSAB> cuiDescriptions = new ArrayList<>();
		
		for (ArrayList<RXNCONSO> consoWithSameCodeSab : conceptData.values())
		{
			//Use a combination of CUI/SAB/Code here - otherwise, we have problems with the "NOCODE" values
			EConcept codeSabConcept = eConcepts_.createConcept(createCuiSabCodeConceptUUID(consoWithSameCodeSab.get(0).rxcui, 
					consoWithSameCodeSab.get(0).sab, consoWithSameCodeSab.get(0).code));
			
			eConcepts_.addStringAnnotation(codeSabConcept, consoWithSameCodeSab.get(0).code, ptUMLSAttributes_.getProperty("CODE").getUUID(), false);

			String sab = consoWithSameCodeSab.get(0).sab;
			
			ArrayList<ValuePropertyPairWithSAB> codeSabDescriptions = new ArrayList<>();
			
			ArrayList<REL> forwardRelationships = new ArrayList<>();
			ArrayList<REL> backwardRelationships = new ArrayList<>();
		
			for (RXNCONSO rowData : consoWithSameCodeSab)
			{
				//put it in as a string, so users can search for AUI
				eConcepts_.addAdditionalIds(codeSabConcept, rowData.rxaui, ptIds_.getProperty("RXAUI").getUUID(), false);
				
				ValuePropertyPairWithSAB desc = new ValuePropertyPairWithSAB(rowData.str, ptDescriptions_.get(rowData.sab).getProperty(rowData.tty), rowData.sab);
				if (consoWithSameCodeSab.size() > 1)
				{
					desc.addStringAttribute(ptUMLSAttributes_.getProperty("RXAUI").getUUID(), rowData.rxaui);
				}
				
				if (rowData.saui != null)
				{
					desc.addStringAttribute(ptUMLSAttributes_.getProperty("SAUI").getUUID(), rowData.saui);
				}
				if (rowData.scui != null)
				{
					
					desc.addStringAttribute(ptUMLSAttributes_.getProperty("SCUI").getUUID(), rowData.scui);
				}
				
				//drop sab, will never be anything but rxnorm due to query
	
				if (rowData.suppress != null)
				{
					
					desc.addUUIDAttribute(ptUMLSAttributes_.getProperty("SUPPRESS").getUUID(), ptSuppress_.getProperty(rowData.suppress).getUUID());
				}
				
				if (rowData.cvf != null)
				{
					if (rowData.cvf.equals("4096"))
					{
						desc.addRefsetMembership(cpcRefsetConcept_);
					}
					else
					{
						throw new RuntimeException("Unexpected value in RXNCONSO cvf column '" + rowData.cvf + "'");
					}
				}
				// T-ODO handle language - not currently a 'live' todo, because RxNorm only has ENG at the moment.
				if (!rowData.lat.equals("ENG"))
				{
					ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
				}
				
				
				//used for sorting description to find one for the CUI concept
				cuiDescriptions.add(desc);
				//Used for picking the best description for this code/sab concept
				codeSabDescriptions.add(desc);
				
				//Add attributes
				conSat.clearParameters();
				conSat.setString(1, rowData.rxcui);
				conSat.setString(2, rowData.rxaui);
				ResultSet rs = conSat.executeQuery();
				processSAT(codeSabConcept.getConceptAttributes(), rs, rowData.code, rowData.sab, consoWithSameCodeSab.size() == 1);
				
				auiRelStatementForward.clearParameters();
				auiRelStatementForward.setString(1, rowData.rxaui);
				forwardRelationships.addAll(REL.read(rowData.sab, auiRelStatementForward.executeQuery(), true, this));
				
				auiRelStatementBackward.clearParameters();
				auiRelStatementBackward.setString(1, rowData.rxaui);
				backwardRelationships.addAll(REL.read(rowData.sab, auiRelStatementBackward.executeQuery(), false, this));
			}
			
			eConcepts_.addRelationship(codeSabConcept, cuiConcept.getPrimordialUuid(), ptUMLSRelationships_.UMLS_CUI.getUUID(), null);
			addRelationships(codeSabConcept, forwardRelationships);
			addRelationships(codeSabConcept, backwardRelationships);
			
			eConcepts_.addRefsetMember(allRefsetConcept_, codeSabConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(allAUIRefsetConcept_, codeSabConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(ptRefsets_.get(sab).getConcept(terminologyCodeRefsetPropertyName_.get(sab)) , codeSabConcept.getPrimordialUuid(), null, true, null);
			
			List<TkDescription> addedDescriptions = eConcepts_.addDescriptions(codeSabConcept, codeSabDescriptions);
			ValuePropertyPairWithAttributes.processAttributes(eConcepts_, codeSabDescriptions, addedDescriptions);
			codeSabConcept.writeExternal(dos_);
			//disabled debug code
			//conceptUUIDsCreated_.add(codeSabConcept.getPrimordialUuid());
		}
		
		//Pick the 'best' description to use on the cui concept
		Collections.sort(cuiDescriptions);
		eConcepts_.addDescription(cuiConcept, cuiDescriptions.get(0).getValue(), DescriptionType.FSN, true, cuiDescriptions.get(0).getProperty().getUUID(), 
				cuiDescriptions.get(0).getProperty().getPropertyType().getPropertyTypeReferenceSetUUID(), false);
		
		//there are no attributes in rxnorm without an AUI.
		
		//add semantic types
		semanticTypeStatement.clearParameters();
		semanticTypeStatement.setString(1, rxCui);
		ResultSet rs = semanticTypeStatement.executeQuery();
		processSemanticTypes(cuiConcept, rs);
		
		cuiRelStatementForward.clearParameters();
		cuiRelStatementForward.setString(1, rxCui);
		addRelationships(cuiConcept, REL.read(null, cuiRelStatementForward.executeQuery(), true, this));
		
		cuiRelStatementBackward.clearParameters();
		cuiRelStatementBackward.setString(1, rxCui);
		addRelationships(cuiConcept, REL.read(null, cuiRelStatementBackward.executeQuery(), false, this));

		eConcepts_.addRefsetMember(allRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		eConcepts_.addRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		cuiConcept.writeExternal(dos_);
		//disabled debug code
		//conceptUUIDsCreated_.add(cuiConcept.getPrimordialUuid());
	}

	@Override
	protected void processRelCVFAttributes(TkRelationship r, List<REL> duplicateRelationships)
	{
		TkRefexUuidMember member = null;
		for (REL dupeRel : duplicateRelationships)
		{
			if (dupeRel.getCvf() != null)
			{
				if (dupeRel.getCvf().equals("4096"))
				{
					if (member == null)
					{
						member = eConcepts_.addRefsetMember(cpcRefsetConcept_, r.getPrimordialComponentUuid(), null, true, null);
						eConcepts_.addStringAnnotation(member, dupeRel.getSourceTargetAnnotationLabel(), 
								ptRelationshipMetadata_.getProperty("sAUI & tAUI").getUUID(), false);
					}
					else
					{
						eConcepts_.addStringAnnotation(member, dupeRel.getSourceTargetAnnotationLabel(), 
								ptRelationshipMetadata_.getProperty("sAUI & tAUI").getUUID(), false);
					}
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + dupeRel.getCvf() + "'");
				}
			}
		}
	}
	
	@Override
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
		return new Property(null, fsnName, preferredName, null, false, descriptionRanking);
	}

	@Override
	protected void allDescriptionsCreated(String sab)
	{
		//noop
	}

	@Override
	protected void loadCustomMetaData() throws Exception
	{
		//noop
	}

	@Override
	protected void addCustomRefsets(BPT_Refsets refset) throws Exception
	{
		refset.addProperty(cpcRefsetConceptKey_);
	}

	@Override
	protected void processSAT(TkComponent<?> itemToAnnotate, ResultSet rs, String itemCode, String itemSab, boolean skipAuiAnnotation) throws SQLException
	{
		while (rs.next())
		{
			//String rxcui = rs.getString("RXCUI");
			String rxaui = rs.getString("RXAUI");
			//String stype = rs.getString("STYPE");
			String code = rs.getString("CODE");
			String atui = rs.getString("ATUI");
			String satui = rs.getString("SATUI");
			String atn = rs.getString("ATN");
			String sab = rs.getString("SAB");
			String atv = rs.getString("ATV");
			String suppress = rs.getString("SUPPRESS");
			String cvf = rs.getString("CVF");
			
			//for some reason, ATUI isn't always provided - don't know why.  fallback on randomly generated in those cases.
			//You would expect that ptTermAttributes_.get() would be looking up sab, rather than having RxNorm hardcoded... but this is an oddity of 
			//a hack we are doing within the RxNorm load.
			TkRefsetStrMember attribute = eConcepts_.addStringAnnotation(itemToAnnotate, 
					(atui == null ? null : ConverterUUID.createNamespaceUUIDFromString("ATUI" + atui)), atv, 
					ptTermAttributes_.get("RXNORM").getProperty(atn).getUUID(), false, null);
			
			if (atui != null)
			{
				eConcepts_.addAdditionalIds(attribute, atui, ptIds_.getProperty("ATUI").getUUID());
			}
			
			//dropping for space savings
//			if (stype != null)
//			{
//				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(stype).getUUID(), ptUMLSAttributes_.getProperty("STYPE").getUUID());
//			}
			
			if (code != null)
			{
				//Only load the code if it is different than the code of the item we are putting this attribute on.
				if (itemCode == null || !itemCode.equals(code))
				{
					eConcepts_.addStringAnnotation(attribute, code, ptUMLSAttributes_.getProperty("CODE").getUUID(), false);
				}
			}

			if (satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, satui, ptUMLSAttributes_.getProperty("SATUI").getUUID(), false);
			}
			
			//only load the sab if it is different than the sab of the item we are putting this attribute on
			if (itemSab == null || !itemSab.equals(sab))
			{
				eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(sab).getUUID(), ptUMLSAttributes_.getProperty("SAB").getUUID());
			}
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(suppress).getUUID(), ptUMLSAttributes_.getProperty("SUPPRESS").getUUID());
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
			
			if (!skipAuiAnnotation)
			{
				//Add an attribute that says which AUI this attribute came from
				eConcepts_.addStringAnnotation(attribute, rxaui, ptUMLSAttributes_.getProperty("RXAUI").getUUID(), false);
			}
		}
		rs.close();
	}
	
	public static void main(String[] args) throws MojoExecutionException
	{
		RxNormMojo mojo = new RxNormMojo();
		mojo.outputDirectory = new File("../RxNorm-econcept/target");
		mojo.srcDataPath = new File("../RxNorm-econcept/target/generated-resources/src/");
		mojo.execute();
	}
}
