package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.dwfa.cement.ArchitectonicAuxiliary;

/**
 * Goal to build RxNorm
 * 
 * @goal buildRxNorm
 * 
 * @phase process-sources
 */
public class RxNormMojo extends AbstractMojo
{
	private String problemListNamespaceSeed_ = "gov.va.med.term.RxNorm";
	private EConceptUtility eConcepts_;
	private ArrayList<PropertyType> propertyTypes_ = new ArrayList<PropertyType>();
	private DataOutputStream dos_;
	private RRFDatabaseHandle db_;

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
			ConverterUUID.enableDupeUUIDException_ = true;
			eConcepts_ = new EConceptUtility(problemListNamespaceSeed_, "RxNorm Path", dos_);

			UUID archRoot = ArchitectonicAuxiliary.Concept.ARCHITECTONIC_ROOT_CONCEPT.getPrimoridalUid();
			UUID metaDataRoot = ConverterUUID.createNamespaceUUIDFromString("metadata");
			eConcepts_.createAndStoreMetaDataConcept(metaDataRoot, "RxNorm Metadata", archRoot, dos_);

			// eConcepts_.loadMetaDataItems(propertyTypes_, metaDataRoot, dos_);

			// eConcepts_.storeRefsetConcepts(refsets, dos_);

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
			// try
			// {
			// dataReader.close();
			// }
			// catch (IOException e)
			// {
			// //noop
			// }
		}
	}

	private void loadDatabase() throws Exception
	{
		// Set up the DB for loading the temp data
		db_ = new RRFDatabaseHandle();
		new File(outputDirectory, "rrfDB.h2.db").delete();
		db_.createDatabase(new File(outputDirectory, "rrfDB"));

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

			db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new InputStreamReader(zf.getInputStream(ze)))));
		}
		zf.close();
	}

	public static void main(String[] args) throws MojoExecutionException
	{
		RxNormMojo mojo = new RxNormMojo();
		mojo.outputDirectory = new File("../RxNorm-econcept/target");
		mojo.srcDataPath = new File("../RxNorm-econcept/target/generated-resources/src/");
		mojo.execute();
	}
}
