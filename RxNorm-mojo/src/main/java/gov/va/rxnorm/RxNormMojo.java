package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.sql.ColumnDefinition;
import gov.va.oia.terminology.converters.umlsUtils.sql.DataType;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.dwfa.cement.ArchitectonicAuxiliary;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

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

			// Set up the DB for loading the temp data
			RRFDatabaseHandle db = new RRFDatabaseHandle();
			new File(outputDirectory, "rrfDB.h2.db").delete();
			db.createDatabase(new File(outputDirectory, "rrfDB"));

			SAXBuilder builder = new SAXBuilder();
			Document d = builder.build(RxNormMojo.class.getResourceAsStream("/RxNormTableDefinitions.xml"));
			Element root = d.getRootElement();

			ArrayList<TableDefinition> tables = new ArrayList<>();

			for (Element table : root.getChildren())
			{
				TableDefinition td = new TableDefinition(table.getAttributeValue("name"));
				for (Element column : table.getChildren())
				{
					Integer size = null;
					if (column.getAttributeValue("size") != null)
					{
						size = Integer.parseInt(column.getAttributeValue("size"));
					}
					Boolean allowNull = null;
					if (column.getAttributeValue("allowNull") != null)
					{
						allowNull = Boolean.valueOf(column.getAttributeValue("allowNull"));
					}
					td.addColumn(new ColumnDefinition(column.getAttributeValue("name"), new DataType(column.getAttributeValue("type"), size, allowNull)));
				}
				tables.add(td);
				db.createTable(td);
			}

			for (TableDefinition td : tables)
			{
				ConsoleUtil.println("Creating table " + td.getTableName());
				StringBuilder insert = new StringBuilder();
				insert.append("INSERT INTO ");
				insert.append(td.getTableName());
				insert.append("(");
				for (ColumnDefinition cd : td.getColumns())
				{
					insert.append(cd.getColumnName());
					insert.append(",");
				}
				insert.setLength(insert.length() - 1);
				insert.append(") VALUES (");
				for (ColumnDefinition cd : td.getColumns())
				{
					insert.append("?,");
				}
				insert.setLength(insert.length() - 1);
				insert.append(")");

				PreparedStatement ps = db.getConnection().prepareStatement(insert.toString());

				ConsoleUtil.println("Loading table " + td.getTableName());
				BufferedReader br = Files.newBufferedReader(new File(srcDataPath, "rrf/" + td.getTableName() + ".RRF").toPath(), Charset.forName("UTF-8"));
				String line = null;
				while ((line = br.readLine()) != null)
				{
					String[] cols = line.split("\\|", -1);
					//-1 is because the files have a trailing seperator, with no data after it
					if (cols.length - 1 != td.getColumns().size())
					{
						throw new RuntimeException("Data length mismatch!");
					}
					
					ps.clearParameters();
					int i = 1;
					for (String s : cols)
					{
						if (i > td.getColumns().size() && (s == null || s.length() == 0))
						{
							//skip the last blank one
							break;
						}
						DataType colType = td.getColumns().get(i - 1).getDataType(); 
						if (colType.isBoolean())
						{
							if (s == null || s.length() == 0)
							{
								ps.setNull(i, Types.BOOLEAN);
							}
							else
							{
								ps.setBoolean(i, Boolean.valueOf(s));
							}
						}
						else if (colType.isInteger())
						{
							if (s == null || s.length() == 0)
							{
								ps.setNull(i, Types.INTEGER);
							}
							else
							{
								ps.setInt(i, Integer.parseInt(s));
							}
						}
						else if (colType.isLong())
						{
							if (s == null || s.length() == 0)
							{
								ps.setNull(i, Types.BIGINT);
							}
							else
							{
								ps.setLong(i, Long.parseLong(s));
							}
						}
						else if (colType.isString())
						{
							if (s == null || s.length() == 0)
							{
								ps.setNull(i, Types.VARCHAR);
							}
							else
							{
								ps.setString(i, s);
							}
						}
						else
						{
							throw new RuntimeException("oops");
						}
						i++;
					}
					ps.execute();
				}
				ps.close();
				br.close();
			}

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

	public static void main(String[] args) throws MojoExecutionException
	{
		RxNormMojo mojo = new RxNormMojo();
		mojo.outputDirectory = new File("../RxNorm-econcept/target");
		mojo.srcDataPath = new File("../RxNorm-econcept/target/generated-resources/src/");
		mojo.execute();
	}
}
