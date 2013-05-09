package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion.BaseContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.dwfa.cement.ArchitectonicAuxiliary;
import org.dwfa.util.id.Type3UuidFactory;
import org.ihtsdo.etypes.EConcept;

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
			File binaryOutputFile = new File(outputDirectory, "RxNorm.jbin");
			
			dos_ = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(binaryOutputFile)));
			ConverterUUID.enableDupeUUIDException_ = true;
			eConcepts_ = new EConceptUtility(problemListNamespaceSeed_, "RxNorm Path", dos_);
			
			UUID archRoot = ArchitectonicAuxiliary.Concept.ARCHITECTONIC_ROOT_CONCEPT.getPrimoridalUid();
			UUID metaDataRoot = ConverterUUID.createNamespaceUUIDFromString("metadata");
			eConcepts_.createAndStoreMetaDataConcept(metaDataRoot, "RxNorm Metadata", archRoot, dos_);
			
			System.out.println("Hello world");
			//eConcepts_.loadMetaDataItems(propertyTypes_, metaDataRoot, dos_);

			//eConcepts_.storeRefsetConcepts(refsets, dos_);
			
			dos_.close();
			
			ConsoleUtil.println("Load Statistics");
			for (String s : eConcepts_.getLoadStats().getSummary())
			{
				ConsoleUtil.println(s);
			}
			
			//this could be removed from final release.  Just added to help debug editor problems.
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
//			try
//			{
//				dataReader.close();
//			}
//			catch (IOException e)
//			{
//				//noop
//			}
		}
	}
}
