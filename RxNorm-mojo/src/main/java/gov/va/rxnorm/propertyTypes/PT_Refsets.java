package gov.va.rxnorm.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Refsets extends BPT_Refsets
{
	public enum Refsets
	{
		ALL("All RxNorm Concepts"),
		CPC("Current Prescribable Content"),
		CUI_CONCEPTS("All RxNorm CUI Concepts"),
		AUI_CONCEPTS("All RxNorm AUI Concepts");

		private Property property;

		private Refsets(String niceName)
		{
			// Don't know the owner yet - will be autofilled when we add this to the parent, below.
			property = new Property(null, niceName);
		}

		public Property getProperty()
		{
			return property;
		}
	}
	
	public PT_Refsets(String terminologyName)
	{
		super(terminologyName);
		for (Refsets mm : Refsets.values())
		{
			addProperty(mm.getProperty());
		}
	}
}
