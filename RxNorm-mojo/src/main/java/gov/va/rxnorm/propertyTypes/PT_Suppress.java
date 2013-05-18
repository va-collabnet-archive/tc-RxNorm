package gov.va.rxnorm.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Suppress extends PropertyType
{
	public PT_Suppress()
	{
		super("Suppress");
		addProperty("N", null, "Not Suppressible");
		addProperty("O", null, "Specific individual names (atoms) set as Obsolete because the name is no longer provided by the original source");
		addProperty("Y", null, "Suppressed by RxNorm editor");
		addProperty("E", null, "Unquantified, non-prescribable drug with related quantified, prescribable drugs. NLM strongly recommends that users not alter editor-assigned suppressibility");

	}
}
