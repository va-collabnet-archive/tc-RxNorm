package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ValuePropertyPair;

public class ValuePropertyPairWithSAB extends ValuePropertyPair
{

	private String sab_;
	
	public ValuePropertyPairWithSAB(String value, Property property, String sab)
	{
		super(value, property);
		sab_ = sab;
	}

	@Override
	public int compareTo(ValuePropertyPair o)
	{
		//Boosting descriptions that come from RXNORM up to the very top.
		if (sab_.equals("RXNORM") && !((ValuePropertyPairWithSAB)o).sab_.equals("RXNORM"))
		{
			return -1;
		}
		else if (!sab_.equals("RXNORM") && ((ValuePropertyPairWithSAB)o).sab_.equals("RXNORM"))
		{
			return 1;
		}
		return super.compareTo(o);
	}
}
