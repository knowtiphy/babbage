package org.knowtiphy.babbage.storage.exceptions;

public class MalformedOperationSpecifiedException extends StorageException
{
	private String accountID;

	public MalformedOperationSpecifiedException()
	{
		super("Malformed Operation Specified");
	}
}
