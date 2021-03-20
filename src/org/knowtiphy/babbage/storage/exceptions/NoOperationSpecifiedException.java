package org.knowtiphy.babbage.storage.exceptions;

public class NoOperationSpecifiedException extends StorageException
{
	private String accountID;

	public NoOperationSpecifiedException()
	{
		super("No Operation Specified");
	}
}
