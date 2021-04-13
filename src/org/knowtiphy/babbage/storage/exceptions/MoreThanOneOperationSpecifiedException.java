package org.knowtiphy.babbage.storage.exceptions;

public class MoreThanOneOperationSpecifiedException extends StorageException
{
	private String accountID;

	public MoreThanOneOperationSpecifiedException()
	{
		super("More Than One Operation Specified");
	}
}
