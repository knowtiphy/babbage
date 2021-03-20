package org.knowtiphy.babbage.storage.exceptions;

public class NoSuchAccountException extends StorageException
{
	private String accountID;

	public NoSuchAccountException(String accountID)
	{
		super("No Such Account :: " + accountID);
		this.accountID = accountID;
	}

	public String getAccountID()
	{
		return accountID;
	}
}
