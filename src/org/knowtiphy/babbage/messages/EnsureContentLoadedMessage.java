package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;

public class EnsureContentLoadedMessage implements IMessage
{
	private final String accountID;
	private final String folderID;
	private final String mID;

	public EnsureContentLoadedMessage(String accountID, String folderID, String mID)
	{
		this.accountID = accountID;
		this.folderID = folderID;
		this.mID = mID;
	}

	@Override
	public void perform(IStorage storage) throws NoSuchAccountException
	{
		storage.ensureMessageContentLoaded(accountID, folderID, mID);
	}
}
