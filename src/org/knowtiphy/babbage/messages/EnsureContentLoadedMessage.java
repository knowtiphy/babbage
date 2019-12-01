package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;

public class EnsureContentLoadedMessage implements IMessage
{
	private final String accountID;
	private final String folderID;
	private final String mID;
	private final boolean immediate;

	public EnsureContentLoadedMessage(String accountID, String folderID, String mID, boolean immediate)
	{
		this.accountID = accountID;
		this.folderID = folderID;
		this.mID = mID;
		this.immediate = immediate;
	}

	@Override
	public void perform(IStorage storage)
	{
		storage.ensureMessageContentLoaded(accountID, folderID, mID, immediate);
	}
}
