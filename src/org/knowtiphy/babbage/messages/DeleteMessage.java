package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;

import java.util.Collection;

public class DeleteMessage implements IMessage
{
	private final String accountID;
	private final String folderID;
	private final Collection<String> mIDs;

	public DeleteMessage(String accountID, String folderID, Collection<String> mIDs)
	{
		this.accountID = accountID;
		this.folderID = folderID;
		this.mIDs = mIDs;
	}

	@Override
	public void perform(IStorage storage)
	{
		storage.deleteMessages(accountID, folderID, mIDs);
	}
}
