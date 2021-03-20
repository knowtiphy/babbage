package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;

import java.util.Collection;

public class MarkAsJunkMessage implements IMessage
{
	private final String accountID;
	private final String folderID;
	private final Collection<String> mIDs;
	private final boolean flag;

	public MarkAsJunkMessage(String accountID, String folderID, Collection<String> mIDs, boolean flag)
	{
		this.accountID = accountID;
		this.folderID = folderID;
		this.mIDs = mIDs;
		this.flag = flag;
	}

	@Override public void perform(IStorage storage) throws NoSuchAccountException
	{
		storage.markMessagesAsJunk(accountID, folderID, mIDs, flag);
	}
}
