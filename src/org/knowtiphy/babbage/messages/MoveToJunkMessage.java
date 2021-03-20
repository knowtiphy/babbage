package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;

import java.util.Collection;

public class MoveToJunkMessage implements IMessage
{
	private final String accountID;
	private final String sourceFolderID;
	private final Collection<String> mIDs;
	private final String targetFolderID;
	private final boolean flag;

	public MoveToJunkMessage(String accountID, String sourceFolderID, Collection<String> mIDs, String targetFolderID,
			boolean flag)
	{
		this.accountID = accountID;
		this.sourceFolderID = sourceFolderID;
		this.mIDs = mIDs;
		this.targetFolderID = targetFolderID;
		this.flag = flag;
	}

	@Override public void perform(IStorage storage) throws NoSuchAccountException
	{
		storage.moveMessagesToJunk(accountID, sourceFolderID, mIDs, targetFolderID, flag);
	}
}
