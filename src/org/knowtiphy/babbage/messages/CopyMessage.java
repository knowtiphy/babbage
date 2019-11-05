package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;

import java.util.Collection;

public class CopyMessage implements IMessage
{
	private final String accountID;
	private final String sourceFolderID;
	private final Collection<String> mIDs;
	private final String targetFolderID;
	private final boolean flag;

	public CopyMessage(String accountID, String sourceFolderID, Collection<String> mIDs, String targetFolderID,
					   boolean flag)
	{
		this.accountID = accountID;
		this.sourceFolderID = sourceFolderID;
		this.mIDs = mIDs;
		this.targetFolderID = targetFolderID;
		this.flag = flag;
	}

	@Override
	public void perform(IStorage storage)
	{
		storage.copyMessages(accountID, sourceFolderID, mIDs, targetFolderID, flag);
	}
}
