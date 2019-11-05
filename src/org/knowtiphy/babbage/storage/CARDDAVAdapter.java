package org.knowtiphy.babbage.storage;

import javax.mail.Folder;
import javax.mail.MessagingException;

public class CARDDAVAdapter extends BaseAdapter
{
	// Start modeling after what is currently in the IMAPAdpater
	public CARDDAVAdapter()
	{
	}

	@Override public String getId()
	{
		return null;
	}

	@Override public String encode(Folder folder) throws MessagingException
	{
		return null;
	}
}
