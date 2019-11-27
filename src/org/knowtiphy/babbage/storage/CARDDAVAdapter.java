package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;

import javax.mail.Folder;
import javax.mail.MessagingException;
import java.util.concurrent.BlockingDeque;

public class CARDDAVAdapter extends BaseAdapter
{
	// Start modeling after what is currently in the IMAPAdpater
	public CARDDAVAdapter(Dataset messageDatabase, ListenerManager listenerManager,
						  BlockingDeque<Runnable> notificationQ)
	{
		super(messageDatabase, listenerManager, notificationQ);
	}

	@Override
	public String getId()
	{
		return null;
	}

	@Override
	public String encode(Folder folder) throws MessagingException
	{
		return null;
	}
}
