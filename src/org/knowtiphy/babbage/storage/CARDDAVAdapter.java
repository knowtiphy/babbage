package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;

import java.util.concurrent.BlockingDeque;

public class CARDDAVAdapter extends BaseAdapter
{
	// Start modeling after what is currently in the IMAPAdpater
	public CARDDAVAdapter(Dataset messageDatabase, ListenerManager listenerManager,
						  BlockingDeque<Runnable> notificationQ)
	{
		super(messageDatabase, listenerManager, notificationQ);
	}

	public void close()
	{

	}

	@Override
	public String getId()
	{
		return null;
	}
}
