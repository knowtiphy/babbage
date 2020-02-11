package org.knowtiphy.babbage.storage.IMAP;

import javax.mail.Folder;
import javax.mail.FolderClosedException;
import javax.mail.MessagingException;
import javax.mail.StoreClosedException;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class PingThread implements Runnable
{
	private static final Logger LOGGER = Logger.getLogger(PingThread.class.getName());

	private final IMAPAdapter adapter;
	private final Folder folder;
	private final long frequency;

	public PingThread(IMAPAdapter adapter, Folder folder, long frequency)
	{
		this.adapter = adapter;
		this.folder = folder;
		this.frequency = frequency;
	}

	@Override
	public void run()
	{
		while (true)
		{
			try
			{
				Thread.sleep(frequency);
				LOGGER.info("PingThread :: Pinging server");
				folder.getMessageCount();
			}
			catch (InterruptedException ex)
			{
				LOGGER.info("PingThread :: Exiting");
				return;
			}
			catch (StoreClosedException ex)
			{
				adapter.addPriorityWork(() -> {
					adapter.reconnect();
					return null;
				});
			}
			catch (FolderClosedException ex)
			{
				adapter.addPriorityWork(() -> adapter.recoverFromClosedFolder(folder));
			}
			catch (MessagingException ex)
			{
				LOGGER.warning(() -> "PingThread :: Pinging server failed " + ex.getLocalizedMessage());
				adapter.addWork(() ->
				{
					adapter.reStartPingThread();
					return null;
				});
			}
		}
	}
}