package org.knowtiphy.babbage.storage.IMAP;

import javax.mail.Folder;
import javax.mail.MessagingException;
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
		LOGGER.info("PingThread :: start");
		while (true)
		{
			try
			{
				LOGGER.info("PingThread :: sleep");
				Thread.sleep(frequency);
				LOGGER.info("PingThread :: Pinging server");
				folder.getMessageCount();
				adapter.rewatch(folder);
			}
			catch ( MessagingException ex)
			{
				System.out.println("PING THREAD ISSUE");
				ex.printStackTrace();
			}
			catch (InterruptedException ex)
			{
				LOGGER.info("PingThread :: Exiting");
				return;
			}
//			catch (StoreClosedException ex)
//			{
//				LOGGER.info("PingThread :: StoreClosedException");
//				adapter.addPriorityWork(() -> {
//					adapter.reconnect();
//					return null;
//				});
//			}
//			catch (FolderClosedException ex)
//			{
//				LOGGER.info("PingThread :: FolderClosedException");
//				adapter.addPriorityWork(() -> adapter.recoverFromClosedFolder(folder));
//			}
//			catch (MessagingException ex)
//			{
//				LOGGER.warning(() -> "PingThread :: Pinging server failed " + ex.getLocalizedMessage());
//				adapter.addWork(() ->
//				{
//					adapter.reStartPingThread();
//					return null;
//				});
//			}
		}
	}
}