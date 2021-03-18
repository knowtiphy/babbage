package org.knowtiphy.babbage.storage.IMAP;

import javax.mail.Folder;
import javax.mail.MessagingException;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class Ping implements Runnable
{
	private static final Logger LOGGER = Logger.getLogger(Ping.class.getName());

	private final IMAPAdapter adapter;
	private final Folder folder;

	public Ping(IMAPAdapter adapter, Folder folder)
	{
		this.adapter = adapter;
		this.folder = folder;
	}

	@Override
	public void run()
	{
		try
		{
			LOGGER.info("PingThread :: Pinging server");
			folder.getMessageCount();
			adapter.rewatch(folder);
		}
		catch (MessagingException ex)
		{
			System.out.println("PING THREAD ISSUE");
			ex.printStackTrace();
		}
		catch (Throwable ex)
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