package org.knowtiphy.babbage.storage.IMAP;

import javax.mail.Folder;
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
//		try
//		{
//			LOGGER.info("Pinging server");
//			folder.getMessageCount();
//		}
//		catch (FolderClosedException ex)
//		{
//			LOGGER.warning("Ping :: Folder Closed Exception :: " + ex.getLocalizedMessage());
//			adapter.addPriorityWork(() ->
//			{
//				adapter.reStartPingThread();
//				return null;
//			}, SYNCH_PRIORITY);
//		}
//		catch (MessagingException ex)
//		{
//			LOGGER.warning("Pinging server failed :: " + ex.getLocalizedMessage());
//			adapter.addPriorityWork(() ->
//			{
//				adapter.reStartPingThread();
//				return null;
//			}, SYNCH_PRIORITY);
//		}
//		catch (Exception ex)
//		{
//			System.out.println("XXXXXXXXXXXXXXXXXXXXXXX bailing");
//			ex.printStackTrace();
//		}
	}
}