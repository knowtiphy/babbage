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
			LOGGER.info("Pinging server");
			int  x = folder.getUnreadMessageCount();
		}
		catch (MessagingException ex)
		{
			LOGGER.warning("Pinging server failed :: " + ex.getLocalizedMessage());
			adapter.addWork(() ->
			{
				adapter.reStartPingThread();
				return null;
			});
		}
		catch (Exception ex)
		{
			System.out.println("XXXXXXXXXXXXXXXXXXXXXXX bailing");
			ex.printStackTrace();
		}
	}
}