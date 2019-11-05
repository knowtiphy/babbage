package org.knowtiphy.babbage.storage;

import javax.mail.Folder;
import javax.mail.MessagingException;
import java.util.logging.Logger;

/**
 * @author graham
 */
public class PingServer implements Runnable
{
	private static final Logger logger = Logger.getLogger(PingServer.class.getName());

	private final IMAPAdapter IMAPAdapter;
	private final Folder folder;
	private final long frequency;

	public PingServer(IMAPAdapter IMAPAdapter, Folder folder, long frequency)
	{
		this.IMAPAdapter = IMAPAdapter;
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
				//noinspection BusyWait
				Thread.sleep(frequency);
				logger.info("Pinging server");
				folder.getMessageCount();
			} catch (InterruptedException ex)
			{
				return;
			} catch (MessagingException ex)
			{
				logger.warning("Pinging server failed :: " + ex.getLocalizedMessage());
				IMAPAdapter.addWork(() ->
				{
					IMAPAdapter.reStartPingThread();
					return null;
				});
			}
		}
	}
}