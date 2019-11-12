package org.knowtiphy.babbage.storage;

import biweekly.Biweekly;
import biweekly.ICalendar;
import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CALDAVAdapter extends BaseAdapter
{
	private static final Logger LOGGER = Logger.getLogger(CALDAVAdapter.class.getName());
	private static final Runnable POISON_PILL = () -> {
	};

	private final Sardine sardine;

	private final String serverName;
	private final String emailAddress;
	private final String password;
	private final String id;

	//	RDF ids to Java folder and message objects
	private final Map<URI, DavResource> m_Calendar = new HashMap<>(100);
	private final Map<Folder, Map<String, Message>> m_PerCalendarEvent = new HashMap<>(1000);

	private final BlockingQueue<Runnable> workQ;
	//private final BlockingQueue<Runnable> contentQ;
	private final Thread doWork;
	private final ExecutorService doContent;
	private final Mutex accountLock;
	private final Dataset messageDatabase;
	private final ListenerManager listenerManager;
	private final BlockingDeque<Runnable> notificationQ;
	private final Model model;

	public CALDAVAdapter(String name, Dataset messageDatabase, ListenerManager listenerManager,
			BlockingDeque<Runnable> notificationQ, Model model) throws InterruptedException
	{
		System.out.println("CALDAVAdapter INSTANTIATED");

		this.messageDatabase = messageDatabase;
		this.listenerManager = listenerManager;
		this.notificationQ = notificationQ;
		this.model = model;

		// Query for serverName, emailAdress, and password
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));

		// Query for these with the passed in model
		this.serverName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_NAME));
		this.emailAddress = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		this.password = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_PASSWORD));

		this.id = Vocabulary.E(Vocabulary.CALDAV_ACCOUNT, emailAddress);

		sardine = SardineFactory.begin(emailAddress, password);

		accountLock = new Mutex();
		accountLock.lock();

		workQ = new LinkedBlockingQueue<>();
		doWork = new Thread(new Worker(workQ));
		// Figure out what this guy is doing
		doContent = Executors.newCachedThreadPool();
	}

	@Override public String getId()
	{
		return id;
	}

	@Override public void close()
	{

	}

	private void notifyListeners(TransactionRecorder recorder)
	{
		notificationQ.addLast(() -> listenerManager.notifyChangeListeners(recorder));
	}

	private void startFolderWatchers() throws MessagingException, IOException
	{
		// Each account needs to have its folders now
		// start a watcher for each one
		assert m_Calendar.isEmpty();

//		List<DavResource> calDavResources = sardine.list(serverName);
//		List<ICalendar> iCals = new ArrayList<>();
//
//		for (int i = 1; i < calDavResources.size(); i++)
//		{
//			DavResource res = calDavResources.get(i);
//			m_Calendar.put(res.getHref(),  res.getCustomProps().get("getctag"));
//
//			InputStream stream = sardine.get(calDavHeader + res);
//			ICalendar iCal = Biweekly.parse(stream).first();
//			// Stream might actually already auto sync somehow?
//			//stream.close();
//
//			if (!iCal.getEvents().isEmpty())
//			{
//				iCals.add(iCal);
//			}
//		}

		// Will make this pretty later and make it into a method of its own
//		for (Folder folder : m_folder.values())
//		{
//			LOGGER.log(Level.INFO, "Starting watcher for {0}", folder.getName());
//			folder.addMessageCountListener(new IMAPAdapter.WatchCountChanges(this, folder));
//			folder.addMessageChangedListener(new IMAPAdapter.WatchMessageChanges(this, folder));
//			try
//			{
//				idleManager.watch(folder);
//			} catch (MessagingException e)
//			{
//				LOGGER.warning(e.getLocalizedMessage());
//			}
//		}
	}

//	@Override public FutureTask<?> getSynchTask() throws UnsupportedOperationException
//	{
//		return new FutureTask<Void>(() ->
//		{
//			//startFolderWatchers();
//			// == To start a watcher that polls calendar c tag changes
//
//			TransactionRecorder recorder = new TransactionRecorder();
//			//synchronizeFolders(recorder);
//			// == To beginning the process of grabbing the calendars
//
//			notifyListeners(recorder);
//
//			for (Folder folder : m_Calendar.values())
//			{
//				TransactionRecorder recorder1 = new TransactionRecorder();
//				//synchMessageIdsAndHeaders(folder, recorder1);
//				// == to Synching the Event IDs?
//				notifyListeners(recorder);
//			}
//
//
//			accountLock.unlock();
//			LOGGER.log(Level.INFO, "{0} :: SYNCH DONE ", emailAddress);
//			return null;
//		});
//	}

	// Factor this out to its own class eventually, since all Adapters will use
	private void ensureMapsLoaded() throws InterruptedException
	{
		accountLock.lock();
		accountLock.unlock();
	}

	private class Worker implements Runnable
	{
		private final BlockingQueue<? extends Runnable> queue;

		Worker(BlockingQueue<? extends Runnable> queue)
		{
			this.queue = queue;
		}

		@Override public void run()
		{
			while (true)
			{
				try
				{
					Runnable task = queue.take();
					if (task == POISON_PILL)
					{
						return;
					}
					else
					{
						ensureMapsLoaded();

						try
						{
							task.run();
						} catch (RuntimeException ex)
						{
							LOGGER.warning(ex.getLocalizedMessage());
						}
					}
				} catch (InterruptedException e)
				{
					return;
				}
			}
		}
	}
}
