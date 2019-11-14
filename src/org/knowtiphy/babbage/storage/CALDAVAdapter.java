package org.knowtiphy.babbage.storage;

import biweekly.Biweekly;
import biweekly.ICalendar;
import biweekly.component.VEvent;
import biweekly.property.LastModified;
import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.StmtIterator;
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

import static org.knowtiphy.babbage.storage.DStore.P;
import static org.knowtiphy.babbage.storage.DStore.R;

public class CALDAVAdapter extends BaseAdapter
{
	private static final Logger LOGGER = Logger.getLogger(CALDAVAdapter.class.getName());
	private static final Runnable POISON_PILL = () -> {
	};

	private final Sardine sardine;

	private final String serverName;
	private final String emailAddress;
	private final String password;
	private final String serverHeader;

	private final String id;

	//	RDF ids to Java folder and message objects
	private final Map<URI, ICalendar> m_Calendar = new HashMap<>(5);
	private final Map<URI, Map<URI, VEvent>> m_PerCalendarEvent = new HashMap<>(1000);

	// Possibly redundant maps
	private final Map<URI, String> calendarSyncMap = new HashMap<>(5);
	private final Map<URI, Map<URI, String>> eventsSyncMap = new HashMap<>(1000);

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
		this.serverHeader = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_HEADER));

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

	@Override public void addListener(Model accountTriples)
	{
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.RDF_TYPE),
				P(accountTriples, Vocabulary.CALDAV_ACCOUNT));
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_NAME),
				serverName);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_EMAIL_ADDRESS),
				emailAddress);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_PASSWORD),
				password);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_HEADER),
				serverHeader);

	}

	private void notifyListeners(TransactionRecorder recorder)
	{
		notificationQ.addLast(() -> listenerManager.notifyChangeListeners(recorder));
	}

	public String encode(ICalendar calendar) throws MessagingException
	{
		return Vocabulary.E(Vocabulary.CALDAV_CALENDAR, getEmailAddress(), calendar.getUid().getValue());
	}

	public String getEmailAddress()
	{
		return emailAddress;
	}


	private void syncCalendars()
	{

	}

	private void syncEvents()
	{
		
	}

	private void startCalendarWatchers() throws MessagingException, IOException
	{
		assert m_Calendar.isEmpty();

		//List<DavResource> calDavResources = sardine.list(serverName);
		Iterator<DavResource> calDavResources = sardine.list(serverName).iterator();
		// 1st iteration is not a calendar
		calDavResources.next();

		while(calDavResources.hasNext())
		{
			DavResource res = calDavResources.next();
			URI calendarURI = res.getHref();
			// This is map form URI -> CTag
			calendarSyncMap.put(calendarURI,  res.getCustomProps().get("getctag"));

			Iterator<DavResource> davEvents = sardine.list(serverHeader + res).iterator();
			// 1st iteration might be the calendar url
			//davEvents.next();

			// Map from VEvent URI -> Etag
			Map<URI, String> eventURIToEtag = new HashMap<>();
			List<DavResource> unparsedVEvents = new ArrayList<>();
			while(davEvents.hasNext())
			{
				DavResource event = davEvents.next();
				eventURIToEtag.put(event.getHref(), event.getEtag());
				unparsedVEvents.add(event);
			}

			// Map from ICalendar URI -> Map from VEvent URI -> Etag
			eventsSyncMap.put(calendarURI, eventURIToEtag);

			InputStream stream = sardine.get(serverHeader + res);
			ICalendar iCal = Biweekly.parse(stream).first();
			List<VEvent> vEvents = iCal.getEvents();

			if (!iCal.getEvents().isEmpty())
			{
				// This is map from Calendar URI -> ICalendar object
				m_Calendar.put(calendarURI, iCal);
				// This is map from Calendar URI -> (Event URI -> VEvent Object)
				m_PerCalendarEvent(calendarURI, )
			}
		}

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

	@Override public FutureTask<?> getSynchTask() throws UnsupportedOperationException
	{
		return new FutureTask<Void>(() ->
		{
			startCalendarWatchers();
			// == To start a watcher that polls calendar c tag changes

			TransactionRecorder recorder = new TransactionRecorder();
			//synchronizeFolders(recorder);
			// == To beginning the process of grabbing the calendars

			notifyListeners(recorder);

			for (DavResource calendar : m_Calendar.values())
			{
				TransactionRecorder recorder1 = new TransactionRecorder();
				//synchMessageIdsAndHeaders(folder, recorder1);
				// == to Synching the Event IDs?
				notifyListeners(recorder);
			}


			accountLock.unlock();
			LOGGER.log(Level.INFO, "{0} :: SYNCH DONE ", emailAddress);
			return null;
		});
	}

	private WriteContext getWriteContext()
	{
		return new WriteContext(messageDatabase);
	}

//	private void synchronizeCalendars(TransactionRecorder recorder) throws MessagingException
//	{
//		LOGGER.info("synchronizeFolders");
//
//		WriteContext context = getWriteContext();
//		context.startTransaction(recorder);
//
//		try
//		{
//			for (DavResource calendar : m_Calendar.values())
//			{
//				String folderName = encode(folder);
//
//				Model model = context.getModel();
//
//				//  store any folders we don't already have, and update folder counts for ones we do have
//				//  TODO -- need to handle deleted folders
//				StmtIterator it = model.listStatements(model.createResource(folderName),
//						model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_CALENDAR));
//
//				if (it.hasNext())
//				{
//					//  TODO -- should really check that the validity hasn't changed
//					assert JenaUtils.checkUnique(it);
//					//	folder counts may have changed
//					DStore.folderCounts(model, this, folder);
//				}
//				else
//				{
//					System.err.println("ADDING FOLDER " + folder.getName());
//					store(model, folder);
//				}
//			}
//
//			context.succeed();
//		} catch (MessagingException ex)
//		{
//			context.fail(ex);
//		}
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
