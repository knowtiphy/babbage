package org.knowtiphy.babbage.storage;

import biweekly.Biweekly;
import biweekly.ICalendar;
import biweekly.component.VEvent;
import biweekly.property.LastModified;
import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.sound.midi.SysexMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.knowtiphy.babbage.storage.DStore.*;

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
	private final Map<String, DavResource> m_Calendar = new HashMap<>(5);
	private final Map<String, Map<String, DavResource>> m_PerCalendarEvents = new HashMap<>(1000);

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

		System.out.println("CALDAV EMAIL :: " + emailAddress);
		System.out.println("CALDAV PASSWORD :: " + password);

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
		try
		{
			workQ.add(POISON_PILL);
			doWork.join();
			doContent.shutdown();
			doContent.awaitTermination(10_000L, TimeUnit.SECONDS);
		} catch (InterruptedException ex)
		{
			//  ignore
		}
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

	public String encode(DavResource calendar)
	{
		return Vocabulary.E(Vocabulary.CALDAV_CALENDAR, getEmailAddress(), calendar.getHref());
	}

	public String getEmailAddress()
	{
		return emailAddress;
	}


	private void startCalendarWatchers() throws MessagingException, IOException
	{
		System.out.println("START CALENDARS CALLED");
		assert m_Calendar.isEmpty();

		List<DavResource> calDavResourcesList = sardine.list(serverName);

		System.out.println("NUM OF CALENDARS :: " + calDavResourcesList.size());
		Iterator<DavResource> calDavResources = sardine.list(serverName).iterator();
		// 1st iteration is not a calendar, just the enclosing directory
		calDavResources.next();

		while(calDavResources.hasNext())
		{
			DavResource calRes = calDavResources.next();
			System.out.println(calRes.getDisplayName());
			// This is map from Calendar URI -> DavResource for a Calendar
			m_Calendar.put(calRes.getHref().toString(), calRes);

			// Start a thread that polls this calendar for CTAG Changes, and starts the rest of the
			// continuous syncing process
		}

	}

	private void syncCalendars(TransactionRecorder recorder)
	{
		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

		for(DavResource calRes : m_Calendar.values())
		{
			String calName = encode(calRes);
			Model model = context.getModel();
			StmtIterator it = model.listStatements(model.createResource(calName),
					model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_CALENDAR));


			if (it.hasNext())
			{
				assert JenaUtils.checkUnique(it);
				// Define storing methods
			}
			else
			{
				System.err.println("ADDING CALENDAR " + calRes.getDisplayName());
				// Define storing methods
				storeCalendar(model, calRes);
			}
		}

		context.succeed();
	}

	private void syncEvents(DavResource calRes, TransactionRecorder recorder) throws Exception
	{
		System.out.println("SYNC EVENTS CALLED");
		// get the stored event URIs
		Set<String> stored = getStoredEvents(DFetch.eventURIs(encode(calRes)));

		Iterator<DavResource> davEvents = sardine.list(serverHeader + calRes).iterator();
		// 1st iteration might be the calendar url
		//davEvents.next();

		// Map from VEvent URI -> Etag
		Map<String, DavResource> eventURIToRes = new HashMap<>();
		Collection<String> addURI = new HashSet<>(1000);
		while(davEvents.hasNext())
		{
			DavResource event = davEvents.next();
			eventURIToRes.put(event.getHref().toString(), event);

			if (!stored.contains(event.getHref().toString()))
			{
				addURI.add(event.getHref().toString());
			}
		}

		// This is map from Calendar URI -> (Event URI -> DavResource for a VEvent)
		m_PerCalendarEvents.put(calRes.getHref().toString(), eventURIToRes);
		Collection<String> removeURI = new HashSet<>(1000);
		for (String eventUri : stored)
		{
			if (!m_PerCalendarEvents.get(calRes).containsKey(eventUri))
			{
				removeURI.add(eventUri);
			}
		}

		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

	try
	{
		for (String event : removeURI)
		{

		}
		for (String event : addURI)
		{

		}

		context.succeed();
	}catch (Exception ex)
	{
		context.fail(ex);
	}

	}

	private Set<String> getStoredEvents(String query)
	{
		Set<String> stored = new HashSet<>(1000);
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			stored.addAll(JenaUtils.set(resultSet, soln -> soln.get("message").asResource().toString()));
		} finally
		{
			messageDatabase.end();
		}

		return stored;
	}

	@Override public FutureTask<?> getSynchTask() throws UnsupportedOperationException
	{
		System.out.println("GET SYNCH TASK CALLED");
		return new FutureTask<Void>(() ->
		{
			startCalendarWatchers();

			System.out.println("BEFORE SYNC CALENDARS");
			TransactionRecorder recorder = new TransactionRecorder();
			syncCalendars(recorder);
			System.out.println("AFTER SYNC CALENDARS");

			notifyListeners(recorder);

			for (DavResource calendar : m_Calendar.values())
			{
				TransactionRecorder recorder1 = new TransactionRecorder();
				syncEvents(calendar, recorder1);
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

	// Factor this out to its own class eventually, since all Adapters will use
	private void ensureMapsLoaded() throws InterruptedException
	{
		accountLock.lock();
		accountLock.unlock();
	}

	private void storeCalendar(Model model, DavResource calendar)
	{
		String calendarName = encode(calendar);
		Resource calRes = model.createResource(calendarName);
		model.add(calRes, model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_CALENDAR));
		model.add(model.createResource(getId()), model.createProperty(Vocabulary.CONTAINS), calRes);
		model.add(calRes, model.createProperty(Vocabulary.HAS_NAME), model.createTypedLiteral(calendar.getDisplayName()));
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
