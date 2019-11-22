package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.Biweekly;
import biweekly.component.VEvent;
import biweekly.property.DateEnd;
import biweekly.property.DateStart;
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
import org.knowtiphy.babbage.storage.*;
import org.knowtiphy.babbage.storage.CALDAV.DFetch;
import org.knowtiphy.babbage.storage.CALDAV.DStore;
import org.knowtiphy.utils.JenaUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.knowtiphy.babbage.storage.CALDAV.DFetch.CALRES;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.EVENTRES;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.P;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.R;

public class CALDAVAdapter extends BaseAdapter
{
	private static final Logger LOGGER = Logger.getLogger(CALDAVAdapter.class.getName());
	private static final long FREQUENCY = 60_000L;

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
	private Thread synchThread;

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

	public static Calendar fromDate(DateStart date)
	{
		Date d = new Date(date.getValue().getTime());
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		return cal;
	}

	public static Calendar fromDate(DateEnd date)
	{
		Date d = new Date(date.getValue().getTime());
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		return cal;
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
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_NAME), serverName);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_EMAIL_ADDRESS), emailAddress);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_PASSWORD), password);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_HEADER), serverHeader);

		// Notify the client of the account triples
		TransactionRecorder accountRec = new TransactionRecorder();
		accountRec.addedStatements(accountTriples);
		notifyListeners(accountRec);
	}

	private void notifyListeners(TransactionRecorder recorder)
	{
		notificationQ.addLast(() -> listenerManager.notifyChangeListeners(recorder));
	}

//	protected String encodeCalendar(DavResource calendar)
//	{
//		return Vocabulary.E(Vocabulary.CALDAV_CALENDAR, getEmailAddress(), calendar.getHref());
//	}

//	protected String encodeCalendar(String calendar)
//	{
//		return Vocabulary.E(Vocabulary.CALDAV_CALENDAR, getEmailAddress(), calendar);
//	}

	protected String encodeCalendar(DavResource calendar)
	{
		return "http://" + calendar.getHref().toString();
	}

	protected String encodeEvent(DavResource event)
	{
		return "http://" + event.getHref().toString();
	}

	protected String encodeEvent(String event)
	{
		return "http://" + event;
	}

	protected String decodeEvent(String event)
	{
		return event.substring(event.indexOf("/dav"));
	}

	protected String decodeEvent(DavResource event)
	{
		String eventURI = event.getHref().toString();
		return eventURI.substring(eventURI.lastIndexOf('/') + 1);
	}

	public String getEmailAddress()
	{
		return emailAddress;
	}

	private void startCalendarWatchers() throws IOException
	{
		System.out.println("START CALENDARS CALLED");
		assert m_Calendar.isEmpty();

		try
		{
			List<DavResource> calDavResourcesList = sardine.list(serverName);

			System.out.println("NUM OF CALENDARS :: " + (calDavResourcesList.size() - 1));
			Iterator<DavResource> calDavResources = sardine.list(serverName).iterator();
			// 1st iteration is not a calendar, just the enclosing directory
			calDavResources.next();

			while (calDavResources.hasNext())
			{
				DavResource calRes = calDavResources.next();
				System.out.println(calRes.getDisplayName());
				// This is map from Calendar URI -> DavResource for a Calendar
				m_Calendar.put(calRes.getHref().toString(), calRes);
			}
		}
		catch (Throwable ex)
		{
			ex.printStackTrace();
		}

	}

	private void syncCalendars(TransactionRecorder recorder) throws Exception
	{
		Set<String> stored = getStored(DFetch.calendarURIs(getId()), CALRES);
		System.out.println("STORED CALENDARS :: " + stored.size());

		Collection<DavResource> addCalendar = new HashSet<>(10);
		for (DavResource calRes : m_Calendar.values())
		{
			if (!stored.contains(encodeCalendar(calRes)))
			{
				addCalendar.add(calRes);
			}
		}

//		for(String calURI : stored)
//		{
//			System.out.println("ADAPTER ID :: " + getId());
//			System.out.println("STORED CALURI :: " + calURI);
//		}


		Collection<String> removeCalendar = new HashSet<>(10);
		for (String calURI : stored)
		{
			if (!m_Calendar.containsKey(calURI))
			{
				removeCalendar.add(calURI);
			}
		}


		WriteContext context = getWriteContext();
		context.startTransaction(recorder);


		try
		{
			for (String calURI : removeCalendar)
			{
				//System.err.println("REMOVING CALENDAR " + calURI);
				//DStore.unstoreRes(messageDatabase.getDefaultModel(), getId(), calURI);
			}
			for (DavResource calRes : addCalendar)
			{
				System.err.println("ADDING CALENDAR " + calRes.getDisplayName());
				storeCalendar(context.getModel(), calRes);
			}

			context.succeed();
		} catch (Exception ex)
		{
			context.fail(ex);
		}

	}

	private void syncEvents(DavResource calRes, TransactionRecorder recorder) throws Exception
	{
		System.out.println("SYNC EVENTS CALLED FOR CALENDAR :: " + calRes.getDisplayName());
		// get the stored event URIs
		Set<String> stored = getStored(DFetch.eventURIs(encodeCalendar(calRes)), EVENTRES);

		System.out.println("STORED EVENTS SIZE :: " + stored.size());
		Iterator<DavResource> davEvents = sardine.list(serverHeader + calRes).iterator();
		// 1st iteration is the calendar uri, so skip
		davEvents.next();

		Map<String, DavResource> eventURIToRes = new HashMap<>();
		Collection<String> addURI = new HashSet<>(1000);
		while (davEvents.hasNext())
		{
			DavResource event = davEvents.next();
			eventURIToRes.put(event.getHref().toString(), event);

			if (!stored.contains(encodeEvent(event)))
			{
				addURI.add(event.getHref().toString());
			}
		}

		for (String eventURI : stored)
		{
			System.out.println("STORED EVENT URI :: " + eventURI);
		};

		// This is map from Calendar URI -> (Event URI -> DavResource for a VEvent)
		m_PerCalendarEvents.put(calRes.getHref().toString(), eventURIToRes);
		Collection<String> removeURI = new HashSet<>(1000);
		for (String eventUri : stored)
		{
			System.out.println(decodeEvent(eventUri));
			if (!m_PerCalendarEvents.get(calRes.getHref().toString()).containsKey(decodeEvent(eventUri)))
			{
				removeURI.add(eventUri);
			}
		}

		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

		System.out.println("ADD EVENT URI SIZE :: " + addURI.size());
		System.out.println("REMOVE EVENT URI SIZE :: " + removeURI.size());

		try
		{
			for (String event : removeURI)
			{
				System.out.println("REMOVING AN EVENT");
				DStore.unstoreRes(messageDatabase.getDefaultModel(), encodeCalendar(calRes), event);
			}
			for (String event : addURI)
			{
				// Parse out event and pass it through
				VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + event)).first().getEvents().get(0);
				System.err.println("ADDING EVENT :: " + vEvent.getSummary().getValue());
				System.err.println("FOR CALENDER URI :: " + encodeCalendar(calRes));
				DStore.event(messageDatabase.getDefaultModel(), encodeCalendar(calRes), encodeEvent(event), vEvent);
			}

			context.succeed();
		} catch (Exception ex)
		{
			context.fail(ex);
		}

	}

	private Set<String> getStored(String query, String resType)
	{
		Set<String> stored = new HashSet<>(1000);
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			stored.addAll(JenaUtils.set(resultSet, soln -> soln.get(resType).asResource().toString()));
		} finally
		{
			messageDatabase.end();
		}

		return stored;
	}

	private void startSynchThread()
	{
		//
		synchThread = new Thread(() -> {
			try
			{
				Thread.sleep(FREQUENCY);
				ensureMapsLoaded();

				// See if any Calendars have been Deleted or Added as well
				List<DavResource> calDavResourcesList = sardine.list(serverName);
				Iterator<DavResource> calDavResources = sardine.list(serverName).iterator();
				// 1st iteration is not a calendar, just the enclosing directory
				calDavResources.next();

				// Need to query database for existing calendars

				for(String calendar : m_Calendar.keySet())
				{

				}

				// Poll calendars check if CTAG has changed
					// If it has, see if the name or anything else has changed
						// Remove old Triples
						// Add updated Triples
					// Now go through if any URIs have been deleted
						// Remove Triples for that URI
					// Now got through if any URIs have been added
						// Add Triples for that URI
					// Now go through checking if any ETags have changed
						// IF they have, chuck out the old one, and repopulate with this one
							// Remove old Triples
							// Add new Triples


			} catch (InterruptedException | IOException e)
			{
				e.printStackTrace();
			}
		});
	}

	@Override public FutureTask<?> getSynchTask() throws UnsupportedOperationException
	{
		System.out.println("GET SYNCH TASK CALLED");
		return new FutureTask<Void>(() -> {
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
				notifyListeners(recorder1);
			}
			accountLock.unlock();

			startSynchThread();
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
		String calendarName = encodeCalendar(calendar);
		Resource calRes = model.createResource(calendarName);
		model.add(calRes, model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_CALENDAR));
		model.add(model.createResource(getId()), model.createProperty(Vocabulary.CONTAINS), calRes);
		model.add(calRes, model.createProperty(Vocabulary.HAS_NAME),
				model.createTypedLiteral(calendar.getDisplayName()));
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
