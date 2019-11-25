package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.Biweekly;
import biweekly.component.VEvent;
import biweekly.util.ICalDate;
import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.knowtiphy.babbage.storage.BaseAdapter;
import org.knowtiphy.babbage.storage.IReadContext;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.ReadContext;
import org.knowtiphy.babbage.storage.TransactionRecorder;
import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.babbage.storage.WriteContext;
import org.knowtiphy.utils.JenaUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.knowtiphy.babbage.storage.CALDAV.DFetch.*;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.*;

public class CALDAVAdapter extends BaseAdapter
{
	private static final Logger LOGGER = Logger.getLogger(CALDAVAdapter.class.getName());
	private static final long FREQUENCY = 30_000L;

	private static final Runnable POISON_PILL = () -> {
	};

	private final Sardine sardine;

	private final String serverName;
	private final String emailAddress;
	private final String password;
	private final String serverHeader;
	private final String id;
	//	RDF ids to Java folder and message objects
	private final Map<String, DavResource> m_Calendar = new ConcurrentHashMap<>(10);
	private final Map<String, Map<String, DavResource>> m_PerCalendarEvents = new ConcurrentHashMap<>(10);
	private final BlockingQueue<Runnable> workQ;
	//private final BlockingQueue<Runnable> contentQ;
	private final Thread doWork;
	private final ExecutorService doContent;
	private final Mutex accountLock;
	private final Dataset messageDatabase;
	private final ListenerManager listenerManager;
	private final BlockingDeque<Runnable> notificationQ;
	private final Model model;
	private String nickName;
	private Thread synchThread;

	public CALDAVAdapter(String name, Dataset messageDatabase, ListenerManager listenerManager,
			BlockingDeque<Runnable> notificationQ, Model model) throws InterruptedException
	{
		System.out.println("CALDAVAdapter INSTANTIATED");
		// Query for serverName, emailAdress, and password

		this.messageDatabase = messageDatabase;
		this.listenerManager = listenerManager;
		this.notificationQ = notificationQ;
		this.model = model;

		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));

		// Query for these with the passed in model
		this.serverName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_NAME));
		this.emailAddress = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		this.password = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_PASSWORD));
		this.serverHeader = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_SERVER_HEADER));
		try
		{
			this.nickName = JenaUtils.getS(JenaUtils.listObjectsOfPropertyU(model, name, Vocabulary.HAS_NICK_NAME));
		} catch (NoSuchElementException ex)
		{
			//	the account doesn't have a nick name
		}

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

	public static ZonedDateTime fromDate(ICalDate date)
	{
		return ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
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

		if (synchThread != null)
		{
			synchThread.interrupt();
		}
	}

	// @formatter:off
	@Override
	public void addListener()
	{
		Model accountTriples = ModelFactory.createDefaultModel();

		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.RDF_TYPE),
				P(accountTriples, Vocabulary.CALDAV_ACCOUNT));
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_NAME), serverName);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_EMAIL_ADDRESS), emailAddress);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_PASSWORD), password);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_HEADER), serverHeader);
		if (nickName != null)
		{
			accountTriples.add(org.knowtiphy.babbage.storage.IMAP.DStore.R(accountTriples, id), org.knowtiphy.babbage.storage.IMAP.DStore.P(accountTriples, Vocabulary.HAS_NICK_NAME), nickName);
		}
		// Notify the client of the account triples
		TransactionRecorder accountRec = new TransactionRecorder();
		accountRec.addedStatements(accountTriples);
		notifyListeners(accountRec);

		IReadContext context = getReadContext();
		context.start();

		String construtCalendarDetails = String
				.format("      CONSTRUCT {   ?%s <%s> <%s> . " +
								"?%s <%s> ?%s .  " +
								"?%s <%s> ?%s}\n " +
								"WHERE {     ?%s <%s> <%s> . " +
								"?%s <%s> ?%s .  " +
								"?%s <%s> ?%s .  " +
								" }",
						// START OF CONSTRUCT
						Vars.VAR_CALENDAR_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_CALENDAR,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_CALENDAR_ID,
						Vars.VAR_CALENDAR_ID, Vocabulary.HAS_NAME, Vars.VAR_CALENDAR_NAME,
						// START OF WHERE
						Vars.VAR_CALENDAR_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_CALENDAR,
						Vars.VAR_ACCOUNT_ID, Vocabulary.CONTAINS, Vars.VAR_CALENDAR_ID,
						Vars.VAR_CALENDAR_ID, Vocabulary.HAS_NAME, Vars.VAR_CALENDAR_NAME);

		Model mCalendarDetails = QueryExecutionFactory.create(construtCalendarDetails, context.getModel()).execConstruct();

		JenaUtils.printModel(mCalendarDetails, "CALENDARS");

		TransactionRecorder rec1 = new TransactionRecorder();
		rec1.addedStatements(mCalendarDetails);
		notifyListeners(rec1);

		String constructEventDetails = String
				.format("      CONSTRUCT {   ?%s <%s> <%s> . " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s}\n   " +

								"WHERE {     ?%s <%s> <%s> .  " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"?%s <%s> ?%s .   " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								"OPTIONAL {  ?%s <%s> ?%s }\n " +
								" }",
						// START OF CONSTRUCT
						Vars.VAR_EVENT_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_EVENT,
						Vars.VAR_CALENDAR_ID, Vocabulary.CONTAINS, Vars.VAR_EVENT_ID,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_SUMMARY, Vars.VAR_SUMMARY,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_START, Vars.VAR_DATE_START,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_END, Vars.VAR_DATE_END,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DESCRIPTION, Vars.VAR_DESCRIPTION,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_PRIORITY, Vars.VAR_PRIORITY,
						// START OF WHERE
						Vars.VAR_EVENT_ID, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_EVENT,
						Vars.VAR_CALENDAR_ID, Vocabulary.CONTAINS, Vars.VAR_EVENT_ID,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_SUMMARY, Vars.VAR_SUMMARY,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_START, Vars.VAR_DATE_START,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DATE_END, Vars.VAR_DATE_END,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_DESCRIPTION, Vars.VAR_DESCRIPTION,
						Vars.VAR_EVENT_ID, Vocabulary.HAS_PRIORITY, Vars.VAR_PRIORITY);

		Model mEventDetails = QueryExecutionFactory.create(constructEventDetails, context.getModel()).execConstruct();
		JenaUtils.printModel(mEventDetails, "EVENTS");

		TransactionRecorder rec2 = new TransactionRecorder();
		rec2.addedStatements(mEventDetails);
		context.end();

		notifyListeners(rec2);
	}
	// @formatter:on

	private void notifyListeners(TransactionRecorder recorder)
	{
		notificationQ.addLast(() -> listenerManager.notifyChangeListeners(recorder));
	}

	protected String encodeCalendar(DavResource calendar)
	{
		return Vocabulary.E(Vocabulary.CALDAV_CALENDAR, getEmailAddress(), calendar.getHref());
	}

	protected String encodeEvent(DavResource calendar, DavResource event)
	{
		return Vocabulary.E(Vocabulary.CALDAV_EVENT, getEmailAddress(), calendar.getHref(), event.getHref());
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
				m_Calendar.put(encodeCalendar(calRes), calRes);
			}
		} catch (Throwable ex)
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
				System.err.println("REMOVING CALENDAR " + calURI);
				DStore.unstoreRes(context.getModel(), getId(), calURI);
			}
			for (DavResource calRes : addCalendar)
			{
				System.err.println("ADDING CALENDAR " + calRes.getDisplayName());
				DStore.storeCalendar(context.getModel(), getId(), encodeCalendar(calRes), calRes);
			}

			context.succeed();
			notifyListeners(recorder);
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

		Map<String, DavResource> eventURIToRes = new ConcurrentHashMap<>();
		Collection<DavResource> addURI = new HashSet<>(1000);
		while (davEvents.hasNext())
		{
			DavResource event = davEvents.next();
			eventURIToRes.put(encodeEvent(calRes, event), event);

			if (!stored.contains(encodeEvent(calRes, event)))
			{
				addURI.add(event);
			}
		}

		// This is map from Calendar URI -> (Event URI -> DavResource for a VEvent)
		m_PerCalendarEvents.put(encodeCalendar(calRes), eventURIToRes);
		Collection<String> removeURI = new HashSet<>(1000);
		for (String eventUri : stored)
		{
			System.out.println(eventUri);
			if (!m_PerCalendarEvents.get(encodeCalendar(calRes)).containsKey(eventUri))
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
			for (DavResource event : addURI)
			{
				// Parse out event and pass it through
				VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + event)).first().getEvents().get(0);
				System.err.println("ADDING EVENT :: " + vEvent.getSummary().getValue());
				System.err.println("FOR CALENDER URI :: " + encodeCalendar(calRes));
				try
				{
					DStore.storeEvent(messageDatabase.getDefaultModel(), encodeCalendar(calRes),
							encodeEvent(calRes, event), vEvent, event);
				} catch (Throwable ex)
				{
					ex.printStackTrace();
				}
			}

			context.succeed();
		} catch (Exception ex)
		{
			context.fail(ex);
		}

	}

	private <T> void updateTriple(Model model, String resURI, String hasProp, T updated)
	{
		model.remove(model.listStatements(R(model, resURI), P(model, hasProp), (RDFNode) null));
		model.add(R(model, resURI), P(model, hasProp), model.createTypedLiteral(updated));
	}

	private void storeCalendarDiffs(String calURI, DavResource cal) throws Exception
	{
		messageDatabase.begin(ReadWrite.READ);
		Model model = messageDatabase.getDefaultModel();
		ResultSet rs;
		try
		{
			rs = QueryExecutionFactory.create(DFetch.calendarProperties(calURI), model).execSelect();
		} finally
		{
			messageDatabase.end();
		}

		TransactionRecorder recorder = new TransactionRecorder();
		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

		updateTriple(model, calURI, Vocabulary.HAS_CTAG, cal.getCustomProps().get("getctag"));

		try
		{
			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();
				if (!soln.getLiteral(NAME).equals(L(model, cal.getDisplayName())))
				{
					updateTriple(model, calURI, Vocabulary.HAS_NAME, cal.getDisplayName());
				}
			}

			context.succeed();
			notifyListeners(recorder);
		} catch (Exception ex)
		{
			context.fail(ex);
		}
	}

	private void storeEventDiffs(String eventURI, DavResource serverEvent) throws Exception
	{
		messageDatabase.begin(ReadWrite.READ);
		Model model = messageDatabase.getDefaultModel();
		ResultSet rs;
		try
		{
			rs = QueryExecutionFactory.create(DFetch.eventProperties(eventURI), model).execSelect();
		} finally
		{
			messageDatabase.end();
		}

		VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + serverEvent)).first().getEvents().get(0);

		TransactionRecorder recorder = new TransactionRecorder();
		WriteContext context = getWriteContext();
		context.startTransaction(recorder);

		updateTriple(model, eventURI, Vocabulary.HAS_ETAG, serverEvent.getEtag());

		try
		{
			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();

				if (vEvent.getDateEnd() != null)
				{
					if (!soln.getLiteral(DATEEND).equals(L(model,
							new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateEnd().getValue()))))))
					{
						updateTriple(model, eventURI, Vocabulary.HAS_DATE_END,
								new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateEnd().getValue()))));
					}
				}
				else
				{
					if (!soln.getLiteral(DATEEND).equals(L(model, new XSDDateTime(GregorianCalendar
							.from(fromDate(vEvent.getDateStart().getValue())
									.plus(Duration.parse(vEvent.getDuration().getValue().toString())))))))
					{
						updateTriple(model, eventURI, Vocabulary.HAS_DATE_END, new XSDDateTime(GregorianCalendar
								.from(fromDate(vEvent.getDateStart().getValue())
										.plus(Duration.parse(vEvent.getDuration().getValue().toString())))));
					}
				}

				if (!soln.getLiteral(SUMMARY).equals(L(model, vEvent.getSummary().getValue())))
				{
					updateTriple(model, eventURI, Vocabulary.HAS_SUMMARY, vEvent.getSummary().getValue());
				}
				else if (!soln.getLiteral(DATESTART).equals(L(model,
						new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue()))))))
				{
					updateTriple(model, eventURI, Vocabulary.HAS_DATE_START,
							new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue()))));
				}
				else if (vEvent.getDescription() != null && soln.getLiteral(DESCRIPTION) != null)
				{
					if (!soln.getLiteral(DESCRIPTION).equals(L(model, vEvent.getDescription().getValue())))
					{
						updateTriple(model, eventURI, Vocabulary.HAS_DESCRIPTION, vEvent.getDescription().getValue());
					}
				}
				else if (vEvent.getPriority() != null && soln.getLiteral(PRIORITY) != null)
				{
					if (!soln.getLiteral(PRIORITY).equals(L(model, vEvent.getPriority().getValue())))
					{
						updateTriple(model, eventURI, Vocabulary.HAS_PRIORITY, vEvent.getPriority().getValue());
					}
				}

			}

			context.succeed();
			notifyListeners(recorder);
		} catch (Exception ex)
		{
			ex.printStackTrace();
			context.fail(ex);
		}
		// Calculate difference, unstore, restore, store new ETAG

	}

	private String getStoredTag(String query, String resType)
	{
		String tag;
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			tag = JenaUtils.single(resultSet, soln -> soln.get(resType).toString());
		} finally
		{
			messageDatabase.end();
		}

		return tag;
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
		synchThread = new Thread(() -> {
			while (true)
			{
				try
				{
					//					Thread.sleep(FREQUENCY);
					//					ensureMapsLoaded();

					// RETHINKING OF DOING

					accountLock.lock();

					System.out.println("IN SYNCH THREAD ::::::::::::::::::::::::::::::::: ");

					Set<String> storedCalendars = getStored(DFetch.calendarURIs(getId()), CALRES);

					Iterator<DavResource> calDavResources = sardine.list(serverName).iterator();
					// 1st iteration is not a calendar, just the enclosing directory
					calDavResources.next();

					Set<String> serverCalURIs = new HashSet<>(10);
					// During this loop, I can check the CTAGS, Check if need to be added/deleted
					// Maybe all at once later on
					while (calDavResources.hasNext())
					{
						DavResource serverCal = calDavResources.next();
						String serverCalURI = encodeCalendar(serverCal);
						serverCalURIs.add(serverCalURI);

						if (!m_Calendar.containsKey(serverCalURI))
						{
							m_Calendar.put(serverCalURI, serverCal);
						}

						// Calendar not in DB, store it and events
						if (!storedCalendars.contains(serverCalURI))
						{
							//m_Calendar.put(encodedServerCalURI, serverCal);

							// Add Events
							Iterator<DavResource> davEvents = sardine.list(serverHeader + serverCal).iterator();
							// 1st iteration is the calendar uri, so skip
							davEvents.next();

							Collection<DavResource> addEvent = new HashSet<>(1000);
							Map<String, DavResource> eventURIToRes = new ConcurrentHashMap<>();
							while (davEvents.hasNext())
							{
								DavResource serverEvent = davEvents.next();
								eventURIToRes.put(encodeEvent(serverCal, serverEvent), serverEvent);
								addEvent.add(serverEvent);
							}

							m_PerCalendarEvents.put(serverCalURI, eventURIToRes);

							TransactionRecorder recorder = new TransactionRecorder();
							WriteContext context = getWriteContext();
							context.startTransaction(recorder);

							try
							{
								System.out.println("SYNCH THREAD ADDDING CAL");
								DStore.storeCalendar(context.getModel(), getId(), serverCalURI, serverCal);

								// Commit this before storing events

								context.succeed();
								notifyListeners(recorder);
							} catch (Exception ex)
							{
								context.fail(ex);
							}

							TransactionRecorder recorder2 = new TransactionRecorder();
							WriteContext context2 = getWriteContext();
							context2.startTransaction(recorder2);

							try
							{

								for (DavResource event : addEvent)
								{
									// Parse out event and pass it through
									VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + event)).first()
											.getEvents().get(0);
									//System.err.println("ADDING EVENT :: " + vEvent.getSummary().getValue());
									System.err.println("FOR CALENDER URI :: " + serverCalURI);
									try
									{
										DStore.storeEvent(messageDatabase.getDefaultModel(), serverCalURI,
												encodeEvent(serverCal, event), vEvent, event);
									} catch (Throwable ex)
									{
										ex.printStackTrace();
									}
								}

								context2.succeed();
								notifyListeners(recorder2);
							} catch (Exception ex)
							{
								context2.fail(ex);
							}

						}
						// Calendar already exists, check if CTags differ, check if names differ
						else
						{
							if (!getStoredTag(DFetch.calendarCTag(serverCalURI), CTAG)
									.equals(serverCal.getCustomProps().get("getctag")))
							{
								System.out.println(
										":::::::::::::::::::::::::::::::::: C TAG HAS CHANGED :::::::::::::::::::::::::::::::::::::::::::::");
								m_Calendar.put(serverCalURI, serverCal);
								storeCalendarDiffs(serverCalURI, serverCal);

								Set<String> storedEvents = getStored(DFetch.eventURIs(serverCalURI), EVENTRES);
								Set<DavResource> addEvents = new HashSet<>();
								Set<String> serverEventURIs = new HashSet<>();

								Iterator<DavResource> davEvents = sardine.list(serverHeader + serverCal).iterator();
								// 1st iteration is the calendar uri, so skip
								davEvents.next();

								while (davEvents.hasNext())
								{
									DavResource serverEvent = davEvents.next();
									String serverEventURI = encodeEvent(serverCal, serverEvent);
									serverEventURIs.add(serverEventURI);

									if (!m_PerCalendarEvents.containsKey(serverCalURI))
									{
										m_PerCalendarEvents.put(serverCalURI, new ConcurrentHashMap<>(100));
									}

									// New Event, store it
									if (!storedEvents.contains(serverEventURI))
									{
										System.out.println("EVENT NOT STORED, TO ADD");
										m_PerCalendarEvents.get(serverCalURI).put(serverEventURI, serverEvent);
										addEvents.add(serverEvent);
									}
									// Not new event, compare ETAGS
									else
									{
										m_PerCalendarEvents.get(serverCalURI).put(serverEventURI, serverEvent);

										String storedTAG = getStoredTag(DFetch.eventETag(serverEventURI), ETAG)
												.replace("\\", "");

										if (!storedTAG.equals(serverEvent.getEtag()))
										{
											storeEventDiffs(serverEventURI, serverEvent);
										}
									}

								}

								// Events to be removed
								Collection<String> removeEvent = new HashSet<>();
								for (String currEventURI : m_PerCalendarEvents.get(serverCalURI).keySet())
								{
									if (!serverEventURIs.contains(currEventURI))
									{
										removeEvent.add(currEventURI);
									}
								}

								TransactionRecorder recorder = new TransactionRecorder();
								WriteContext context = getWriteContext();
								context.startTransaction(recorder);
								try
								{
									for (String event : removeEvent)
									{
										System.out.println("REMOVING AN EVENT");
										DStore.unstoreRes(messageDatabase.getDefaultModel(), serverCalURI, event);
									}
									for (DavResource event : addEvents)
									{
										// Parse out event and pass it through
										VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + event)).first()
												.getEvents().get(0);
										System.err.println("ADDING EVENT :: " + vEvent.getSummary().getValue());
										try
										{
											Model m = ModelFactory.createDefaultModel();
											DStore.storeEvent(m, serverCalURI, encodeEvent(serverCal, event), vEvent,
													event);
											JenaUtils.printModel(m, "XXXXXXXXXXXXXXXXXXXXXXXXX");
											messageDatabase.getDefaultModel().add(m);
										} catch (Throwable ex)
										{
											ex.printStackTrace();
										}
									}

									context.succeed();
									notifyListeners(recorder);
								} catch (Exception ex)
								{
									context.fail(ex);
								}

							}
						}

					}

					// Calendars to be removed
					// For every Calendar URI in m_Calendar, if server does not contain it, remove it
					// Maybe do all at once? Or would that be too abrasive to user?
					for (String currentCalUri : m_Calendar.keySet())
					{

						if (!serverCalURIs.contains(currentCalUri))
						{
							System.out.println("SYNCH THREAD REMOVING CAL");

							TransactionRecorder recorder = new TransactionRecorder();
							WriteContext context = getWriteContext();
							context.startTransaction(recorder);

							try
							{

								for (String eventURI : m_PerCalendarEvents.get(currentCalUri).keySet())
								{
									DStore.unstoreRes(context.getModel(), currentCalUri, eventURI);
								}

								DStore.unstoreRes(context.getModel(), getId(), currentCalUri);

								m_PerCalendarEvents.remove(currentCalUri);
								m_Calendar.remove(currentCalUri);

								context.succeed();
								notifyListeners(recorder);
							} catch (Exception ex)
							{
								context.fail(ex);
							}
						}
					}

					accountLock.unlock();
					Thread.sleep(FREQUENCY);

				}
				catch (InterruptedException shutdown)
				{
					return;
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		});

		synchThread.start();
	}

	@Override public FutureTask<?> getSynchTask() throws UnsupportedOperationException
	{
		System.out.println("GET SYNCH TASK CALLED");
		return new FutureTask<Void>(() -> {
			//			startCalendarWatchers();
			//
			//			TransactionRecorder recorder = new TransactionRecorder();
			//			syncCalendars(recorder);
			//
			//			notifyListeners(recorder);
			//
			//			for (DavResource calendar : m_Calendar.values())
			//			{
			//				TransactionRecorder recorder1 = new TransactionRecorder();
			//				syncEvents(calendar, recorder1);
			//				notifyListeners(recorder1);
			//			}
			//			accountLock.unlock();

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

	private ReadContext getReadContext()
	{
		return new ReadContext(messageDatabase);
	}

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
