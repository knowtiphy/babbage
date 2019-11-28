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
import org.knowtiphy.babbage.storage.IMAP.DStore;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.knowtiphy.babbage.storage.CALDAV.DStore.*;
import static org.knowtiphy.babbage.storage.CALDAV.Fetch.*;

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
	private final Mutex accountLock;
	private String nickName;
	private Thread synchThread;

	public CALDAVAdapter(String name, Dataset messageDatabase, ListenerManager listenerManager,
			BlockingDeque<Runnable> notificationQ, Model model) throws InterruptedException
	{
		super(messageDatabase, listenerManager, notificationQ);
		System.out.println("CALDAVAdapter INSTANTIATED");
		// Query for serverName, emailAdress, and password

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

		workQ = new LinkedBlockingQueue<>();
		doWork = new Thread(new Worker(workQ));
		doWork.start();
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
		} catch (InterruptedException ex)
		{
			//  ignore
		}

		if (synchThread != null)
		{
			synchThread.interrupt();
		}
	}

	@Override public void addListener()
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
			accountTriples
					.add(DStore.R(accountTriples, id), DStore.P(accountTriples, Vocabulary.HAS_NICK_NAME), nickName);
		}
		// Notify the client of the account triples
		notifyListeners(accountTriples);

		messageDatabase.begin(ReadWrite.READ);
		Model mCalendarDetails = QueryExecutionFactory.create(skeleton(), messageDatabase.getDefaultModel())
				.execConstruct();
		messageDatabase.end();
		notifyListeners(mCalendarDetails);

		messageDatabase.begin(ReadWrite.READ);
		Model mEventDetails = QueryExecutionFactory.create(initialState(), messageDatabase.getDefaultModel())
				.execConstruct();
		messageDatabase.end();
		notifyListeners(mEventDetails);

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

	private <T> void updateTriple(Model messageDB, Model adds, Model deletes, String resURI, String hasProp, T updated)
	{
		deletes.add(messageDB.listStatements(R(messageDB, resURI), P(messageDB, hasProp), (RDFNode) null));
		adds.add(R(messageDB, resURI), P(messageDB, hasProp), messageDB.createTypedLiteral(updated));
	}

	private void storeCalendarDiffs(String calURI, DavResource cal) throws Exception
	{
		messageDatabase.begin(ReadWrite.READ);
		Model messageDB = messageDatabase.getDefaultModel();
		Model adds = ModelFactory.createDefaultModel();
		Model deletes = ModelFactory.createDefaultModel();

		try
		{
			ResultSet rs = QueryExecutionFactory.create(calendarProperties(calURI), messageDB).execSelect();
			updateTriple(messageDB, adds, deletes, calURI, Vocabulary.HAS_CTAG, cal.getCustomProps().get("getctag"));

			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();
				if (!soln.getLiteral(NAME).equals(L(messageDB, cal.getDisplayName())))
				{
					updateTriple(messageDB, adds, deletes, calURI, Vocabulary.HAS_NAME, cal.getDisplayName());
				}
			}
		} finally
		{
			messageDatabase.end();
		}

		update(adds, deletes);
	}

	private void storeEventDiffs(String eventURI, DavResource serverEvent) throws Exception
	{
		VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + serverEvent)).first().getEvents().get(0);

		messageDatabase.begin(ReadWrite.READ);
		Model messageDB = messageDatabase.getDefaultModel();
		Model adds = ModelFactory.createDefaultModel();
		Model deletes = ModelFactory.createDefaultModel();

		try
		{
			updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_ETAG, serverEvent.getEtag());
			ResultSet rs = QueryExecutionFactory.create(eventProperties(eventURI), messageDB).execSelect();
			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();

				if (vEvent.getDateEnd() != null)
				{
					if (!soln.getLiteral(DATEEND).equals(L(messageDB,
							new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateEnd().getValue()))))))
					{
						updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_DATE_END,
								new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateEnd().getValue()))));
					}
				}
				else
				{
					if (!soln.getLiteral(DATEEND).equals(L(messageDB, new XSDDateTime(GregorianCalendar
							.from(fromDate(vEvent.getDateStart().getValue())
									.plus(Duration.parse(vEvent.getDuration().getValue().toString())))))))
					{
						updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_DATE_END, new XSDDateTime(
								GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue())
										.plus(Duration.parse(vEvent.getDuration().getValue().toString())))));
					}
				}

				if (!soln.getLiteral(SUMMARY).equals(L(messageDB, vEvent.getSummary().getValue())))
				{
					updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_SUMMARY,
							vEvent.getSummary().getValue());
				}
				else if (!soln.getLiteral(DATESTART).equals(L(messageDB,
						new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue()))))))
				{
					updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_DATE_START,
							new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue()))));
				}
				else if (vEvent.getDescription() != null && soln.getLiteral(DESCRIPTION) != null)
				{
					if (!soln.getLiteral(DESCRIPTION).equals(L(messageDB, vEvent.getDescription().getValue())))
					{
						updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_DESCRIPTION,
								vEvent.getDescription().getValue());
					}
				}
				else if (vEvent.getPriority() != null && soln.getLiteral(PRIORITY) != null)
				{
					if (!soln.getLiteral(PRIORITY).equals(L(messageDB, vEvent.getPriority().getValue())))
					{
						updateTriple(messageDB, adds, deletes, eventURI, Vocabulary.HAS_PRIORITY,
								vEvent.getPriority().getValue());
					}
				}

			}
		} finally
		{
			messageDatabase.end();
		}

		update(adds, deletes);

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
					workQ.add(() -> {

						try
						{

							accountLock.lock();

							System.out.println(
									":::::::::::::::::::::::::: IN SYNCH THREAD ::::::::::::::::::::::::::::::::: ");

							Set<String> storedCalendars = getStored(calendarURIs(getId()), CALRES);

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
									m_Calendar.put(serverCalURI, serverCal);

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

									Model addCalendar = ModelFactory.createDefaultModel();
									storeCalendar(addCalendar, getId(), serverCalURI, serverCal);
									update(addCalendar, ModelFactory.createDefaultModel());

									Model addVEvents = ModelFactory.createDefaultModel();

									for (DavResource event : addEvent)
									{
										// Parse out event and pass it through
										VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + event)).first()
												.getEvents().get(0);
										//System.err.println("ADDING EVENT :: " + vEvent.getSummary().getValue());
										//System.err.println("FOR CALENDER URI :: " + serverCalURI);

										storeEvent(addVEvents, serverCalURI, encodeEvent(serverCal, event), vEvent,
												event);
									}

									update(addVEvents, ModelFactory.createDefaultModel());

								}
								// Calendar already exists, check if CTags differ, check if names differ
								else
								{
									if (!getStoredTag(calendarCTag(serverCalURI), CTAG)
											.equals(serverCal.getCustomProps().get("getctag")))
									{
										System.out.println(
												":::::::::::::::::::::::::::::::::: C TAG HAS CHANGED :::::::::::::::::::::::::::::::::::::::::::::");
										m_Calendar.put(serverCalURI, serverCal);
										storeCalendarDiffs(serverCalURI, serverCal);

										Set<String> storedEvents = getStored(eventURIs(serverCalURI), EVENTRES);
										Set<DavResource> addEvents = new HashSet<>();
										Set<String> serverEventURIs = new HashSet<>();

										Iterator<DavResource> davEvents = sardine.list(serverHeader + serverCal)
												.iterator();
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
												m_PerCalendarEvents.get(serverCalURI).put(serverEventURI, serverEvent);
												addEvents.add(serverEvent);
											}
											// Not new event, compare ETAGS
											else
											{
												m_PerCalendarEvents.get(serverCalURI).put(serverEventURI, serverEvent);

												String storedTAG = getStoredTag(eventETag(serverEventURI), ETAG)
														.replace("\\", "");

												if (!storedTAG.equals(serverEvent.getEtag()))
												{
													storeEventDiffs(serverEventURI, serverEvent);
												}
											}

										}

										// Events to be removed, this needs to be events in the DB
										Collection<String> removeEvent = new HashSet<>();
										for (String currEventURI : storedEvents)
										{
											if (!serverEventURIs.contains(currEventURI))
											{
												removeEvent.add(currEventURI);
												m_PerCalendarEvents.get(serverCalURI).remove(currEventURI);
											}
										}

										messageDatabase.begin(ReadWrite.READ);
										Model deletes = ModelFactory.createDefaultModel();
										removeEvent.forEach(
												event -> unstoreRes(messageDatabase.getDefaultModel(), deletes,
														serverCalURI, event));
										messageDatabase.end();

										Model adds = ModelFactory.createDefaultModel();
										addEvents.forEach(event -> {
											try
											{
												storeEvent(adds, serverCalURI, encodeEvent(serverCal, event),
														Biweekly.parse(sardine.get(serverHeader + event)).first()
																.getEvents().get(0), event);
											} catch (IOException e)
											{
												e.printStackTrace();
											}
										});

										update(adds, deletes);

									}
								}

							}

							// Calendars to be removed
							// For every Calendar URI in m_Calendar, if server does not contain it, remove it
							// Maybe do all at once? Or would that be too abrasive to user?
							for (String storedCalURI : storedCalendars)
							{

								if (!serverCalURIs.contains(storedCalURI))
								{
									Set<String> currStoredEvents = getStored(eventURIs(storedCalURI), EVENTRES);

									Model deleteEvent = ModelFactory.createDefaultModel();
									messageDatabase.begin(ReadWrite.READ);
									currStoredEvents.forEach(
											eventURI -> unstoreRes(messageDatabase.getDefaultModel(), deleteEvent,
													storedCalURI, eventURI));
									messageDatabase.end();

									update(null, deleteEvent);

									Model deleteCalendar = ModelFactory.createDefaultModel();
									messageDatabase.begin(ReadWrite.READ);
									unstoreRes(messageDatabase.getDefaultModel(), deleteCalendar, getId(),
											storedCalURI);
									messageDatabase.end();

									if (m_PerCalendarEvents.get(storedCalURI) != null)
									{
										m_PerCalendarEvents.remove(storedCalURI);
									}

									if (m_Calendar.get(storedCalURI) != null)
									{
										m_Calendar.remove(storedCalURI);
									}

									update(null, deleteCalendar);
								}
							}

							accountLock.unlock();
						} catch (Exception e)
						{
							e.printStackTrace();
						}

					});

					Thread.sleep(FREQUENCY);
				} catch (InterruptedException ex)
				{
					return;
				}
			}
		});

		synchThread.start();
	}

	@Override public FutureTask<?> getSynchTask() throws UnsupportedOperationException
	{
		return new FutureTask<Void>(() -> {
			startSynchThread();

			LOGGER.log(Level.INFO, "{0} :: SYNCH DONE ", emailAddress);
			return null;
		});
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
