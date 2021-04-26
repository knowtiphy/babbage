package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.Biweekly;
import biweekly.ICalendar;
import biweekly.component.VEvent;
import biweekly.property.Summary;
import biweekly.util.ICalDate;
import com.github.sardine.DavResource;
import org.apache.commons.io.IOUtils;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.babbage.Babbage;
import org.knowtiphy.babbage.storage.BaseDavAdapter;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.EventSetBuilder;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.MessagingException;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Function;
import java.util.logging.Logger;

import static org.knowtiphy.babbage.storage.CALDAV.DFetch.CALRES;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.CTAG;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.DATEEND;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.DATESTART;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.DESCRIPTION;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.ETAG;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.EVENTRES;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.NAME;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.PRIORITY;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.SUMMARY;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.calendarCTag;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.calendarProperties;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.eventETag;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.eventProperties;
import static org.knowtiphy.babbage.storage.CALDAV.DFetch.eventURIs;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.addCalendar;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.unstoreRes;
import static org.knowtiphy.babbage.storage.CALDAV.Encode.encode;
import static org.knowtiphy.utils.JenaUtils.L;

public class CALDAVAdapter extends BaseDavAdapter
{
	private static final Logger LOGGER = Logger.getLogger(CALDAVAdapter.class.getName());

	//	RDF ids to Java calendar and event objects per calendar
	protected final Map<String, DavResource> calendars = new HashMap<>(10);
	protected final Map<String, Map<String, DavResource>> events = new HashMap<>(1000);

	public CALDAVAdapter(String name, String type, Dataset messageDatabase,
						 ListenerManager listenerManager, BlockingDeque<Runnable> notificationQ, Model model)
	{
		super(name, type, messageDatabase, listenerManager, notificationQ, model);
		operations.put(Vocabulary.ADD_CALDAV_EVENT, this::toServer);
	}

	//	TODO -- why is this here?
	public static ZonedDateTime fromDate(ICalDate date)
	{
		return ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
	}

	@Override
	public Callable<Void> getSyncTask()
	{
		return new SyncTask();
	}

	@Override
	public void initialize(Map<String, Function<String, Future<?>>> syncs, Delta delta) throws MessagingException, IOException
	{
		super.initialize(syncs, delta);
		syncs.put(Vocabulary.CALDAV_ACCOUNT, this::syncAccount);
		syncs.put(Vocabulary.CALDAV_CALENDAR, (x) -> new FutureTask<>(() -> true));
		syncs.put(Vocabulary.CALDAV_EVENT, (x) -> new FutureTask<>(() -> true));
	}

	protected Future<?> syncAccount(String aid)
	{
		System.out.println("CALDAV syncAccount " + aid);
		return addWork(getSyncTask());
	}

	//	add account triples to the message cache
	//	note: it's a cache -- so we can't simply store these triples in the cache permnantly, as
	//	the cache is designed to be deleteable at any point in time.

	@Override
	protected void addInitialTriples(Delta delta)
	{
		delta.bothOP(id, RDF.type.toString(), type);
		delta.bothDP(getId(), Vocabulary.HAS_EMAIL_ADDRESS, emailAddress);
		delta.bothDP(getId(), Vocabulary.HAS_SERVER_NAME, serverName);
		delta.bothDPN(getId(), Vocabulary.HAS_NICK_NAME, nickName);
	}

	private void storeCalendarDiffs(String calURI, DavResource cal, Delta delta)
	{
		Model model = cache.getDefaultModel();
		query(() ->
		{
			try (QueryExecution qexec = QueryExecutionFactory.create(calendarProperties(calURI), model))
			{
				ResultSet rs = qexec.execSelect();
				updateTriple(model, delta, calURI, Vocabulary.HAS_CTAG, cal.getCustomProps().get("getctag"));
				while (rs.hasNext())
				{
					QuerySolution soln = rs.next();
					if (!soln.getLiteral(NAME).equals(L(model, cal.getDisplayName())))
					{
						updateTriple(model, delta, calURI, Vocabulary.HAS_NAME, cal.getDisplayName());
					}
				}
			}
		});
	}

	private void storeEventDiffs(String eventURI, DavResource serverEvent, Delta delta) throws Exception
	{
		VEvent vEvent = Biweekly.parse(sardine.get(serverHeader + serverEvent)).first().getEvents().get(0);

		Model model = cache.getDefaultModel();
		query(() -> updateTriple(model, delta, eventURI, Vocabulary.HAS_ETAG, serverEvent.getEtag()));

		query(() ->
		{
			try (QueryExecution qexec = QueryExecutionFactory.create(eventProperties(eventURI), model))
			{
				ResultSet rs = qexec.execSelect();
				while (rs.hasNext())
				{
					QuerySolution soln = rs.next();

					if (vEvent.getDateEnd() != null)
					{
						if (!soln.getLiteral(DATEEND).equals(L(model,
								new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateEnd().getValue()))))))
						{

							updateTriple(model, delta, eventURI, Vocabulary.HAS_DATE_END,
									new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateEnd().getValue()))));
						}
					}
					else
					{
						if (!soln.getLiteral(DATEEND).equals(L(model, new XSDDateTime(GregorianCalendar
								.from(fromDate(vEvent.getDateStart().getValue())
										.plus(Duration.parse(vEvent.getDuration().getValue().toString())))))))
						{

							updateTriple(model, delta, eventURI, Vocabulary.HAS_DATE_END, new XSDDateTime(
									GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue())
											.plus(Duration.parse(vEvent.getDuration().getValue().toString())))));
						}
					}

					if (!soln.getLiteral(SUMMARY).equals(L(model, vEvent.getSummary().getValue())))
					{

						updateTriple(model, delta, eventURI, Vocabulary.HAS_SUMMARY, vEvent.getSummary().getValue());
					}
					else if (!soln.getLiteral(DATESTART).equals(L(model,
							new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue()))))))
					{

						updateTriple(model, delta, eventURI, Vocabulary.HAS_DATE_START,
								new XSDDateTime(GregorianCalendar.from(fromDate(vEvent.getDateStart().getValue()))));
					}
					else if (vEvent.getDescription() != null && soln.getLiteral(DESCRIPTION) != null)
					{
						if (!soln.getLiteral(DESCRIPTION).equals(L(model, vEvent.getDescription().getValue())))
						{

							updateTriple(model, delta, eventURI, Vocabulary.HAS_DESCRIPTION,
									vEvent.getDescription().getValue());
						}
					}
					else if (vEvent.getPriority() != null && soln.getLiteral(PRIORITY) != null)
					{
						if (!soln.getLiteral(PRIORITY).equals(L(model, vEvent.getPriority().getValue())))
						{

							updateTriple(model, delta, eventURI, Vocabulary.HAS_PRIORITY,
									vEvent.getPriority().getValue());
						}
					}
				}
			}
		});
	}

	//	populate the events for the calendar
	private Set<DavResource> populateEvents(DavResource calendar, Map<String, DavResource> calEvents) throws IOException
	{
		// get events, 1st iteration is the calendar uri, so skip it
		var davEvents = sardine.list(serverHeader + calendar).iterator();
		davEvents.next();

		var adds = new HashSet<DavResource>(1000);
		while (davEvents.hasNext())
		{
			DavResource serverEvent = davEvents.next();
			calEvents.put(encode(calendar, serverEvent, emailAddress), serverEvent);
			adds.add(serverEvent);
		}

		return adds;
	}

	public class SyncTask implements Callable<Void>
	{
		@Override
		public Void call()
		{
			try
			{
				System.out.println(":::::::::::::::::::::::::: IN SYNCH THREAD ::::::::::::::::::::::::::::::::: ");

				Set<String> storedCalendars = getStored(DFetch.calendarURIs(getId()), CALRES);
				var delta = getDelta();
				Iterator<DavResource> calDavResources = sardine.list(serverName).iterator();
				// 1st iteration is not a calendar, just the enclosing directory
				calDavResources.next();

				Set<String> serverCalURIs = new HashSet<>(10);
				// During this loop, I can check the CTAGS, Check if need to be added/deleted
				// Maybe all at once later on
				while (calDavResources.hasNext())
				{
					DavResource serverCal = calDavResources.next();
					String serverCalURI = Encode.encode(serverCal, emailAddress);
					serverCalURIs.add(serverCalURI);

					if (!calendars.containsKey(serverCalURI))
					{
						calendars.put(serverCalURI, serverCal);
					}

					// Calendar not in DB, store it and events
					if (!storedCalendars.contains(serverCalURI))
					{
						// Add Events
						Iterator<DavResource> davEvents = sardine.list(serverHeader + serverCal).iterator();
						// 1st iteration is the calendar uri, so skip
						davEvents.next();

						Collection<DavResource> addEvent = new HashSet<>(1000);
						Map<String, DavResource> eventURIToRes = new ConcurrentHashMap<>();
						while (davEvents.hasNext())
						{
							DavResource serverEvent = davEvents.next();
							eventURIToRes.put(encode(serverCal, serverEvent, emailAddress), serverEvent);
							addEvent.add(serverEvent);
						}

						events.put(serverCalURI, eventURIToRes);

						addCalendar(delta, getId(), serverCalURI, serverCal);

						for (var event : addEvent)
						{
							DStore.addEvent(delta, serverCalURI, encode(serverCal, event, emailAddress),
									Biweekly.parse(sardine.get(serverHeader + event)).first().getEvents()
											.get(0), event);
						}
					}
					// Calendar already exists, check if CTags differ, check if names differ
					else
					{
						if (!events.containsKey(serverCalURI))
						{
							events.put(serverCalURI, new ConcurrentHashMap<>(100));
							Iterator<DavResource> davEvents = sardine.list(serverHeader + serverCal).iterator();
							// 1st iteration is the calendar uri, so skip
							davEvents.next();

							while (davEvents.hasNext())
							{
								DavResource serverEvent = davEvents.next();
								events.get(serverCalURI)
										.put(encode(serverCal, serverEvent, emailAddress), serverEvent);
							}

						}

						if (!getStoredTag(calendarCTag(serverCalURI), CTAG)
								.equals(serverCal.getCustomProps().get("getctag")))
						{
							System.out.println(
									":::::::::::::::::::::::::::::::::: C TAG HAS CHANGED :::::::::::::::::::::::::::::::::::::::::::::");
							calendars.put(serverCalURI, serverCal);
							storeCalendarDiffs(serverCalURI, serverCal, delta);

							Set<String> storedEvents = getStored(eventURIs(serverCalURI), EVENTRES);
							Set<DavResource> addEvents = new HashSet<>();
							Set<String> serverEventURIs = new HashSet<>();

							Iterator<DavResource> davEvents = sardine.list(serverHeader + serverCal).iterator();
							// 1st iteration is the calendar uri, so skip
							davEvents.next();

							while (davEvents.hasNext())
							{
								DavResource serverEvent = davEvents.next();
								String serverEventURI = encode(serverCal, serverEvent, emailAddress);
								serverEventURIs.add(serverEventURI);

								if (!events.containsKey(serverCalURI))
								{
									events.put(serverCalURI, new ConcurrentHashMap<>(100));
								}

								// New Event, store it
								if (!storedEvents.contains(serverEventURI))
								{
									events.get(serverCalURI).put(serverEventURI, serverEvent);
									addEvents.add(serverEvent);
								}
								// Not new event, compare ETAGS
								else
								{
									String storedTAG = getStoredTag(eventETag(serverEventURI), ETAG).replace("\\", "");

									if (!storedTAG.equals(serverEvent.getEtag()))
									{
										storeEventDiffs(serverEventURI, serverEvent, delta);
										events.get(serverCalURI).put(serverEventURI, serverEvent);
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
									events.get(serverCalURI).remove(currEventURI);
								}
							}

							removeEvent.forEach(event ->
									query(() -> unstoreRes(cache.getDefaultModel(), delta, serverCalURI, event)));

							for (var event : addEvents)
							{
								DStore.addEvent(delta, serverCalURI, encode(serverCal, event, emailAddress),
										Biweekly.parse(sardine.get(serverHeader + event)).first().getEvents()
												.get(0), event);
							}
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

						if (events.get(storedCalURI) != null)
						{
							events.remove(storedCalURI);
						}

						if (calendars.get(storedCalURI) != null)
						{
							calendars.remove(storedCalURI);
						}

						currStoredEvents.forEach(
								eventURI -> query(() ->
										unstoreRes(cache.getDefaultModel(), delta, storedCalURI,
												eventURI)));

						query(() -> unstoreRes(cache.getDefaultModel(), delta, getId(), storedCalURI));
					}
				}

				var event = new EventSetBuilder();
				var eid = event.newEvent(Vocabulary.ACCOUNT_SYNCED);
				event.addOP(eid, Vocabulary.HAS_ACCOUNT, id);

				applyAndNotify(delta, event);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}

			return null;
		}
	}

	private Future<?> toServer(String oid, Model operation)
	{
//		CAL PATH /dav/calendars/user/theaardvark@fastmail.com/2c809517-316a-4aa7-968f-e29178f7c244/
//			EVENT PATH /dav/calendars/user/theaardvark@fastmail.com/2c809517-316a-4aa7-968f-e29178f7c244/659d3e5f-8882-455f-80f6-2e6a1add4059.ics

		var cid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_CALENDAR).toString();
		var mid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_EVENT).toString();

		DavResource serverCal = calendars.get(cid);
		System.out.println(serverCal);
		System.out.println(serverCal.getPath());
		var target = serverHeader + serverCal;
		System.out.println("TARGET = " + target);

		var title = JenaUtils.getS(operation, mid, Vocabulary.HAS_SUMMARY);
		String location = null;
		try
		{
			location = JenaUtils.getS(operation, mid, Vocabulary.HAS_LOCATION);
		}
		catch (NoSuchElementException ex)
		{
			//	the account doesn't have a nick name
		}

		var start = JenaUtils.getDT(operation, mid, Vocabulary.HAS_DATE_START);
		var end = JenaUtils.getDT(operation, mid, Vocabulary.HAS_DATE_END);

		var foo = location;
		return addWork(() -> {
			System.out.println("ADD EVENT");
			System.out.println(title);
			System.out.println(foo);
			System.out.println(start);
			System.out.println(end);

			VEvent event = new VEvent();

			Summary summary = event.setSummary(title);
			summary.setLanguage("en-us");

			if (foo != null)
			{
				event.setLocation(foo);
			}

			Date startDate = Date.from(start.toInstant());// new Date(lastSearchDate.atStartOfDay(ZoneId.of("America/New_York")).toEpochSecond() * 1000);
			Date endDate = Date.from(end.toInstant());// new Date(lastSearchDate.atStartOfDay(ZoneId.of("America/New_York")).toEpochSecond() * 1000);

			event.setDateStart(startDate);
			event.setDateEnd(endDate);


//			Duration duration = new Duration.Builder().hours(1).build();
//			event.setDuration(duration);

//			Recurrence recur = new Recurrence.Builder(Frequency.WEEKLY).interval(2).build();
//			event.setRecurrenceRule(recur);

			//	have to generate a unique UID for the vevent somehow

			String eventPath = Vocabulary.E(serverName.substring(0, serverName.length() - 1),
					calendars.get(cid).getName(), event.getUid().getValue() + ".ics");
			event.setUrl(eventPath);
			System.out.println(event.getUrl());
			System.out.println(event.getUrl().toString().contains("\n"));

			ICalendar ical = new ICalendar();
			ical.setProductId(Babbage.class.getCanonicalName());
			ical.addEvent(event);

			String str = Biweekly.write(ical).go();

//			int s = str.indexOf("BEGIN:VEVENT");
//			int e = str.indexOf("END:VEVENT");
//
//			str = str.substring(s, e + "END:VEVENT".length());

			System.out.println(str);


			try
			{
				sardine.enablePreemptiveAuthentication("caldav.fastmail.com");
				System.out.println(sardine.exists(
						Vocabulary.E(serverName, calendars.get(cid).getName())));
				sardine.put(eventPath, IOUtils.toInputStream(str, "UTF-8"), "/text/calendar", true);
			}
			catch (IOException ex)
			{
				ex.printStackTrace(System.err);
			}
			return null;
		});
	}
}

//		operations.put(Vocabulary.SYNC, this::syncOp);
//		operations.put(Vocabulary.SYNC_AHEAD, this::syncAhead);
