package org.knowtiphy.babbage.storage.CARDDAV;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.knowtiphy.babbage.storage.BaseAdapter;
import org.knowtiphy.babbage.storage.CALDAV.CALDAVAdapter;
import org.knowtiphy.babbage.storage.IMAP.DStore;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import java.util.HashSet;
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

import static org.knowtiphy.babbage.storage.CALDAV.DStore.P;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.R;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.initialState;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.skeleton;

public class CARDDAVAdapter extends BaseAdapter
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
	private final Map<String, DavResource> m_addressBook = new ConcurrentHashMap<>(10);
	private final Map<String, Map<String, DavResource>> m_PerBookCards = new ConcurrentHashMap<>(10);
	private final BlockingQueue<Runnable> workQ;
	//private final BlockingQueue<Runnable> contentQ;
	private final Thread doWork;
	private final Mutex accountLock;
	private String nickName;
	private Thread synchThread;

	// Start modeling after what is currently in the IMAPAdpater
	public CARDDAVAdapter(String name, Dataset messageDatabase, ListenerManager listenerManager,
			BlockingDeque<Runnable> notificationQ, Model model)
	{
		super(messageDatabase, listenerManager, notificationQ);
		System.out.println("CARDDAVAdapter INSTANTIATED");

		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_SERVER_NAME));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_EMAIL_ADDRESS));
		assert JenaUtils.checkUnique(JenaUtils.listObjectsOfProperty(model, name, Vocabulary.HAS_PASSWORD));

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

	protected String encodeAddressBook(DavResource addressBook)
	{
		return Vocabulary.E(Vocabulary.CARDDAV_ADDRESSBOOK, getEmailAddress(), addressBook.getHref());
	}

	protected String encodeCard(DavResource addressBook, DavResource card)
	{
		return Vocabulary.E(Vocabulary.CARDDAV_CARD, getEmailAddress(), addressBook.getHref(), card.getHref());
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

	public String getEmailAddress()
	{
		return emailAddress;
	}

	private <T> void updateTriple(Model messageDB, Model adds, Model deletes, String resURI, String hasProp, T updated)
	{
		deletes.add(messageDB.listStatements(R(messageDB, resURI), P(messageDB, hasProp), (RDFNode) null));
		adds.add(R(messageDB, resURI), P(messageDB, hasProp), messageDB.createTypedLiteral(updated));
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
				P(accountTriples, Vocabulary.CARDDAV_ACCOUNT));
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_NAME), serverName);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_EMAIL_ADDRESS), emailAddress);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_PASSWORD), password);
		accountTriples.add(R(accountTriples, id), P(accountTriples, Vocabulary.HAS_SERVER_HEADER), serverHeader);
		if (nickName != null)
		{
			accountTriples
					.add(org.knowtiphy.babbage.storage.IMAP.DStore.R(accountTriples, id), DStore
							.P(accountTriples, Vocabulary.HAS_NICK_NAME), nickName);
		}
		// Notify the client of the account triples
		notifyListeners(accountTriples);

		messageDatabase.begin(ReadWrite.READ);
		Model mAddressBookDetails = QueryExecutionFactory.create(skeleton(), messageDatabase.getDefaultModel())
				.execConstruct();
		messageDatabase.end();
		notifyListeners(mAddressBookDetails);

		messageDatabase.begin(ReadWrite.READ);
		Model mCardDetails = QueryExecutionFactory.create(initialState(), messageDatabase.getDefaultModel())
				.execConstruct();
		messageDatabase.end();
		notifyListeners(mCardDetails);

	}

	@Override public String getId()
	{
		return id;
	}

	private void startSynchThread()
	{
//		synchThread = new Thread(() -> {
//			while (true)
//			{
//				try
//				{
//					workQ.add(() -> {
//
//						try
//						{
//
//							accountLock.lock();
//
//							System.out.println(
//									":::::::::::::::::::::::::: IN CARDDAV SYNCH THREAD ::::::::::::::::::::::::::::::::: ");
//
//							Set<String> storedAddressBooks = getStored(addressBookURIs(getId()), ABOOKRES);
//
//							Iterator<DavResource> cardDavResources = sardine.list(serverName).iterator();
//							// 1st iteration is not a calendar, just the enclosing directory
//							cardDavResources.next();
//
//							Set<String> serverBookURIs = new HashSet<>(10);
//							// During this loop, I can check the CTAGS, Check if need to be added/deleted
//							// Maybe all at once later on
//							while (cardDavResources.hasNext())
//							{
//								DavResource serverBook = cardDavResources.next();
//								String serverBookURI = encodeAddressBook(serverBook);
//								serverBookURIs.add(serverBookURI);
//
//								if (!m_addressBook.containsKey(serverBookURI))
//								{
//									m_addressBook.put(serverBookURI, serverBook);
//								}
//
//								// AddressBook not in DB, store it and cards
//								if (!storedAddressBooks.contains(serverBookURI))
//								{
//									m_addressBook.put(serverBookURI, serverBook);
//
//									// Add Cards
//									Iterator<DavResource> davCards = sardine.list(serverHeader + serverBook).iterator();
//									// 1st iteration is the addressBook uri, so skip
//									davCards.next();
//
//									Collection<DavResource> addCard = new HashSet<>(1000);
//									Map<String, DavResource> cardURIToRes = new ConcurrentHashMap<>();
//									while (davCards.hasNext())
//									{
//										DavResource serverEvent = davCards.next();
//										cardURIToRes.put(encodeCard(serverBook, serverEvent), serverEvent);
//										addCard.add(serverEvent);
//									}
//
//									m_PerBookCards.put(serverBookURI, cardURIToRes);
//
//									Model addAddressBook = ModelFactory.createDefaultModel();
//									storeAddressBook(addAddressBook, getId(), serverBookURI, serverBook);
//									update(addAddressBook, ModelFactory.createDefaultModel());
//
//									Model addVCards = ModelFactory.createDefaultModel();
//
//									addCard.forEach(card -> {
//										try
//										{
//											storeCard(addVCards, serverBookURI, encodeCard(serverBook, card),
//													Ezvcard.parse(sardine.get(serverHeader + card)).first(), card);
//										} catch (IOException e)
//										{
//											e.printStackTrace();
//										}
//									});
//
//									update(addVCards, ModelFactory.createDefaultModel());
//
//								}
//								// Calendar already exists, check if CTags differ, check if names differ
//								else
//								{
//									if (!getStoredTag(addressBookCTAG(serverBookURI), CTAG)
//											.equals(serverBook.getCustomProps().get("getctag")))
//									{
//										System.out.println(
//												":::::::::::::::::::::::::::::::::: C TAG HAS CHANGED :::::::::::::::::::::::::::::::::::::::::::::");
//										m_addressBook.put(serverBookURI, serverBook);
//										storeCalendarDiffs(serverBookURI, serverBook);
//
//										Set<String> storedCards = getStored(cardURIs(serverBookURI), CARDRES);
//										Set<DavResource> addCards = new HashSet<>();
//										Set<String> serverCardURIs = new HashSet<>();
//
//										Iterator<DavResource> davCards = sardine.list(serverHeader + serverBook)
//												.iterator();
//										// 1st iteration is the addressBook uri, so skip
//										davCards.next();
//
//										while (davCards.hasNext())
//										{
//											DavResource serverCard = davCards.next();
//											String serverCardURI = encodeCard(serverBook, serverCard);
//											serverCardURIs.add(serverCardURI);
//
//											if (!m_PerBookCards.containsKey(serverBookURI))
//											{
//												m_PerBookCards.put(serverBookURI, new ConcurrentHashMap<>(100));
//											}
//
//											// New Card, store it
//											if (!storedCards.contains(serverCardURI))
//											{
//												m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCard);
//												addCards.add(serverCard);
//											}
//											// Not new event, compare ETAGS
//											else
//											{
//												m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCard);
//
//												String storedTAG = getStoredTag(contactETAG(serverCardURI), ETAG)
//														.replace("\\", "");
//
//												if (!storedTAG.equals(serverCard.getEtag()))
//												{
//													storeEventDiffs(serverCardURI, serverCard);
//												}
//											}
//
//										}
//
//										// Cards to be removed, this needs to be events in the DB
//										Collection<String> removeCard = new HashSet<>();
//										for (String currCardURI : storedCards)
//										{
//											if (!serverCardURIs.contains(currCardURI))
//											{
//												removeCard.add(currCardURI);
//												m_PerBookCards.get(serverBookURI).remove(currCardURI);
//											}
//										}
//
//										messageDatabase.begin(ReadWrite.READ);
//										Model deletes = ModelFactory.createDefaultModel();
//										removeCard.forEach(
//												event -> unstoreRes(messageDatabase.getDefaultModel(), deletes,
//														serverBookURI, event));
//										messageDatabase.end();
//
//										Model adds = ModelFactory.createDefaultModel();
//										addCards.forEach(event -> {
//											try
//											{
//												storeCard(adds, serverBookURI, encodeCard(serverBook, event),
//														Biweekly.parse(sardine.get(serverHeader + event)).first()
//																.getEvents().get(0), event);
//											} catch (IOException e)
//											{
//												e.printStackTrace();
//											}
//										});
//
//										update(adds, deletes);
//
//									}
//								}
//
//							}
//
//							// AddressBooks to be removed
//							// For every AddressBook URI in m_AddressBook, if server does not contain it, remove it
//							for (String storedAddressBookURI : storedAddressBooks)
//							{
//
//								if (!serverBookURIs.contains(storedAddressBookURI))
//								{
//									Set<String> currStoredCards = getStored(cardURIs(storedAddressBookURI), CARDRES);
//
//									Model deleteEvent = ModelFactory.createDefaultModel();
//									messageDatabase.begin(ReadWrite.READ);
//									currStoredCards.forEach(
//											eventURI -> unstoreRes(messageDatabase.getDefaultModel(), deleteEvent,
//													storedAddressBookURI, eventURI));
//									messageDatabase.end();
//
//									update(ModelFactory.createDefaultModel(), deleteEvent);
//
//									Model deleteCalendar = ModelFactory.createDefaultModel();
//									messageDatabase.begin(ReadWrite.READ);
//									unstoreRes(messageDatabase.getDefaultModel(), deleteCalendar, getId(),
//											storedAddressBookURI);
//									messageDatabase.end();
//
//									if (m_PerBookCards.get(storedAddressBookURI) != null)
//									{
//										m_addressBook.remove(storedAddressBookURI);
//									}
//
//									if (m_addressBook.get(storedAddressBookURI) != null)
//									{
//										m_addressBook.remove(storedAddressBookURI);
//									}
//
//									update(ModelFactory.createDefaultModel(), deleteCalendar);
//								}
//							}
//
//							accountLock.unlock();
//						} catch (Exception e)
//						{
//							e.printStackTrace();
//						}
//
//					});
//
//					Thread.sleep(FREQUENCY);
//				} catch (InterruptedException ex)
//				{
//					return;
//				}
//			}
//		});

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
