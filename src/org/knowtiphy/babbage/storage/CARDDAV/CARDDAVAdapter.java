package org.knowtiphy.babbage.storage.CARDDAV;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import ezvcard.Ezvcard;
import ezvcard.VCard;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.BaseAdapter;
import org.knowtiphy.babbage.storage.CALDAV.CALDAVAdapter;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import java.io.IOException;
import java.util.Collection;
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

import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.*;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.*;

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
		return Vocabulary.E(Vocabulary.CARDDAV_ADDRESSBOOK, emailAddress, addressBook.getHref());
	}

	protected String encodeCard(DavResource addressBook, DavResource card)
	{
		return Vocabulary.E(Vocabulary.CARDDAV_CARD, emailAddress, addressBook.getHref(), card.getHref());
	}

	private void storeAddressBookDiffs(String addressBookUri, DavResource addressBook) throws Exception
	{
		Model messageDB = messageDatabase.getDefaultModel();

		update(() -> {
			Delta delta = new Delta();

			ResultSet rs = QueryExecutionFactory.create(addressBookProperties(addressBookUri), messageDB).execSelect();
			updateTriple(messageDB, delta, addressBookUri, Vocabulary.HAS_CTAG,
					addressBook.getCustomProps().get("getctag"));

			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();
				if (!soln.getLiteral(NAME).equals(L(messageDB, addressBook.getDisplayName())))
				{
					updateTriple(messageDB, delta, addressBookUri, Vocabulary.HAS_NAME, addressBook.getDisplayName());
				}
			}

			return delta;
		});

	}

	private void storeCardDiffs(String cardURI, DavResource serverCard) throws Exception
	{
		VCard vCard = Ezvcard.parse(sardine.get(serverHeader + serverCard)).first();

		Model messageDB = messageDatabase.getDefaultModel();

		update(() -> {

			Delta delta = new Delta();

			updateTriple(messageDB, delta, cardURI, Vocabulary.HAS_ETAG, serverCard.getEtag());

			ResultSet rs = QueryExecutionFactory.create(cardProperties(cardURI), messageDB).execSelect();
			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();

				if (!soln.getLiteral(FORMATTEDNAME).equals(L(messageDB, vCard.getFormattedName().getValue())))
				{

					updateTriple(messageDB, delta, cardURI, Vocabulary.HAS_SUMMARY,
							vCard.getFormattedName().getValue());
				}

				// Questions about checking if numbers and emails are diff, might just have to chuck out?

			}

			return delta;
		});

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
		Delta accountInfo = new Delta();
		accountInfo.addR(id, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_ACCOUNT)
				.addL(id, Vocabulary.HAS_SERVER_NAME, serverName).addL(id, Vocabulary.HAS_EMAIL_ADDRESS, emailAddress)
				.addL(id, Vocabulary.HAS_PASSWORD, password);
		if (nickName != null)
		{
			accountInfo.addL(id, Vocabulary.HAS_NICK_NAME, nickName);
		}

		notifyListeners(accountInfo);

		Delta skeleton = new Delta();
		skeleton.getAdds().add(query(
				() -> QueryExecutionFactory.create(skeleton(), messageDatabase.getDefaultModel()).execConstruct()));
		notifyListeners(skeleton);

		Delta initialState = new Delta();
		initialState.getAdds().add(query(
				() -> QueryExecutionFactory.create(initialState(), messageDatabase.getDefaultModel()).execConstruct()));

	}

	@Override public String getId()
	{
		return id;
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
									":::::::::::::::::::::::::: IN CARDDAV SYNCH THREAD ::::::::::::::::::::::::::::::::: ");

							Set<String> storedAddressBooks = getStored(addressBookURIs(getId()), ABOOKRES);

							Iterator<DavResource> cardDavResources = sardine.list(serverName).iterator();
							// 1st iteration is not a calendar, just the enclosing directory
							cardDavResources.next();

							Set<String> serverBookURIs = new HashSet<>(10);
							// During this loop, I can check the CTAGS, Check if need to be added/deleted
							// Maybe all at once later on
							while (cardDavResources.hasNext())
							{
								DavResource serverBook = cardDavResources.next();
								String serverBookURI = encodeAddressBook(serverBook);
								serverBookURIs.add(serverBookURI);

								if (!m_addressBook.containsKey(serverBookURI))
								{
									m_addressBook.put(serverBookURI, serverBook);
								}

								// AddressBook not in DB, store it and cards
								if (!storedAddressBooks.contains(serverBookURI))
								{
									m_addressBook.put(serverBookURI, serverBook);

									// Add Cards
									Iterator<DavResource> davCards = sardine.list(serverHeader + serverBook).iterator();
									// 1st iteration is the addressBook uri, so skip
									davCards.next();

									Collection<DavResource> addCard = new HashSet<>(1000);
									Map<String, DavResource> cardURIToRes = new ConcurrentHashMap<>();
									while (davCards.hasNext())
									{
										DavResource serverEvent = davCards.next();
										cardURIToRes.put(encodeCard(serverBook, serverEvent), serverEvent);
										addCard.add(serverEvent);
									}

									m_PerBookCards.put(serverBookURI, cardURIToRes);

									update(() -> {
										Delta delta = new Delta();
										storeAddressBook(delta, getId(), serverBookURI, serverBook);
										return delta;
									});

									update(() -> {
										Delta delta = new Delta();
										addCard.forEach(card -> {
											try
											{
												storeCard(delta, serverBookURI, encodeCard(serverBook, card),
														Ezvcard.parse(sardine.get(serverHeader + card)).first(), card);
											} catch (IOException e)
											{
												e.printStackTrace();
											}
										});

										return delta;
									});

								}
								// Calendar already exists, check if CTags differ, check if names differ
								else
								{
									if (!getStoredTag(addressBookCTAG(serverBookURI), CTAG)
											.equals(serverBook.getCustomProps().get("getctag")))
									{
										System.out.println(
												":::::::::::::::::::::::::::::::::: C TAG HAS CHANGED :::::::::::::::::::::::::::::::::::::::::::::");
										m_addressBook.put(serverBookURI, serverBook);
										storeAddressBookDiffs(serverBookURI, serverBook);

										Set<String> storedCards = getStored(cardURIs(serverBookURI), CARDRES);
										Set<DavResource> addCards = new HashSet<>();
										Set<String> serverCardURIs = new HashSet<>();

										Iterator<DavResource> davCards = sardine.list(serverHeader + serverBook)
												.iterator();
										// 1st iteration is the addressBook uri, so skip
										davCards.next();

										while (davCards.hasNext())
										{
											DavResource serverCard = davCards.next();
											String serverCardURI = encodeCard(serverBook, serverCard);
											serverCardURIs.add(serverCardURI);

											if (!m_PerBookCards.containsKey(serverBookURI))
											{
												m_PerBookCards.put(serverBookURI, new ConcurrentHashMap<>(100));
											}

											// New Card, store it
											if (!storedCards.contains(serverCardURI))
											{
												m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCard);
												addCards.add(serverCard);
											}
											// Not new event, compare ETAGS
											else
											{
												m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCard);

												String storedTAG = getStoredTag(cardETAG(serverCardURI), ETAG)
														.replace("\\", "");

												if (!storedTAG.equals(serverCard.getEtag()))
												{
													storeCardDiffs(serverCardURI, serverCard);
												}
											}

										}

										// Cards to be removed, this needs to be events in the DB
										Collection<String> removeCard = new HashSet<>();
										for (String currCardURI : storedCards)
										{
											if (!serverCardURIs.contains(currCardURI))
											{
												removeCard.add(currCardURI);
												m_PerBookCards.get(serverBookURI).remove(currCardURI);
											}
										}

										update(() -> {
											Delta delta = new Delta();

											removeCard.forEach(
													event -> unstoreRes(messageDatabase.getDefaultModel(), delta,
															serverBookURI, event));

											addCards.forEach(event -> {
												try
												{
													storeCard(delta, serverBookURI, encodeCard(serverBook, event),
															Ezvcard.parse(sardine.get(serverHeader + event)).first(),
															event);
												} catch (IOException e)
												{
													e.printStackTrace();
												}
											});

											return delta;
										});

									}
								}

							}

							// AddressBooks to be removed
							// For every AddressBook URI in m_AddressBook, if server does not contain it, remove it
							for (String storedAddressBookURI : storedAddressBooks)
							{

								if (!serverBookURIs.contains(storedAddressBookURI))
								{
									Set<String> currStoredCards = getStored(cardURIs(storedAddressBookURI), CARDRES);

									if (m_PerBookCards.get(storedAddressBookURI) != null)
									{
										m_addressBook.remove(storedAddressBookURI);
									}

									if (m_addressBook.get(storedAddressBookURI) != null)
									{
										m_addressBook.remove(storedAddressBookURI);
									}

									update(() -> {
										Delta delta = new Delta();
										currStoredCards.forEach(
												eventURI -> unstoreRes(messageDatabase.getDefaultModel(), delta,
														storedAddressBookURI, eventURI));
										return delta;
									});

									update(() -> {
										Delta delta = new Delta();
										unstoreRes(messageDatabase.getDefaultModel(), delta, getId(),
												storedAddressBookURI);
										return delta;
									});

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
