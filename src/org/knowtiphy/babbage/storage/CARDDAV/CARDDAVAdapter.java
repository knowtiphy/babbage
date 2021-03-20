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
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.babbage.storage.CALDAV.CALDAVAdapter;
import org.knowtiphy.babbage.storage.DaveAdapter;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.ListenerManager;
import org.knowtiphy.babbage.storage.Mutex;
import org.knowtiphy.babbage.storage.OldListenerManager;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.Pair;
import org.knowtiphy.utils.ThreeTuple;

import java.io.IOException;
import java.util.ArrayList;
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

import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.ABOOKRES;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.CARDRES;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.CTAG;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.ETAG;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.GROUPRES;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.NAME;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.addressBookCTAG;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.addressBookProperties;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.addressBookURIs;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.cardETAG;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.cardURIs;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.groupProperties;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.groupURIs;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.initialStateCards;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.memberCardURI;
import static org.knowtiphy.babbage.storage.CARDDAV.DFetch.skeleton;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.L;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.computeMemberCardDiffs;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.deleteGroup;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.deleteVCard;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.storeAddressBook;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.storeMemberCards;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.storeResource;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.storeResourceMeta;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.unStoreMeta;
import static org.knowtiphy.babbage.storage.CARDDAV.DStore.unstoreRes;

public class CARDDAVAdapter extends DaveAdapter
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
	private final Map<String, DavResource> m_addressBook = new ConcurrentHashMap<>(2);
	private final Map<String, Map<String, DavResource>> m_PerBookCards = new ConcurrentHashMap<>(2);
	private final BlockingQueue<Runnable> workQ;
	//private final BlockingQueue<Runnable> contentQ;
	private final Thread doWork;
	private final Mutex accountLock;
	private String nickName;
	private Thread synchThread;

	public CARDDAVAdapter(String name, String type, Dataset messageDatabase,
						  OldListenerManager listenerManager, ListenerManager newListenerManager,
						  BlockingDeque<Runnable> notificationQ, Model model)
	{
		super(type, messageDatabase, listenerManager, newListenerManager, notificationQ);

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

		this.id = Vocabulary.E(Vocabulary.CARDDAV_ACCOUNT, emailAddress);

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

		apply(delta -> {
			updateTriple(messageDB, delta, addressBookUri, Vocabulary.HAS_CTAG,
					addressBook.getCustomProps().get("getctag"));
		});

		applyAndNotify(delta -> {
			ResultSet rs = QueryExecutionFactory.create(addressBookProperties(addressBookUri), messageDB).execSelect();
			while (rs.hasNext())
			{
				QuerySolution soln = rs.next();
				if (!soln.getLiteral(NAME).equals(L(messageDB, addressBook.getDisplayName())))
				{
					updateTriple(messageDB, delta, addressBookUri, Vocabulary.HAS_NAME, addressBook.getDisplayName());
				}
			}

		});

	}

	private void storeResourceDiffs(String serverBookURI, String resourceURI, DavResource serverResource)
			throws Exception
	{
		VCard vCard = Ezvcard.parse(sardine.get(serverHeader + serverResource)).first();
		Model messageDB = messageDatabase.getDefaultModel();

		if (!vCard.getExtendedProperties().isEmpty() && vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-KIND").get(0)
				.getValue().equals("group"))
		{

			// get old set of MemberCards
			Collection<String> oldMemCards = getStored(memberCardURI(resourceURI), CARDRES);

			// Delete old memberUID Info
			apply(delta -> {
				unStoreMeta(messageDB, delta, resourceURI);
			});

			// Store new memberUID Info
			apply(delta -> {
				storeResourceMeta(delta, new ThreeTuple<>(resourceURI, serverResource, vCard), new ArrayList<>());
			});

			// Get updated set of memberCards
			Collection<String> updatedMemCards = getStored(memberCardURI(resourceURI), CARDRES);

			System.out.println("GROUP CHANGE");
			// relations to delete, relations to add. old vs new mem cards

			applyAndNotify(delta -> {

				ResultSet rs = QueryExecutionFactory.create(groupProperties(resourceURI), messageDB).execSelect();
				while (rs.hasNext())
				{
					QuerySolution soln = rs.next();
					if (!soln.getLiteral(NAME).equals(L(messageDB, vCard.getFormattedName().getValue())))
					{
						updateTriple(messageDB, delta, resourceURI, Vocabulary.HAS_NAME,
								vCard.getFormattedName().getValue());
						System.out.println("NAME CHANGE");
					}
				}

				computeMemberCardDiffs(messageDB, delta, resourceURI, oldMemCards, updatedMemCards);

			});
		}
		else
		{
			apply(delta -> {
				unStoreMeta(messageDB, delta, resourceURI);
			});

			applyAndNotify(delta -> {
				deleteVCard(messageDB, delta, serverBookURI, resourceURI);
				storeResource(delta, new ArrayList<>(), serverBookURI, resourceURI, vCard, serverResource);
			});

			apply(delta -> {
				storeResourceMeta(delta, new ThreeTuple<>(resourceURI, serverResource, vCard), new ArrayList<>());
			});

		}

	}

	@Override public void close(Model model)
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
		accountInfo.addOP(id, RDF.type.toString(), Vocabulary.CARDDAV_ACCOUNT)
				.addDP(id, Vocabulary.HAS_SERVER_NAME, serverName).addDP(id, Vocabulary.HAS_EMAIL_ADDRESS, emailAddress)
				.addDP(id, Vocabulary.HAS_PASSWORD, password);
		if (nickName != null)
		{
			accountInfo.addDP(id, Vocabulary.HAS_NICK_NAME, nickName);
		}

		notifyOldListeners(accountInfo);

		queryAndNotify(delta -> delta
				.add(QueryExecutionFactory.create(skeleton(), messageDatabase.getDefaultModel()).execConstruct()));

		queryAndNotify(delta -> delta
				.add(QueryExecutionFactory.create(initialStateCards(), messageDatabase.getDefaultModel())
						.execConstruct()));

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
					workQ.add(new SyncTask());

					Thread.sleep(FREQUENCY);
				} catch (InterruptedException ex)
				{
					return;
				}
			}
		});

		synchThread.start();
	}

	private FutureTask<?> getSynchTask() throws UnsupportedOperationException
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

					if (queue.peek() instanceof CALDAVAdapter.SyncTask)
					{
						queue.take();
					}

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

	public class SyncTask implements Runnable
	{

		@Override public void run()

		{
			try
			{

				accountLock.lock();

				System.out.println(
						":::::::::::::::::::::::::: IN CARDDAV SYNCH THREAD ::::::::::::::::::::::::::::::::: ");

				Set<String> storedAddressBooks = getStored(addressBookURIs(getId()), ABOOKRES);

				Iterator<DavResource> cardDavResources = sardine.list(serverName).iterator();
				// 1st resource is actually the ADDRESS BOOK??? What, so if something has multple addressbook,
				// will need to check the contentType of this davResource and do something based on that
				DavResource serverBookRes = cardDavResources.next();
				String serverBookURI = encodeAddressBook(serverBookRes);

				// Apparently some servers allow you to delete addressBooks....
				Set<String> serverBookURIs = new HashSet<>(10);
				serverBookURIs.add(serverBookURI);

				if (!m_addressBook.containsKey(serverBookURI))
				{
					m_addressBook.put(serverBookURI, serverBookRes);
					m_PerBookCards.put(serverBookURI, new ConcurrentHashMap<>(100));
				}

				// Addressbook not in DB
				if (!storedAddressBooks.contains(serverBookURI))
				{
					applyAndNotify(delta -> {
						storeAddressBook(delta, getId(), serverBookURI, serverBookRes);
					});

					// For explicit sake, everything from here on, should be a vCard of some sort
					Iterator<DavResource> davCards = cardDavResources;
					//Collection<Pair<String, DavResource>> addCard = new HashSet<>(1000);

					// From here on, every one of these resouces is an addressbook resouce
					while (davCards.hasNext())
					{
						DavResource serverCardRes = davCards.next();
						String serverCardURI = encodeCard(serverBookRes, serverCardRes);

						m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCardRes);

						// Can I use map instead of this???? Prob
						//addCard.add(new Pair<>(serverCardURI, serverCardRes));
					}

					// Tuple to further Store data without notifying client
					Collection<ThreeTuple<String, DavResource, VCard>> furtherProcess = new ArrayList<>();
					applyAndNotify(delta -> {
						m_PerBookCards.get(serverBookURI).forEach((key, value) -> {
							try
							{
								VCard vCard = Ezvcard.parse(sardine.get(serverHeader + value)).first();
								storeResource(delta, furtherProcess, serverBookURI, key, vCard, value);
							} catch (IOException e)
							{
								e.printStackTrace();
							}
						});

					});

					// New groups to notify client of its card relations
					Collection<String> toAddGroupURIs = new ArrayList<>();
					apply(delta -> {
						// Store UIDs for Contacts, memberUIds for Groups, and ETags for both
						furtherProcess.forEach(element -> storeResourceMeta(delta, element, toAddGroupURIs));

					});

					toAddGroupURIs.forEach(groupURI -> {
						Collection<String> memberCards = getStored(memberCardURI(groupURI), CARDRES);
						applyAndNotify(delta -> {
							storeMemberCards(delta, groupURI, memberCards);
						});
					});

				}
				// Addressbook already exists, check if CTags differ, check if names differ
				else
				{
					if (m_PerBookCards.get(serverBookURI).isEmpty())
					{
						Iterator<DavResource> davCards = sardine.list(serverName).iterator();
						davCards.next();
						while (davCards.hasNext())
						{
							DavResource serverCardRes = davCards.next();
							String serverCardURI = encodeCard(serverBookRes, serverCardRes);
							m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCardRes);
						}
					}

					if (!getStoredTag(addressBookCTAG(serverBookURI), CTAG)
							.equals(serverBookRes.getCustomProps().get("getctag")))
					{
						System.out.println(
								":::::::::::::::::::::::::::::::::: CARDDAV C TAG HAS CHANGED :::::::::::::::::::::::::::::::::::::::::::::");
						m_addressBook.put(serverBookURI, serverBookRes);
						storeAddressBookDiffs(serverBookURI, serverBookRes);

						Iterator<DavResource> davCards = sardine.list(serverName).iterator();
						// 1st iteration is the addressBook uri, so skip
						davCards.next();

						Set<String> storedCards = getStored(cardURIs(serverBookURI), CARDRES);
						Set<String> storedGroups = getStored(groupURIs(serverBookURI), GROUPRES);

						Set<Pair<String, DavResource>> addCards = new HashSet<>();
						Set<String> serverCardURIs = new HashSet<>();

						while (davCards.hasNext())
						{
							DavResource serverCardRes = davCards.next();
							String serverCardURI = encodeCard(serverBookRes, serverCardRes);
							serverCardURIs.add(serverCardURI);

							if (!m_PerBookCards.get(serverBookURI).containsKey(serverCardURI))
							{
								m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCardRes);
							}

							// New Card, store it
							if (!storedCards.contains(serverCardURI) && !storedGroups.contains(serverCardURI))
							{
								addCards.add(new Pair<>(serverCardURI, serverCardRes));
							}
							// Not new event, compare ETAGS
							else
							{
								m_PerBookCards.get(serverBookURI).put(serverCardURI, serverCardRes);

								String storedTAG = getStoredTag(cardETAG(serverCardURI), ETAG).replace("\\", "");

								if (!storedTAG.equals(serverCardRes.getEtag()))
								{
									storeResourceDiffs(serverBookURI, serverCardURI, serverCardRes);
								}
							}

						}

						// Cards to be removed, this needs to be events in the DB
						Collection<String> removeCard = new ArrayList<>(90);
						Collection<String> removeGroup = new ArrayList<>(10);

						for (String currCardURI : storedCards)
						{
							if (!serverCardURIs.contains(currCardURI))
							{
								System.out.println("REMOVED CARD");
								removeCard.add(currCardURI);
								m_PerBookCards.get(serverBookURI).remove(currCardURI);
							}
						}

						for (String currCardURI : storedGroups)
						{
							if (!serverCardURIs.contains(currCardURI))
							{
								System.out.println("REMOVED GROUP");
								removeGroup.add(currCardURI);
								m_PerBookCards.get(serverBookURI).remove(currCardURI);
							}
						}

						apply(delta -> {
							removeCard.forEach(
									card -> unstoreRes(messageDatabase.getDefaultModel(), delta, serverBookURI, card));
							removeGroup.forEach(
									group -> unstoreRes(messageDatabase.getDefaultModel(), delta, serverBookURI,
											group));
						});

						Collection<ThreeTuple<String, DavResource, VCard>> furtherProcess = new ArrayList<>();
						applyAndNotify(delta -> {

							removeCard.forEach(
									card -> deleteVCard(messageDatabase.getDefaultModel(), delta, serverBookURI, card));

							removeGroup.forEach(
									group -> deleteGroup(messageDatabase.getDefaultModel(), delta, serverBookURI,
											group));

							addCards.forEach(card -> {
								try
								{
									VCard vCard = Ezvcard.parse(sardine.get(serverHeader + card.snd())).first();
									storeResource(delta, furtherProcess, serverBookURI, card.fst(), vCard, card.snd());
								} catch (IOException e)
								{
									e.printStackTrace();
								}
							});
						});

						// New groups to notify client of its card relations
						Collection<String> toAddGroupURIs = new ArrayList<>();
						apply(delta -> {
							furtherProcess.forEach(tuple -> storeResourceMeta(delta, tuple, toAddGroupURIs));
						});

						toAddGroupURIs.forEach(groupURI -> {
							Collection<String> memberCards = getStored(memberCardURI(groupURI), CARDRES);
							applyAndNotify(delta -> {
								storeMemberCards(delta, groupURI, memberCards);
							});
						});

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

						applyAndNotify(delta -> {
							currStoredCards.forEach(eventURI -> unstoreRes(messageDatabase.getDefaultModel(), delta,
									storedAddressBookURI, eventURI));
						});

						applyAndNotify(delta -> {
							unstoreRes(messageDatabase.getDefaultModel(), delta, getId(), storedAddressBookURI);
						});

					}
				}

				accountLock.unlock();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
