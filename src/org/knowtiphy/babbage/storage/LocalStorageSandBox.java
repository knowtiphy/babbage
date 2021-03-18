package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.knowtiphy.babbage.Babbage;
import org.knowtiphy.babbage.storage.CALDAV.CALDAVAdapter;
import org.knowtiphy.babbage.storage.CARDDAV.CARDDAVAdapter;
import org.knowtiphy.babbage.storage.IMAP.IMAPAdapter;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.OS;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.knowtiphy.utils.JenaUtils.R;

/**
 * A local storage layer for storing mail messages in a local RDF database (so in the file system of
 * the machine).
 *
 * @author graham
 */
public class LocalStorageSandBox implements IStorage
{
	private static final Logger LOGGER = Logger.getLogger(LocalStorageSandBox.class.getName());

	private static final String CACHE = "cache";
	private static final String ACCOUNTS_FILE = "accounts.ttl";

	private static final Runnable POISON_PILL = () -> {
	};

	//	can't use a SelectBuilder because addFilter can throw an exception
	private static final String getAccountType =
			"SELECT ?id ?type "
					+ " WHERE {?id <" + RDF.type + "> ?type \n."
					+ "		?type <" + RDFS.subClassOf + "> <" + Vocabulary.ACCOUNT + ">\n."
					+ "		filter(?type != <" + Vocabulary.ACCOUNT + ">)"
					+ "      }";

	private static final String OPERATION_QUERY =
			"SELECT ?id ?type ?aid"
					+ " WHERE {?id <" + RDF.type + "> ?type \n."
					+ "		?type <" + RDFS.subClassOf + "> <" + Vocabulary.OPERATION + ">\n."
					+ "		?id <" + Vocabulary.HAS_ACCOUNT + "> ?aid \n."
					+ "		filter(?type != <" + Vocabulary.OPERATION + ">)"
					+ "      }";

	// map from accountType -> class for the relevant adapter
	private static final Map<String, Class<?>> m_Class = new HashMap<>();

	static
	{
		m_Class.put(Vocabulary.IMAP_ACCOUNT, IMAPAdapter.class);
		m_Class.put(Vocabulary.CALDAV_ACCOUNT, CALDAVAdapter.class);
		m_Class.put(Vocabulary.CARDDAV_ACCOUNT, CARDDAVAdapter.class);
	}

	private final Dataset messageDatabase;

	private final OldListenerManager listenerManager = new OldListenerManager();
	private final ListenerManager newListenerManager = new ListenerManager();
	private final Map<String, IAdapter> adapters = new HashMap<>(100);

	private final BlockingDeque<Runnable> notificationQ = new LinkedBlockingDeque<>();

	public LocalStorageSandBox() throws Exception
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "()");

		//	data structures shared between accounts

		Path databaseLocation = Paths.get(OS.getDataDir(Babbage.class).toString(), CACHE);
		Files.createDirectories(databaseLocation);

		messageDatabase = TDB2Factory.connectDataset(databaseLocation.toString());

		//  read accounts -- TODO: this information should all be in a database not a file

		Path accountsDir = Paths.get(OS.getSettingsDir(Babbage.class).toString());
		Files.createDirectories(accountsDir);

		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, Files.newInputStream(Paths.get(OS.getSettingsDir(Babbage.class).toString(), ACCOUNTS_FILE)), Lang.TURTLE);
		Model accountsModel = createAccountsModel(model);

		//	start all the adapters

		ResultSet result = QueryExecutionFactory.create(getAccountType, accountsModel).execSelect();

		while (result.hasNext())
		{
			QuerySolution soln = result.next();
			Resource name = soln.getResource("id");
			Resource type = soln.getResource("type");
			LOGGER.log(Level.CONFIG, "{0} {1} {2}",
					new String[]{LocalStorageSandBox.class.getName(), name.toString(), type.toString()});

			@SuppressWarnings("unchecked")
			Class<IAdapter> cls = (Class<IAdapter>) m_Class.get(type.toString());
			IAdapter adapter = cls.getConstructor(String.class, String.class, Dataset.class,
					OldListenerManager.class, ListenerManager.class, BlockingDeque.class, Model.class)
					.newInstance(name.toString(), type.toString(), messageDatabase,
							listenerManager, newListenerManager, notificationQ, accountsModel);
			adapter.initialize();
			adapters.put(adapter.getId(), adapter);
		}

		//	thread which notifies listeners of changes
		//noinspection CallToThreadStartDuringObjectConstruction
		new Thread(() -> {
			while (true)
			{
				try
				{
					Runnable task = notificationQ.take();
					if (task == POISON_PILL)
					{
						return;
					}
					else
					{
						try
						{
							task.run();
						}
						catch (RuntimeException ex)
						{
							LOGGER.warning("Notifier thread failed :: " + ex.getLocalizedMessage());
						}
					}
				}
				catch (InterruptedException e)
				{
					return;
				}
			}
		}).start();

		//	TODO -- only really want to do this once
		var delta = new Delta();
		Vocabulary.allSubClasses.forEach((sub, sup) -> delta.addOP(sub, RDFS.subClassOf.toString(), sup));
		BaseAdapter.apply(messageDatabase, delta);

		LOGGER.exiting(this.getClass().getCanonicalName(), "()");
	}

	@Override
	public void close()
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "close");//"::()");

		for (IAdapter adapter : adapters.values())
		{
			adapter.close();
		}
		notificationQ.add(POISON_PILL);
		messageDatabase.close();

		LOGGER.exiting(this.getClass().getCanonicalName(), "close");//"::()");
	}

	private IAdapter A(String accountId)
	{
		IAdapter adapter = adapters.get(accountId);
		if (adapter == null)
		{
			//	TODO -- throw a no such account error
		}
		return adapter;
	}

	@Override
	public Model getAccounts()
	{
		Model accs = JenaUtils.createRDFSModel(Vocabulary.accountSubClasses);
		adapters.values().forEach(a -> accs.add(R(accs, a.getId()), RDF.type, R(accs, a.getType())));
		return accs;
	}

	@Override
	public Model getAccountInfo(String accountId)
	{
		return A(accountId).getAccountInfo();
	}

	@Override
	public Future<?> doOperation(Model operation)
	{
		var op = JenaUtils.createRDFSModel(operation, Vocabulary.operationsubClasses);

		var results = new LinkedList<Future<?>>();
		QueryExecutionFactory.create(OPERATION_QUERY, op).execSelect().forEachRemaining(
				soln -> {
					var oid = soln.getResource("id").toString();
					var type = soln.getResource("type").toString();
					var aid = soln.getResource("aid").toString();
					results.add(A(aid).doOperation(oid, type, op));
				});

		//	there is only one operation
		return results.getFirst();
	}

	@Override
	public void sync(String id, String fid) throws ExecutionException, InterruptedException
	{
		A(id).sync(fid);
	}

	@Override
	public Model getSpecialFolders()
	{
		Model model = ModelFactory.createDefaultModel();
		adapters.values().forEach(adapter -> model.add(adapter.getSpecialFolders()));
		return model;
	}

	@Override
	public ResultSet query(String id, String query)
	{
		return A(id).query(query);
	}

	@Override
	public ReadContext getReadContext()
	{
		return new ReadContext(messageDatabase);
	}

	public Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId)
	{
		return A(accountId).ensureMessageContentLoaded(folderId, messageId);
	}

	@Override
	public Future<?> loadAhead(String accountId, String folderId, Collection<String> messageIds)
	{
		return A(accountId).loadAhead(folderId, messageIds);
	}

	// Will call an addListener method in each adapter
	@Override
	public Map<String, FutureTask<?>> addOldListener(IOldStorageListener listener)
	{
		listenerManager.addListener(listener);

		for (IAdapter adapter : adapters.values())
		{
			// Add relevant triples to the model
			adapter.addListener();
		}

		// Start the synching of the IMAP org.knowtiphy.pinkpigmail.server, adds its work to the front of the Queue
		// but need to put these account of the Queue
		Map<String, FutureTask<?>> accountToFuture = new ConcurrentHashMap<>(100);
//		for (IAdapter adapter : m_adapter.values())
//		{
//			FutureTask<?> futureTask = adapter.getSynchTask();
//			accountToFuture.put(adapter.getId(), futureTask);
//			// For each account, spin up a thread that does a sync for it
//			new Thread(futureTask).start();
//		}

		return accountToFuture;
	}

	// TODO --- Will call an addListener method in each adapter
	@Override
	public void addListener(IStorageListener listener)
	{
		newListenerManager.addListener(listener);
	}

	@Override
	public Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds) throws MessagingException
	{
		return A(accountId).deleteMessages(folderId, messageIds);
	}

	@Override
	public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId,
										Collection<String> messageIds, String targetFolderId, boolean delete)
	{
		return A(accountId).moveMessagesToJunk(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
								  String targetFolderId, boolean delete) throws MessagingException
	{
		return A(accountId).copyMessages(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds,
											boolean flag)
	{
		return A(accountId).markMessagesAsAnswered(messageIds, folderId, flag);
	}

	@Override
	public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds,
										boolean flag)
	{
		return A(accountId).markMessagesAsJunk(messageIds, folderId, flag);
	}

	@Override
	public Future<?> send(Model model) throws StorageException
	{
		try
		{
			return adapters.get(getAccountId(model)).send(model);
		}
		catch (Exception ex)
		{
			throw new StorageException(ex);
		}
	}

	@Override
	public void send(MessageModel model) throws StorageException
	{
		try
		{
			Message message = adapters.get(model.getAccountId()).createMessage(model);
			Transport.send(message);
			if (model.getCopyToId() != null)
			{
				adapters.get(model.getAccountId()).appendMessages(model.getCopyToId(), new Message[]{message});
			}
		}
		catch (MessagingException | IOException ex)
		{
			throw new StorageException(ex);
		}
		//		try
		//		{
		//			Message message = createMessage(accountId, messageId, false);
		//			Transport.send(message);
		//			if (sendId != null)
		//			{
		//				appendMessages(new Message[]{message}, sendId);
		//			}
		//
		//			WriteContext contextw = getWriteContext();
		//			contextw.startTransaction();
		//			try
		//			{
		//				//  TODO -- this is wrong, since I don't think unstore unstores everything it needs to
		//				DStore.unstoreDraft(contextw.getModel(), messageId);
		//				contextw.commit();
		//			} finally
		//			{
		//				contextw.endTransaction();
		//			}
		//		} catch (MessagingException ex)
		//		{
		//			throw new StorageException(ex);
		//		}
	}

	//	creates a model for the accounts from the incoming model by adding sub-class information

	private Model createAccountsModel(Model model)
	{
		Model dModel = ModelFactory.createRDFSModel(model);
		JenaUtils.addSubClasses(dModel, Vocabulary.accountSubClasses);
		return dModel;
	}

	//	extract the account id from a model
	private Resource getAccountId(Model model)
	{
		ResultSet result = QueryExecutionFactory.create(getAccountType, createAccountsModel(model)).execSelect();
		if (!result.hasNext())
		{
			// throw some kind of error because we couldn't find the account
		}

		return result.next().getResource("id");
	}
}

//	//@Override
//	public String getDraftMessageId(String accountId)
//	{
//		return null;//m_account.get(accountId).getSendId();
//	}
//
//	//@Override
//	public Future<?> appendMessages(String accountId, String folderId, Collection<String> messageIds) //throws StorageException
//	{
//		return null; //getAccount(accountId).appendMessages(folderId, org.knowtiphy.pinkpigmail.messages);
//	}

// Below two methods copy pasted from Account.java, using as testing and for reference
//	private static Resource R(Model model, String name)
//	{
//		return model.createResource(name);
//	}
//
//	private static Property P(ModelCon model, String name)
//	{
//		return model.createProperty(name);
//	}


//	//@Override
//	public void saveToDrafts(String accountId, String messageId, String draftId) //throws StorageException
//	{
////		appendMessages(new Message[]{createMessage(accountId, messageId, true)}, draftId);
////		rewatch(m_folder.get(draftId));
//	}

//TODO: Snippet of JSON testing for conversion
		/*ByteArrayOutputStream stream = new ByteArrayOutputStream();
		RDFDataMgr.write(stream, mFD, Lang.RDFJSON);

		byte[] data = stream.toByteArray();

		String str = new String(data, StandardCharsets.UTF_8);
		StringReader reader = new StringReader(str);
		System.out.println(str);

		Model modelTest = ModelFactory.createDefaultModel();
		modelTest.read(reader, null, "RDF/JSON");

		JenaUtils.printModel(modelTest, "TEST FROM JSON");*/
