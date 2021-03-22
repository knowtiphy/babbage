package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
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
import org.knowtiphy.babbage.storage.exceptions.NoAccountSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.NameSource;
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
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
	private static final String ACCOUNT_TYPE =
			"SELECT * "
					+ " WHERE {?name <" + RDF.type + "> ?type \n."
					+ "		?type <" + RDFS.subClassOf + "> <" + Vocabulary.ACCOUNT + ">\n."
					+ "		filter(?type != <" + Vocabulary.ACCOUNT + ">)"
					+ "      }";

	private static final String OPERATION_QUERY =
			"SELECT *"
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

	public static NameSource nameSource = new NameSource(Vocabulary.NBASE);

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

		//	introduce new triples into the database
		var delta = new Delta();

		//	start all the adapters

		var query = QueryFactory.create(ACCOUNT_TYPE);
		try (QueryExecution qexec = QueryExecutionFactory.create(query, accountsModel))
		{
			ResultSet result = qexec.execSelect();
			while (result.hasNext())
			{
				var soln = result.next();
				var name = soln.getResource("name").toString();
				var type = soln.getResource("type").toString();
				LOGGER.log(Level.CONFIG, "{0} {1} {2}",
						new String[]{LocalStorageSandBox.class.getName(), name, type});

				@SuppressWarnings("unchecked")
				Class<IAdapter> cls = (Class<IAdapter>) m_Class.get(type);
				IAdapter adapter = cls.getConstructor(String.class, String.class, Dataset.class,
						OldListenerManager.class, ListenerManager.class, BlockingDeque.class, Model.class)
						.newInstance(name, type, messageDatabase,
								listenerManager, newListenerManager, notificationQ, accountsModel);
				adapter.initialize(delta);

				adapters.put(adapter.getId(), adapter);
			}
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

		//	add subclassing triples to the message cache to support RDFS reasoning over the cache

		Vocabulary.allSubClasses.forEach((sub, sup) -> delta.bothOP(sub, RDFS.subClassOf.toString(), sup));

		//	add all the new triples
		BaseAdapter.apply(messageDatabase, delta);

		LOGGER.exiting(this.getClass().getCanonicalName(), "()");
	}

	@Override
	public void close()
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "close");

		Model model = ModelFactory.createDefaultModel();
		for (IAdapter adapter : adapters.values())
		{
			adapter.close(model);
		}

		//	TODO -- need to save the model
		//JenaUtils.printModel(model, "SAVE");

		notificationQ.add(POISON_PILL);
		messageDatabase.close();

		LOGGER.exiting(this.getClass().getCanonicalName(), "close");//"::()");
	}

	private IAdapter A(String accountId) throws NoSuchAccountException
	{
		IAdapter adapter = adapters.get(accountId);
		if (adapter == null)
		{
			throw new NoSuchAccountException(accountId);
		}
		return adapter;
	}

	@Override
	public Model getAccounts()
	{
		//	TODO -- do this by querying the database?
		Model accounts = JenaUtils.createRDFSModel(Vocabulary.accountSubClasses);
		adapters.values().forEach(account ->
				accounts.add(R(accounts, account.getId()), RDF.type, R(accounts, account.getType())));
		return accounts;
	}

	@Override
	public Future<?> doOperation(Model operation) throws NoSuchAccountException, NoOperationSpecifiedException
	{
		var op = JenaUtils.createRDFSModel(operation, Vocabulary.operationsubClasses);

		var rs = QueryExecutionFactory.create(OPERATION_QUERY, op).execSelect();
		//	the operation model didn't contain something recognizable as an operation
		if (!rs.hasNext())
		{
			throw new NoOperationSpecifiedException();
		}

		var sol = rs.next();
		return A(sol.getResource("aid").toString()).doOperation(
				sol.getResource("id").toString(), sol.getResource("type").toString(), op);
	}

	//	TODO -- have to work out sync vs initialize
	@Override
	public Model sync(String id) throws ExecutionException, InterruptedException, NoSuchAccountException
	{
		A(id).sync();
		return ModelFactory.createDefaultModel();
	}

	@Override
	public Future<?> sync(String id, String fid) throws ExecutionException, InterruptedException, NoSuchAccountException
	{
		return A(id).sync(fid);
	}

	@Override
	public ResultSet query(String id, String query) throws NoSuchAccountException
	{
		return A(id).query(query);
	}

	@Override
	public ReadContext getReadContext()
	{
		return new ReadContext(messageDatabase);
	}

	public Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId) throws NoSuchAccountException
	{
		return A(accountId).ensureMessageContentLoaded(folderId, messageId);
	}

	@Override
	public Future<?> loadAhead(String accountId, String folderId, Collection<String> messageIds) throws NoSuchAccountException
	{
		return A(accountId).loadAhead(folderId, messageIds);
	}

	// Will call an addListener method in each adapter
	@Override
	public Map<String, Future<?>> addOldListener(IOldStorageListener listener)
	{
		listenerManager.addListener(listener);

		for (IAdapter adapter : adapters.values())
		{
			// Add relevant triples to the model
			adapter.addListener();
		}

		// Start the synching of the IMAP org.knowtiphy.pinkpigmail.server, adds its work to the front of the Queue
		// but need to put these account of the Queue
		//		for (IAdapter adapter : adapters.values())
//		{
//			FutureTask<?> futureTask = new FutureTask<>(() ->adapter.sync());
//			accountToFuture.put(adapter.getId(), futureTask);
//			// For each account, spin up a thread that does a sync for it
//			new Thread(futureTask).start();
//		}

		return new ConcurrentHashMap<>(100);
	}

	// TODO --- Will call an addListener method in each adapter
	@Override
	public void addListener(IStorageListener listener)
	{
		newListenerManager.addListener(listener);
	}

	@Override
	public Future<?> moveMessagesToJunk(String accountId, String sourceFolderId,
										Collection<String> messageIds, String targetFolderId, boolean delete) throws NoSuchAccountException
	{
		return A(accountId).moveMessagesToJunk(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
								  String targetFolderId, boolean delete) throws MessagingException, NoSuchAccountException
	{
		return A(accountId).copyMessages(sourceFolderId, messageIds, targetFolderId, delete);
	}

	@Override
	public Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds,
											boolean flag) throws NoSuchAccountException
	{
		return A(accountId).markMessagesAsAnswered(messageIds, folderId, flag);
	}

	@Override
	public Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds,
										boolean flag) throws NoSuchAccountException
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
	private String getAccountId(Model model) throws NoAccountSpecifiedException
	{
		ResultSet result = QueryExecutionFactory.create(ACCOUNT_TYPE, createAccountsModel(model)).execSelect();
		if (!result.hasNext())
		{
			throw new NoAccountSpecifiedException();
		}

		return result.next().getResource("id").toString();
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
