package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
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
import org.knowtiphy.utils.Concurrency;
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
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A local storage layer for storing mail messages in a local RDF database (so in the file system of
 * the machine).
 *
 * @author graham
 */
public class LocalStorage implements IStorage
{
	private static final Logger LOGGER = Logger.getLogger(LocalStorage.class.getName());

	private static final String CACHE = "cache";
	private static final String ACCOUNTS_FILE = "accounts.ttl";

	private static final Runnable POISON_PILL = () -> {
	};

	private final ExecutorService workers = Executors.newFixedThreadPool(4);

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

	// map from account type vocabulary to class for the relevant adapter
	private static final Map<String, Class<?>> adapterClasses = new HashMap<>();

	static
	{
		adapterClasses.put(Vocabulary.IMAP_ACCOUNT, IMAPAdapter.class);
		adapterClasses.put(Vocabulary.CALDAV_ACCOUNT, CALDAVAdapter.class);
		adapterClasses.put(Vocabulary.CARDDAV_ACCOUNT, CARDDAVAdapter.class);
	}

	public static NameSource nameSource = new NameSource(Vocabulary.NBASE);

	private final Dataset cache;

	private final ListenerManager listenerManager = new ListenerManager();
	private final Map<String, IAdapter> adapters = new HashMap<>(100);

	private final BlockingDeque<Runnable> notificationQ = new LinkedBlockingDeque<>();

	public LocalStorage() throws Exception
	{
		LOGGER.entering(this.getClass().getCanonicalName(), "()");

		//	data structures shared between accounts

		Path databaseLocation = Paths.get(OS.getDataDir(Babbage.class).toString(), CACHE);
		Files.createDirectories(databaseLocation);

		cache = TDB2Factory.connectDataset(databaseLocation.toString());

		//  read accounts -- TODO: this information should all be in a database not a file

		Path accountsDir = Paths.get(OS.getSettingsDir(Babbage.class).toString());
		Files.createDirectories(accountsDir);

		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, Files.newInputStream(Paths.get(OS.getSettingsDir(Babbage.class).toString(), ACCOUNTS_FILE)), Lang.TURTLE);
		Model accountsModel = createAccountsModel(model);

		//	start all the adapters in parallel and remember any deltas they have

		var futures = new LinkedList<Future<Delta>>();

		try (QueryExecution qexec = QueryExecutionFactory.create(ACCOUNT_TYPE, accountsModel))
		{
			ResultSet result = qexec.execSelect();


			while (result.hasNext())
			{
				var soln = result.next();
				var name = soln.getResource("name").toString();
				var type = soln.getResource("type").toString();
				LOGGER.log(Level.CONFIG, "{0} {1} {2}",
						new String[]{LocalStorage.class.getName(), name, type});

				@SuppressWarnings("unchecked")
				Class<IAdapter> cls = (Class<IAdapter>) adapterClasses.get(type);
				IAdapter adapter = cls.getConstructor(String.class, String.class,
						Dataset.class, ListenerManager.class, BlockingDeque.class, Model.class)
						.newInstance(name, type, cache, listenerManager, notificationQ, accountsModel);
				adapters.put(adapter.getId(), adapter);

				futures.add(workers.submit((Callable<Delta>) () -> {
					var delta = new Delta();
					adapter.initialize(delta);
					return delta;
				}));
			}
		}

		//	merge the adapter triples into one big delta
		var triples = Delta.merge(Concurrency.wait(futures));

		//	add subclassing triples to the message cache to support RDFS reasoning over the cache
		Vocabulary.allSubClasses.forEach((sub, sup) -> triples.bothOP(sub, RDFS.subClassOf.toString(), sup));

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

		//	add new triples to the cache
		System.out.println(triples);
		BaseAdapter.apply(cache, triples);

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
		cache.close();

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
	public Model sync(String id) throws StorageException
	{
		try
		{
			A(id).sync();
			return ModelFactory.createDefaultModel();
		}
		catch (Exception ex)
		{
			throw new StorageException(ex);
		}
	}

	@Override
	public Future<?> sync(String id, String fid) throws StorageException
	{
		try
		{
			return A(id).sync(fid);
		}
		catch (Exception ex)
		{
			throw new StorageException(ex);
		}
	}

	@Override
	public ResultSet query(String query) throws StorageException
	{
		return BaseAdapter.query(cache, query);
	}

	@Override
	public ReadContext getReadContext()
	{
		return new ReadContext(cache);
	}

	@Override
	public void addListener(IStorageListener listener)
	{
		listenerManager.addListener(listener);
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
		return JenaUtils.addSubClasses(ModelFactory.createRDFSModel(model), Vocabulary.accountSubClasses);
	}

	//	extract the account id from a model
	private String getAccountId(Model model) throws NoAccountSpecifiedException
	{
		try (QueryExecution qexec = QueryExecutionFactory.create(ACCOUNT_TYPE, createAccountsModel(model)))
		{
			ResultSet result = qexec.execSelect();
			if (!result.hasNext())
			{
				throw new NoAccountSpecifiedException();
			}

			return result.next().getResource("id").toString();
		}
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
