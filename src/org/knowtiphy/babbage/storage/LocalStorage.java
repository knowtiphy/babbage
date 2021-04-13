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
import org.knowtiphy.babbage.storage.exceptions.MalformedOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.MoreThanOneOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;
import org.knowtiphy.utils.Concurrency;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.NameSource;
import org.knowtiphy.utils.OS;

import javax.mail.MessagingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;
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
					+ " WHERE {?oid <" + RDF.type + "> ?type \n."
					+ "		?type <" + RDFS.subClassOf + "> <" + Vocabulary.OPERATION + ">\n"
					+ "		optional{ ?oid <" + Vocabulary.HAS_ACCOUNT + "> ?aid }\n"
					+ "		optional{ ?oid <" + Vocabulary.HAS_RESOURCE + "> ?rid }\n"
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

	// map from type vocabulary to a sync method
	private static final Map<String, Function<String, Future<?>>> syncs = new ConcurrentHashMap<>();

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

				futures.add(workers.submit(() -> {
					var delta = new Delta(cache);
					adapter.initialize(syncs, delta);
					return delta;
				}));
			}
		}

		//	merge the adapter triples into one big delta
		var triples = Delta.merge(cache, Concurrency.wait(futures));

		//	add subclassing triples to the message cache to support RDFS reasoning over the cache
		Vocabulary.allSubClasses.forEach((sub, sup) -> triples.bothOP(sub, RDFS.subClassOf.toString(), sup));

		//	thread which notifies listeners of changes
		//noinspection CallToThreadStartDuringObjectConstruction
		Thread doWork = new Thread(() -> {
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
		});
		doWork.setDaemon(true);
		doWork.start();

		//	add new triples to the cache
		System.out.println(triples);
		triples.apply();

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
	public Future<?> doOperation(Model operation) throws StorageException
	{
		var op = JenaUtils.createRDFSModel(operation, Vocabulary.operationsubClasses);

		//	no need to close the result set since its in memory
		var opRS = QueryExecutionFactory.create(OPERATION_QUERY, op).execSelect();

		//	the operation model didn't contain something recognizable as an operation
		if (!opRS.hasNext())
		{
			throw new NoOperationSpecifiedException();
		}

		var opSol = opRS.next();

		//	the operation contains more than operation -- may support in the future but for now it
		//	complicates the returning of a single future
		if (opRS.hasNext())
		{
			throw new MoreThanOneOperationSpecifiedException();
		}

		var aid = opSol.getResource("aid");
		var rid = opSol.getResource("rid");
		//	if both aid and rid are null, or both are non null then the operation makes no sense
		if ((aid == null && rid == null) || (aid != null && rid != null))
		{
			throw new MalformedOperationSpecifiedException();
		}

		var opType = opSol.getResource("type").toString();

		try
		{
			if (rid != null)
			{
				assert opType.equals(Vocabulary.SYNC);
				System.out.println("ITS A SYNCH " + rid);

				var typeRS = query("select * where { <" + rid + "> a ?type }");
				while (typeRS.hasNext())
				{
					var typeSol = typeRS.next();
					var foo = syncs.get(typeSol.get("type").toString());
					if (foo != null)
					{
						return foo.apply(rid.toString());
					}
				}
			}

			//	fall back to the old ops that used the accouint id
			assert aid != null;
			return A(aid.toString()).doOperation(opSol.getResource("oid").toString(), opType, op);
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

	//	creates a model for the accounts from the incoming model by adding sub-class information
	private Model createAccountsModel(Model model)
	{
		return JenaUtils.addSubClasses(ModelFactory.createRDFSModel(model), Vocabulary.accountSubClasses);
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
//@Override
//public void send(MessageModel model) throws StorageException
//{
//	try
//	{
//		Message message = adapters.get(model.getAccountId()).createMessage(model);
//		Transport.send(message);
//		if (model.getCopyToId() != null)
//		{
//			adapters.get(model.getAccountId()).appendMessages(model.getCopyToId(), new Message[]{message});
//		}
//	}
//	catch (MessagingException | IOException ex)
//	{
//		throw new StorageException(ex);
//	}
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
//}
//	//	TODO -- have to work out sync vs initialize
//	@Override
//	public Model sync(String id) throws StorageException
//	{
//		try
//		{
//			var rs = query("select * where { <" + id + "> a ?type }");
//			while (rs.hasNext())
//			{
//				var soln = rs.next();
//				var foo = syncs.get(soln.get("type").toString());
//				if (foo != null)
//				{
//					foo.apply(id);
//				}
//			}
//
//			A(id).sync();
//			return ModelFactory.createDefaultModel();
//		}
//		catch (Exception ex)
//		{
//			throw new StorageException(ex);
//		}
//	}
//
//	@Override
//	public Future<?> sync(String id, String fid) throws StorageException
//	{
//		try
//		{
//			return A(id).sync(fid);
//		}
//		catch (Exception ex)
//		{
//			throw new StorageException(ex);
//		}
//	}