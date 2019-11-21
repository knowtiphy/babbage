package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.component.VEvent;
import org.apache.commons.io.IOUtils;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.*;
import org.knowtiphy.babbage.storage.IAdapter;
import org.knowtiphy.babbage.storage.IMAP.IMAPAdapter;
import org.knowtiphy.babbage.storage.IMAP.MessageContent;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.*;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author graham
 */
public interface DStore
{
	static Resource R(Model model, String name)
	{
		return model.createResource(name);
	}

	static Property P(ModelCon model, String name)
	{
		return model.createProperty(name);
	}

	static <T> Literal L(ModelCon model, T value)
	{
		return model.createTypedLiteral(value);
	}

	static <S> void attr(Model model, Resource subject, String predicate, S value, Function<S, ? extends Literal> fn)
	{
		if (value != null)
		{
			model.add(subject, P(model, predicate), fn.apply(value));
		}
	}

	static void event(Model model, String calendarName, String eventName, VEvent event)
	{
		Resource eventRes = R(model, eventName);
		model.add(eventRes, P(model, Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_EVENT));
		model.add(R(model, calendarName), P(model, Vocabulary.CONTAINS), eventRes);

		attr(model, eventRes, Vocabulary.HAS_SUMMARY, event.getSummary(), x -> L(model, x));
		attr(model, eventRes, Vocabulary.HAS_DATE_START, event.getDateStart(), x -> L(model, new XSDDateTime(JenaUtils.fromDate(x))));
		attr(model, eventRes, Vocabulary.HAS_DATE_END, event.getDateEnd(), x -> L(model, new XSDDateTime(JenaUtils.fromDate(x))));
		attr(model, eventRes, Vocabulary.HAS_DESCRIPTION, event.getDescription(), x -> L(model, x));
		attr(model, eventRes, Vocabulary.HAS_PRIORITY, event.getPriority(), x -> L(model, x));
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreMessage(Model model, String folderName, String messageName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource messageRes = R(model, messageName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = model.listStatements(messageRes, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				model.remove(model.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		model.remove(model.listStatements(messageRes, null, (RDFNode) null));
		model.remove(model.listStatements(R(model, folderName), P(model, Vocabulary.CONTAINS), messageRes));
	}

}