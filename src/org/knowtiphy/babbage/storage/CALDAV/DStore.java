package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.component.VEvent;
import com.github.sardine.DavResource;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.time.Duration;
import java.util.GregorianCalendar;
import java.util.function.Function;

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

	static <S, T> void addAttribute(Delta delta, String subject, String predicate, S value, Function<S, T> fn)
	{
		if (value != null)
		{
			delta.addL(subject, predicate, fn.apply(value));
		}
	}


	static void storeCalendar(Delta delta, String adapterID, String encodedCalendar, DavResource calendar)
	{
		delta.addR(encodedCalendar, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_CALENDAR)
				.addR(adapterID, Vocabulary.CONTAINS, encodedCalendar);

		addAttribute(delta, encodedCalendar, Vocabulary.HAS_NAME, calendar.getDisplayName(), x -> x);
		addAttribute(delta, encodedCalendar, Vocabulary.HAS_CTAG, calendar.getCustomProps().get("getctag"), x -> x);
	}

	static void storeEvent(Delta delta, String calendarId, String eventId, VEvent vEvent, DavResource event)
	{
		delta.addR(eventId, Vocabulary.RDF_TYPE, Vocabulary.CALDAV_EVENT)
				.addR(calendarId, Vocabulary.CONTAINS, eventId);

		addAttribute(delta, eventId, Vocabulary.HAS_ETAG, event.getEtag(), x -> x);

		addAttribute(delta, eventId, Vocabulary.HAS_SUMMARY, vEvent.getSummary().getValue(), x -> x);

		addAttribute(delta, eventId, Vocabulary.HAS_DATE_START,
				CALDAVAdapter.fromDate(vEvent.getDateStart().getValue()),
				x -> new XSDDateTime(GregorianCalendar.from(x)));

		addAttribute(delta, eventId, Vocabulary.HAS_DATE_END, vEvent.getDateEnd() != null ?
						CALDAVAdapter.fromDate(vEvent.getDateEnd().getValue()) :
						CALDAVAdapter.fromDate(vEvent.getDateStart().getValue())
								.plus(Duration.parse(vEvent.getDuration().getValue().toString())),
				x -> new XSDDateTime(GregorianCalendar.from(x)));

		addAttribute(delta, eventId, Vocabulary.HAS_DESCRIPTION,
				vEvent.getDescription() != null ? vEvent.getDescription().getValue() : null, x -> x);

		addAttribute(delta, eventId, Vocabulary.HAS_PRIORITY,
				vEvent.getPriority() != null ? vEvent.getPriority().getValue() : null, x -> x);

	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreRes(Model messageDB, Delta delta, String containerName, String resName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource res = R(messageDB, resName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = messageDB.listStatements(res, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(messageDB.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(messageDB.listStatements(res, null, (RDFNode) null));
		delta.delete(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.CONTAINS), res));
	}

}