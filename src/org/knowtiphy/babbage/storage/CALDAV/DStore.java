package org.knowtiphy.babbage.storage.CALDAV;

import biweekly.component.VEvent;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.time.Duration;
import java.util.GregorianCalendar;
import java.util.function.Function;
import java.util.function.Predicate;

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

	static <T> Literal L(Model model, T value, RDFDatatype rdfDatatype)
	{
		return model.createTypedLiteral(value, rdfDatatype);
	}

	static <S> void attr(Model model, Resource subject, String predicate, S value, Function<S, ? extends Literal> fn)
	{
		if (value != null)
		{
			model.add(subject, P(model, predicate), fn.apply(value));
		}
	}

	static <S> S optionalAttr(VEvent vEvent, Predicate<VEvent> predicate, Function<VEvent, S> fn)
	{
		if (predicate.test(vEvent))
		{
			return fn.apply(vEvent);
		}
		else
		{
			return null;
		}
	}

	static void event(Model model, String calendarName, String eventName, VEvent event)
	{
		Resource eventRes = R(model, eventName);
		model.add(eventRes, P(model, Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_EVENT));
		model.add(R(model, calendarName), P(model, Vocabulary.CONTAINS), eventRes);

		attr(model, eventRes, Vocabulary.HAS_SUMMARY, event.getSummary().getValue(), x -> L(model, x));

		attr(model, eventRes, Vocabulary.HAS_DATE_START,
				CALDAVAdapter.fromDate(event.getDateStart().getValue()),
				x -> L(model, new XSDDateTime(GregorianCalendar.from(x))));

		attr(model, eventRes, Vocabulary.HAS_DATE_END, event.getDateEnd() != null ?
						CALDAVAdapter.fromDate(event.getDateEnd().getValue()) :
						CALDAVAdapter.fromDate(event.getDateStart().getValue())
								.plus(Duration.parse(event.getDuration().getValue().toString())),
				x -> L(model, new XSDDateTime(GregorianCalendar.from(x))));

		attr(model, eventRes, Vocabulary.HAS_DESCRIPTION,
				optionalAttr(event, x -> x.getDescription() != null, y -> y.getDescription().getValue()),
				x -> L(model, x));

		attr(model, eventRes, Vocabulary.HAS_PRIORITY,
				optionalAttr(event, x -> x.getPriority() != null, y -> y.getPriority().getValue()), x -> L(model, x));
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreRes(Model model, String containerName, String resName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource res = R(model, resName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = model.listStatements(res, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				model.remove(model.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		model.remove(model.listStatements(res, null, (RDFNode) null));
		model.remove(model.listStatements(R(model, containerName), P(model, Vocabulary.CONTAINS), res));
	}

}