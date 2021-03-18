package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.utils.JenaUtils;

public class EventSetBuilder
{
	public Model model = ModelFactory.createDefaultModel();
	private long ids;

	public EventSetBuilder()
	{
		JenaUtils.addSubClasses(model, Vocabulary.eventSubClasses);
	}

	public String newEvent(String type)
	{
		var eid = Vocabulary.E(type, ids + "");
		ids++;
		JenaUtils.addOP(model, eid, RDF.type.toString(), type);
		return eid;
	}

	public EventSetBuilder addOP(String eid, String p, String o)
	{
		JenaUtils.addOP(model, eid, p, o);
		return this;
	}

	public <T> EventSetBuilder addDP(String eid, String p, T o)
	{
		JenaUtils.addDP(model, eid, p, o);
		return this;
	}

	@Override
	public String toString()
	{
		return "EventSetBuilder{" + "model=" + model + "}";
	}
}