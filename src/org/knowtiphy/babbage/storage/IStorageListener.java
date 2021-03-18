package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

public interface IStorageListener
{
	void handleEvent(Model model);
}
