package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

/**
 * @author graham
 */
public interface IReadContext
{

	Model getModel();

	void start();

	void end();
}
