package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

/**
 *
 * @author graham
 */
@FunctionalInterface
public interface IOldStorageListener
{
    void delta(Model added, Model removed);
}
