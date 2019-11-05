package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

/**
 *
 * @author graham
 */
@FunctionalInterface
public interface IStorageListener
{
    void delta(Model added, Model removed);
}
