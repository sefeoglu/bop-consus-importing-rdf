package io.piveau.importing.utils;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

/**
 * Created by sim on 15.06.2017.
 */
public class ADMS {
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "http://www.w3.org/ns/adms#";
    public static final Resource NAMESPACE;

    public static final Property status;

    public ADMS() {
    }

    public static String getURI() {
        return NS;
    }

    static {
        NAMESPACE = m.createResource(NS);
        status = m.createProperty(NS, "status");
    }
}
