package io.piveau.importing.utils;

import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;

public class Hydra {

    private static final Model m = ModelFactory.createDefaultModel();

    private static final Resource PAGED_COLLECTION;
    private static final Resource PARTIAL_COLLECTION_VIEW;

    private static final Property TOTAL_ITEMS;


    private static final Property NEXT;
    private static final Property NEXT_PAGE;

    static {
        PARTIAL_COLLECTION_VIEW = m.createResource(JenaUtils.NS_HYDRA + "PartialCollectionView");
        PAGED_COLLECTION = m.createResource(JenaUtils.NS_HYDRA + "PagedCollection");

        TOTAL_ITEMS = m.createProperty(JenaUtils.NS_HYDRA + "totalItems");
        NEXT = m.createProperty(JenaUtils.NS_HYDRA + "next");
        NEXT_PAGE = m.createProperty(JenaUtils.NS_HYDRA + "nextPage");
    }

    private Resource paging;
    private Property next;

    private int total;

    private Hydra(Resource paging, Property next, int total) {
        this.paging = paging;
        this.next = next;
        this.total = total;
    }

    public static Hydra findPaging(Model model) {
        ResIterator it = model.listResourcesWithProperty(RDF.type, PAGED_COLLECTION);
        Property next = NEXT_PAGE;
        int total = 0;
        Resource hydra = null;
        if (!it.hasNext()) {
            next = NEXT;
            it = model.listResourcesWithProperty(RDF.type, PARTIAL_COLLECTION_VIEW);
            if (it.hasNext()) {
                total = model.listObjectsOfProperty(TOTAL_ITEMS).next().asLiteral().getInt();
                hydra = it.nextResource();
            }
        } else {
            hydra = it.nextResource();
            total = hydra.getProperty(TOTAL_ITEMS).getInt();
        }
        return hydra != null ? new Hydra(hydra, next, total) : null;
    }

    public int total() {
        return total;
    }

    public String next() {
        if (next == NEXT) {
            return paging.hasProperty(next) ? paging.getProperty(next).getResource().getURI() : null;
        } else {
            return paging.hasProperty(next) ? paging.getProperty(next).getString() : null;
        }
    }

}
