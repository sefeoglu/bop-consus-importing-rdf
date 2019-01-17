package io.piveau.importing.utils;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.util.PrintUtil;
import org.apache.jena.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sim on 10.02.2017.
 */
public class JenaUtils {
    private static final Logger log = LoggerFactory.getLogger(JenaUtils.class);

    private static ModelExtract extractor = new ModelExtract(new StatementBoundaryBase() {
        @Override
        public boolean stopAt(Statement s) {
            return false;
        }
    });

    public static final String NS_HYDRA = "http://www.w3.org/ns/hydra/core#";

    public static final Map<String, String> DCATAP_PREFIXES = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("dcat", DCAT.NS);
                put("skos", SKOS.uri);
                put("foaf", FOAF.NS);
                put("dct", DCTerms.NS);
                put("gmd", "http://www.isotc211.org/2005/gmd#");
                put("v", "http://www.w3.org/2006/vcard/ns#");
                put("adms", ADMS.NS);
                put("spdx", "http://spdx.org/rdf/terms#");
                put("schema", "http://schema.org/");
                put("locn", "http://www.w3.org/ns/locn#");
                put("org", ORG.NS);
                put("time", "http://www.w3.org/2006/time#");
                put("hydra", NS_HYDRA);
            }});

    public static String extractIdentifier(Resource resource) {
        if (resource.isURIResource()) {
            return resource.getURI();
        } else {
            Statement id = resource.getProperty(DCTerms.identifier);
            return id != null ? id.getLiteral().getString() : null;
        }
    }

    public static Model extractResource(Resource resource) {
        try {
            Model d = extractor.extract(resource, resource.getModel());
            d.setNsPrefixes(JenaUtils.DCATAP_PREFIXES);
            return d;
        } catch (Exception e) {
            log.error("model extraction", e);
            return null;
        }
    }

    public static String findIdentifier(Resource resource) {
        StmtIterator it = resource.listProperties(DCTerms.identifier);
        if (it.hasNext()) {
            return it.next().getString();
        } else {
            String uri = resource.getURI();
            int idx = uri.lastIndexOf("/");
            if (idx != -1) {
                return uri.substring(idx + 1);
            } else {
                return uri;
            }
        }
    }

    public static String print(Resource resource) {
        PrintUtil.registerPrefixMap(DCATAP_PREFIXES);
        Model d = extractor.extract(resource, resource.getModel());
        return PrintUtil.print(d);
    }

    public static String prettyPrint(Model model, String outputFormat) {
        Lang format = Lang.RDFXML;
        switch (outputFormat) {
            case "application/rdf+xml":
                format = Lang.RDFXML;
                break;
            case "application/ld+json":
                format = Lang.JSONLD;
                break;
            case "text/turtle":
                format = Lang.TURTLE;
                break;
            case "text/n3":
                format = Lang.N3;
                break;
            case "application/trig":
                format = Lang.TRIG;
                break;
            case "application/n-triples":
                format = Lang.NTRIPLES;
                break;
            default:
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        RDFDataMgr.write(output, model, format);
        return output.toString();
    }

}
