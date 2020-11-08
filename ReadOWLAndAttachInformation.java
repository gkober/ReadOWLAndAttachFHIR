package com.spirit.DMRE.Utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;


import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleBNode;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.*;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;

import com.jayway.jsonpath.JsonPath;
import com.spirit.DMRE.SparqlJsonClient;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.ReferenceClientParam;
import ca.uhn.fhir.rest.gclient.TokenClientParam;
import ca.uhn.fhir.util.BundleUtil;
/**
 * 
 * @author gerhard
 *
 */
public class ReadOWLAndAttachInformation {
	FhirContext ctx = FhirContext.forR4();
	IGenericClient client;
	HashMap <String, List<String>> resourcesMap= new HashMap<String,List<String>>();
	ModelBuilder builder = new ModelBuilder();
	ValueFactory factory = SimpleValueFactory.getInstance();
	private static Logger logger = Logger.getLogger("com.spirit.DMRE.ReadOWLAndAttachInformation");
	
	public ReadOWLAndAttachInformation() {
		
	}
	/**
	 * 
	 * @param streamForParsing
	 * @param patientID
	 * @throws RDFParseException
	 * @throws UnsupportedRDFormatException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void parseOWLforIRIs(InputStream streamForParsing, String patientID) throws RDFParseException, UnsupportedRDFormatException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
				"parseOWLforIRIs" + " "
				);
		HashMap<String,List<String>> classProperties = new HashMap<String,List<String>>();
		List<String> propertiesList = null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		int n = 0;
		while ((n = streamForParsing.read(buf)) >= 0)
		    baos.write(buf, 0, n);
		byte[] content = baos.toByteArray();
		//doubling the inpuStream for the model and for the submission to the json-query-endpoint
		InputStream is1 = new ByteArrayInputStream(content);
		InputStream is2 = new ByteArrayInputStream(content);

		InputStreamReader isReader = new InputStreamReader(is2);
		BufferedReader reader = new BufferedReader(isReader);
		StringBuffer sb = new StringBuffer();
	    String str;
	    while((str = reader.readLine())!= null){
	         sb.append(str);
	      }
		//Model model = Rio.parse(streamForParsing,"",RDFFormat.RDFXML);
		Model model = Rio.parse(is1,"",RDFFormat.RDFXML);
		for(Statement statement:model) {
			if(!(statement.getSubject() instanceof SimpleBNode)) {
			IRI subject = (IRI)statement.getSubject();
			IRI predicate = statement.getPredicate();
			Value object = statement.getObject();
			logger.fine("===checking for the predicates ===" + str);
			logger.finer(statement.toString());
			logger.fine("Subject: " + subject.getLocalName());
				if(object.equals(OWL.CLASS)) {
					String sparqlQuery = this.getSPARQLQueryForProperties("<"+subject.toString()+">");
					System.out.println(sparqlQuery);
					//askSparqlQuery by using a Client to a http-endpoint
					SparqlJsonClient sjc = new SparqlJsonClient();
					propertiesList = sjc.generateRequest(sparqlQuery, sb.toString());
					classProperties.put(subject.getLocalName(), propertiesList);
				}
			}
		}
		
		//used for asking the FHIR-Store for the classes.
		this.client = ctx.newRestfulGenericClient("http://hapi.fhir.org/baseR4");
		
		Bundle bundle = null;
		List<IBaseResource> patients = new ArrayList<>();
		List<IBaseResource> otherResources = new ArrayList<>();
		List<String> jsonPathesObservation = new ArrayList<String>();
		List<String> jsonPathesPatient = new ArrayList<String>();
		List<String> jsonPathesMedication = new ArrayList<String>();
		for(String a : classProperties.keySet()) {
			if(a.equalsIgnoreCase("Observation")) {
				for (String s : classProperties.get(a)) {
					jsonPathesObservation.add("$."+this.cutPrefix("http://dmre/", s));
				}
			}
			if(a.equalsIgnoreCase("Patient")) {
				for (String s : classProperties.get(a)) {
					jsonPathesPatient.add("$."+this.cutPrefix("http://dmre/", s));
				}
			}
			if(a.equalsIgnoreCase("Medication")) {
				for (String s : classProperties.get(a)) {
					jsonPathesMedication.add("$."+this.cutPrefix("http://dmre/", s));
				}
			}
		}
		for(String fhirObject : classProperties.keySet()) {
			for(String propertyOfClass : classProperties.get(fhirObject)) {
				String propToWhere = this.cutPrefix("http://dmre/", propertyOfClass.toLowerCase());
				if(fhirObject.equalsIgnoreCase("patient")) {
					try {
						bundle = (Bundle) client.search()
										.forResource(fhirObject)
										.where(new TokenClientParam("_id").exactly().code(patientID))
										.returnBundle(Bundle.class)
										.execute();
						patients.addAll(BundleUtil.toListOfResources(ctx, bundle));
						getValueFromBundleAndPutToMap(bundle,jsonPathesPatient);
						while(bundle.getLink(IBaseBundle.LINK_NEXT) != null) {
							bundle = client.loadPage().next(bundle).execute();
							patients.addAll(BundleUtil.toListOfResources(ctx, bundle));
							getValueFromBundleAndPutToMap(bundle,jsonPathesPatient);
						}
					}catch(Exception e) {
						logger.fine("Exception happened, but proceeding");
					}
				}else {
					try {
						bundle = (Bundle) client.search()
										.forResource(fhirObject)
										.where(new ReferenceClientParam("patient").hasId(patientID))
										.returnBundle(Bundle.class)
										.execute();
						getValueFromBundleAndPutToMap(bundle,jsonPathesObservation);
						otherResources.addAll(BundleUtil.toListOfResources(ctx, bundle));
						while(bundle.getLink(IBaseBundle.LINK_NEXT) != null) {
							bundle = client.loadPage().next(bundle).execute();
							getValueFromBundleAndPutToMap(bundle,jsonPathesObservation);
						}
					}catch(Exception e) {
						logger.fine("Exception happened, but proceeding");
					}
				}
				
			}
		}
		this.putFHIRBundleInformationToOWL(resourcesMap, classProperties, model);
	}
	/**
	 * 
	 * @param resourcesMap2
	 * @param classProperties
	 * @param model
	 */
	public void putFHIRBundleInformationToOWL(HashMap<String, List<String>> resourcesMap2, HashMap<String,List<String>> classProperties, Model model) {
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
							"putFHIRBundleInformationToOWL" + " "
							);
		for(String a : classProperties.keySet()) {
			logger.fine(a);
			for (String s : classProperties.get(a)) {
				logger.fine("- " +s);
				addInfoToModel(a, s, model);
			}
		}
		Rio.write(model,System.out,RDFFormat.RDFXML);
	}
	/**
	 * 
	 * @param topClass
	 * @param property
	 * @param model
	 * @return
	 */
	private Model addInfoToModel(String topClass, String property, Model model) {
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
				"addInfoToModel" + " "
				);
		property = this.cutPrefix("http://dmre/",property);
		for(Entry<String, List<String>> ID : resourcesMap.entrySet()) {
			if(ID.getKey().contains(topClass)) {
			//System.out.println(ID.getKey() + " " + ID.getValue());
				for(String savedProperty : ID.getValue()) {
					if(savedProperty.contains(property)) {
						//tidy
						if(savedProperty != null && property != null && savedProperty.startsWith(property)) {
							savedProperty =  savedProperty.substring(property.length());
						}
						//getting the property
						savedProperty = savedProperty.substring(savedProperty.indexOf(":")+1);
						savedProperty.trim();
						//replacing brackets
						savedProperty = savedProperty.replaceAll("\\[", "(").replaceAll("\\]", ")");
						property = property.replaceAll("\\[", "(").replaceAll("\\]", ")");
						//creating the needed IRIs and Literals for the graph
						IRI entity = factory.createIRI(ID.getKey());
						IRI topClassIRI = factory.createIRI("http://dmre/" + topClass);
						IRI propConnectionToOwnIdentity = factory.createIRI("http://dmre/"+property);
						Literal propToAdd = factory.createLiteral(savedProperty);
						model.add(entity,RDF.TYPE,topClassIRI);
						model.add(entity,propConnectionToOwnIdentity,propToAdd);
					}				
				}
			}
		}
		return model;	
	}
	
	/**
	 * 
	 * @param allEntries
	 * @param resourceType equals to fhirObject in the code -> resourceType is the FHIR-wording
	 * @param property
	 * @throws Exception 
	 */
	public void getValueFromBundleAndPutToMap(Bundle bundle, List<String> jsonpath) throws Exception{
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
				"getValueFromBundleAndPutToMap" + " "
				);	
		for (int i = 0; i< bundle.getEntry().size();i++) {
			List<String> allProperties = new ArrayList<String>();
			BundleEntryComponent bt = bundle.getEntry().get(i);
			for(String singlejsonPath : jsonpath) {
				String value = JsonPath.read(ctx.newJsonParser()
					.encodeResourceToString(bt.getResource())
					, singlejsonPath).toString();
				allProperties.add(singlejsonPath+":"+value);
			}
			resourcesMap.put(bt.getResource().getId(), new ArrayList<>(allProperties));
		}		
	}
	/**
	 * 
	 * @param allEntries
	 * @param resourceType
	 * @param property
	 */
	public void getFhirValueFromObjectProperty(List<Bundle> allEntries, String resourceType, String property) {
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
				"getFhirValueFromObjectProperty" + " "
				);
//		System.out.println("getFhirValueFromObjectProperty");
//		System.out.println("allEntriesSize: " + allEntries.size());
//		System.out.println("RESOURCEType: " + resourceType);
//		System.out.println("PROPERTY: " + property);
		for(Bundle singleBundle : allEntries) {
			for(BundleEntryComponent bundleEntry : singleBundle.getEntry()) {
				if((bundleEntry.getResource().getResourceType().toString().equalsIgnoreCase(resourceType))) {
					//System.out.println("Entry having ResourceType " + resourceType );
				}else {
					System.out.println("something strange happened");
					System.out.println(bundleEntry.getResource().getResourceType().toString());
					System.out.println(resourceType);
					System.out.println("--------");
				}
			}
		}
	}
	/**
	 * 
	 * @param owlClassName
	 * @return
	 */
	public String getSPARQLQueryForProperties(String owlClassName) {
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
				"getSPARQLQueryForProperties" + " "
				);
		String sparql = 
				"prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"prefix owl: <http://www.w3.org/2002/07/owl#>\n" + 
				"prefix xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
				"prefix dmre: <http://dmre>\n" + 
				"select ?class ?property where { \n" + 
				"   ?property rdfs:domain "+owlClassName+" .\n" + 
				"   ?property a owl:DatatypeProperty .\n" + 
				"  }";
		return sparql;
	}
	/**
	 * 
	 * @param prefix
	 * @param toCut
	 * @return
	 */
	public String cutPrefix(String prefix, String toCut) {
		System.out.println("==> " + this.getClass().getCanonicalName() + " " +
				"cutPrefix" + " "
				);
		if(toCut != null && prefix != null && toCut.startsWith(prefix)) {
			toCut =  toCut.substring(prefix.length());
		}
		//replace () by [] - >this is needed because rdf4j complains about []
		toCut = toCut.replaceAll("\\(", "[").replaceAll("\\)", "]");	
		return toCut;
	}
	/**
	 * 
	 * @param args
	 * @throws RDFParseException
	 * @throws UnsupportedRDFormatException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws RDFParseException, UnsupportedRDFormatException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		ReadOWLAndAttachInformation a = new ReadOWLAndAttachInformation();
		//a.ReadOWL();
		
		
		String data = "/Users/gerhard/tryOntology.owl";
		File initialFile = new File(data);
	    InputStream targetStream = new FileInputStream(initialFile);
	    System.out.println("Testcase 1 - lot's of entries");
	    a.parseOWLforIRIs(targetStream,"5a5f527f-ff2f-4486-94f3-8c3494356137");
	    System.out.println("Testcase 2 - one patient, one observation");
	    //a.parseOWLforIRIs(targetStream,"1567076");
	    
	    System.out.println("\nExiting");
	   
	
	}
	public void devHelper() {
		// Create a context and a client
		FhirContext ctx = FhirContext.forR4();
		String serverBase = "http://hapi.fhir.org/baseR4";
		IGenericClient client = ctx.newRestfulGenericClient(serverBase);
		

		// We'll populate this list
		List<IBaseResource> patients = new ArrayList<>();

		// We'll do a search for all Patients and extract the first page
		Bundle bundle = client
		   .search()
		   .forResource(Patient.class)
		   .where(Patient.NAME.matches().value("orlitsch"))
		   .returnBundle(Bundle.class)
		   .execute();
		System.out.println("addint patient to ListOfResources");
		patients.addAll(BundleUtil.toListOfResources(ctx, bundle));

		// Load the subsequent pages
		while (bundle.getLink(IBaseBundle.LINK_NEXT) != null) {
		   bundle = client
		      .loadPage()
		      .next(bundle)
		      .execute();
		   System.out.println("addint patient to ListOfResources-additional");
		   patients.addAll(BundleUtil.toListOfResources(ctx, bundle));
		}

		System.out.println("Loaded " + patients.size() + " patients!");
		for(IBaseResource p : patients) {
			System.out.println(p.getClass());
			if(p instanceof Patient) {
				Patient p1 = (Patient) p;
				System.out.println(ctx.newJsonParser().encodeResourceToString(p1));
				System.out.println("i am instance of patient");
			}
		}
		System.out.println(ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString(patients.get(0)));
		Patient p = (Patient) patients.get(0);
		String prop = p.getNameFirstRep().getFamily();
		System.out.println(prop);
		
		//select patientID from patient-resources and get Observations
		p.getIdentifierFirstRep().getValueElement().getValue();
		String patientID = p.getIdElement().getIdPart();
		System.out.println("The id is: " + p.getId());
		System.out.println("The id is: " + p.getIdElement().getIdPart());
		
		
		//Query for Observations
		Bundle observations = (Bundle) client.search()
								.forResource(Observation.class)
								.where(new ReferenceClientParam("subject").hasId(patientID))
								.returnBundle(Bundle.class)
								.execute();
		for(BundleEntryComponent e : observations.getEntry()) {
			Observation obs = (Observation) e.getResource();
			//use JSONPath for getting the properties
			System.out.println(ctx.newJsonParser().encodeResourceToString(obs));
			String JsonObs = ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString((IBaseResource) e);
			//System.out.println(JsonObs);
			String name = JsonPath.read(JsonObs, "$.subject.reference");
			String valueQ = JsonPath.read(JsonObs, "$.valueQuantity.value").toString();
			System.out.println("JSONValueQ is: " + valueQ);
			System.out.println("JSONPAthResult is : " +name);
		}
		
			
	}

}

