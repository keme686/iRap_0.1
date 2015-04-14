/**
 * 
 */
package de.unibonn.iai.eis.irap.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;

import de.unibonn.iai.eis.irap.helper.Global;
import de.unibonn.iai.eis.irap.helper.LoggerLocal;
import de.unibonn.iai.eis.irap.model.Changeset;
import de.unibonn.iai.eis.irap.model.Interest;
import de.unibonn.iai.eis.irap.model.Subscriber;
import de.unibonn.iai.eis.irap.sparql.SPARQLExecutor;

/**
 * @author keme686
 *
 */
public class EvaluationManager {

	private static Logger logger = LoggerLocal.getLogger(EvaluationManager.class.getName());
	private Changeset changeset;
	
	
	public EvaluationManager(Changeset changeset) {
		this.changeset  = changeset;
	}
	
	/**
	 * evaluate all interests on a changeset in parallel for each subscriber
	 * Called from: ChangesetManager
	 */
	public void start(){
		List<Subscriber> subscribers = getSubscribers(changeset);
		logger.info("Changeset: " + changeset.getUri() + " - " + changeset.getSequenceNum());
		for(Subscriber s: subscribers){	
			logger.info("Subscriber: " + s.getName());
			logger.info("Interests: " + s.getInterests().size());
			if(!s.getInterests().isEmpty())
			//TODO: make evaluation in different threads for each subscriber.
				new InterestEvaluation(s, changeset).evaluateChangeset();
			
		}
		logger.info("---------------------------------------------");
	}
	
	public  List<Subscriber> getSubscribers(Changeset changeset){
		List<Subscriber> subscribers = new ArrayList<Subscriber>();
		Query query = getSubscribersQuery(changeset.getUri());
		
		ResultSet rs = SPARQLExecutor.executeSelect(Global.PI_SPARQL_ENDPOINT, query);
		Map<String, Subscriber> results = new HashMap<String, Subscriber>();
		if(rs == null){
			logger.error("Invalid PI SPARQL ENDPOINT! Either the endpoint is not started or invalid endpoint information was given!" );
			System.exit(1);
		}
		while(rs.hasNext()){
			QuerySolution s = rs.nextSolution();
			RDFNode subscriberRes = s.get("subscriber");	
			
			Literal subName = s.getLiteral("name");
			Subscriber subscriber;
			if(!results.containsKey(subscriberRes.toString())){
				subscriber = new Subscriber(subscriberRes.toString(), subName.toString());
				results.put(subscriberRes.toString(), subscriber);
			}else{
				subscriber = results.get(subscriberRes.toString());
			}
			
			RDFNode url = s.get("url");
			
			RDFNode interestRes = s.get("interest");
			Literal id = s.getLiteral("id");
			RDFNode pigraph = s.get("pigraph");
			
			RDFNode target = s.get("target");
			Literal iquery = s.getLiteral("query");
			
			RDFNode targetUpdateUri = s.get("targetUpdateUri");			
			
			Interest interest = new Interest(url.toString(), changeset.getUri());
			interest.setInterestId(id.toString());
			interest.setTargetUri(target.toString());
			interest.setTargetUpdateUri(targetUpdateUri.toString());
			interest.setQuery(QueryFactory.create(iquery.toString()));
			interest.setId(interestRes.toString());
			interest.setPigraph(pigraph.toString());
			subscriber.addInterest(interest);
		}
		subscribers.addAll(results.values());
		return subscribers;
	}
	
	private Query getSubscribersQuery(String changeseturi){
		String qstr = SPARQLExecutor.prefixes() 
				+ "  PREFIX irap: <http://eis.iai.uni-bonn.de/irap/ontology/> "
				+ " SELECT * WHERE { "
				+ "  ?interest a irap:Interest."
				+ "  ?interest irap:id ?id."
				+ "  ?interest irap:subscriber ?subscriber."
				+ "  ?interest  irap:pigraphuri  ?pigraph."
				+ "  ?interest irap:sourceuri ?url."
				+ "  ?interest irap:targetqueryuri ?target."
				+ "  ?interest irap:targetupdateuri ?targetUpdateUri. "
				+ "  ?interest irap:changesetUrl  <" + changeseturi + "> . " 
				+ "  ?interest irap:query ?query."
				+ "  ?subscriber rdfs:label ?name."
				+ " } ";
		Query query = QueryFactory.create(qstr);
		return query;
	}
}
