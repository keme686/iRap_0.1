/**
 * 
 */
package de.unibonn.iai.eis.irap.evaluator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.tdb.TDBFactory;

import de.unibonn.iai.eis.irap.changeset.ChangesetManager;
import de.unibonn.iai.eis.irap.helper.Global;
import de.unibonn.iai.eis.irap.helper.LoggerLocal;
import de.unibonn.iai.eis.irap.model.Changeset;
import de.unibonn.iai.eis.irap.model.Interest;
import de.unibonn.iai.eis.irap.model.Subscriber;
import de.unibonn.iai.eis.irap.sparql.QueryPatternExtractor;
import de.unibonn.iai.eis.irap.sparql.QueryDecomposer;
import de.unibonn.iai.eis.irap.sparql.SPARQLExecutor;

/**
 * @author keme686
 *
 */
public class EvaluationThread{

	private static Logger logger = LoggerLocal.getLogger(EvaluationThread.class.getName());
			
	private Subscriber subscriber;
	private Changeset changeset;

	public EvaluationThread(Subscriber subscriber, Changeset changeset) {
		this.subscriber = subscriber;
		this.changeset = changeset;
	}

	/**
	 * evaluate each interests of a subscriber sequentially on a changeset
	 */
	
	public void evaluateChangeset() {
		List<Interest> interests = subscriber.getInterests();
		for (Interest i : interests) {
			evaluate(i, changeset);
		}
	}

	/**
	 * evaluate an interest over a changeset in two steps:
	 * <ol>
	 * <li>evaluate interest on removed triples</li>
	 * <li>evaluate interest on added triples</li>
	 * <ol>
	 * since SPARQL update operation works this way, it should be kept same step
	 * as above: delete then add (rename)
	 * 
	 * @param interest
	 *            the interest expression by a subscriber which contains source
	 *            Vs target datasets, changeset url, local PI graph name, etc
	 * @param changeset
	 *            an update that contains only removed and added triples since
	 *            the last update
	 */
	public void evaluate(final Interest interest, Changeset changeset) {
	
		//evaluate deletion on target dataset
		 evaluateRemoved(interest, changeset);
		//evaluate addition
		 evaluateAdded(interest, changeset);
	}

	/**
	 *  
	 * @param interest
	 * @param changeset
	 * @return
	 */
	public void evaluateRemoved(final Interest interest, Changeset changeset) {
		//first evaluate the removed triples on Potentially interesting triples of an interest
		
		// remove triples from potentially interesting graph of this interest, if they exists		 
		if(evaluateRemovedForPI(changeset.getRemovedTriples(), interest)){
			logger.info("Potentially interesting deleted triples has been removed from local dataset!");
		}else{
			logger.debug("Cannot remove potentially interesting deleted triples from local dataset!");
		}
		// Evaluate interest expression directly on target endpoint of a subscriber
		List<Model>  removeResult = evaluateOnRemovedOfTarget( changeset.getRemovedTriples(), interest);
		
		if(updatePI(removeResult.get(1), interest.getPigraph(), true)){
			logger.info("Potenatilly interesting triples removed from target dataset!");
		}else{
			logger.debug("Cannot store potenatilly interesting triples extracted from target dataset!");
		}
		// propagate interesting triples to interest subscribers' target update uri (endpoint - in this case)
		
		StringBuffer removeTargetQuery = QueryDecomposer.toUpdate(removeResult.get(0), false);
		if( SPARQLExecutor.executeUpdate(interest.getTargetUpdateUri(), removeTargetQuery.toString())){
			logger.info("Interesting removed triples propagated to target dataset: " + interest.getTargetUpdateUri());
		}else{
			logger.debug("Cannot propagate interesting removed triples to target dataset:" + interest.getTargetUpdateUri());
		}
		
	}
	/**
	 * store potentially interesting triples to local triple store
	 * 
	 * @param model
	 * @param graph
	 * @param isAdd
	 * @return
	 */
	private boolean updatePI(Model model, String graph, boolean isAdd){
		StringBuffer insertPIQuery = QueryDecomposer.toUpdate(model, graph, isAdd);
		return SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, insertPIQuery.toString());
	}
	
	/**
	 * triple based delete operation over the potentially interesting triples of
	 * a specific interest expression
	 * 
	 * @param model
	 * @param interest
	 * @return
	 */
	public boolean  evaluateRemovedForPI(final Model removedModel, final Interest interest) {

		StringBuffer removePIQuery = QueryDecomposer.toUpdate(removedModel, interest.getPigraph(), false);
		boolean removingPI = SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, removePIQuery.toString());		
		return removingPI;
	}

	
	private List<Model> evaluateOnRemovedOfTarget(Model removedModel, final Interest interest) {
		List<TriplePath> paths = interest.getTriplePaths();
		// List<TriplePath> optpaths = interest.getOptionalTriplePaths();
		// List<Model> result = new ArrayList<Model>();
		// TODO: include OGP - optional graph patterns (optpaths) with BGP -
		// basic graph patterns (paths)

		// Query q = SPARQLUtils.toConstructQuery(paths, optpaths);
		Query interestQuery = QueryDecomposer.toConstructQuery(paths);
		System.out.println("Full Interest query: " + interestQuery);

		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		//Model toBeRemoved = ModelFactory.createDefaultModel();
				
		Model r0 = SPARQLExecutor.executeConstruct(removedModel, interestQuery);
		interestingTriples.add(r0);
		removedModel.remove(r0);
		
		for (int i = paths.size() - 1; i > 0; i--) {
			List<Query> askQueries = QueryDecomposer.composeAskQueries(paths, i);
			for (Query q : askQueries) {
				if (SPARQLExecutor.executeAsk(removedModel, q)) {
					List<TriplePath> askPaths = QueryPatternExtractor.getBGPTriplePaths(q);

					Query cq = QueryDecomposer.toConstructQuery(askPaths);
					Model r = SPARQLExecutor.executeConstruct(removedModel, cq);
					// extract related triples with the matching partial triple
					// patterns of the interest query
					if (!r.isEmpty()) {
						Model rwithMissingTriples = extractMissingFromTarget(paths, askPaths, r, interest); //extractRelatedFromTarget
						if (!rwithMissingTriples.isEmpty()) {
							rwithMissingTriples.write(System.out, "N-TRIPLE");
							System.out.println("Missing triples Matching found from target!");
							interestingTriples.add(rwithMissingTriples);
							
							//compute set difference from rwithMissingTriples and r, which gives us potentially interesting triples
							Model diff = ModelFactory.createDefaultModel();
							diff = rwithMissingTriples.remove(r);
							potentiallyInterestingTriples.add(diff);
						} else {
							System.out.println("Missing triples Matching not found from target! So ignoring these triples");
						}
					}
					removedModel.remove(r);
				}
			}
		}
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}

	private Query bindValues(List<TriplePath> paths, Query query,	Model resultModel) {
		query.setResultVars();
		//System.out.println(query);
		ResultSet rs = SPARQLExecutor.executeSelect(resultModel, query);

		List<Binding> prevBindings = new ArrayList<Binding>();
		List<Var> vars = new ArrayList<Var>();
		Set<Var> varset = new HashSet<Var>();

		//System.out.println("iterating ..");

		while (rs.hasNext()) {
			Binding b = rs.nextBinding();
			prevBindings.add(b);
			Iterator<Var> itVar = b.vars();
			while (itVar.hasNext()) {
				Var v = itVar.next();
				varset.add(v);
			}
		}
		vars.addAll(varset);

		Query qeuryConst = QueryDecomposer.toConstructQuery(paths);
		qeuryConst.setValuesDataBlock(vars, prevBindings);
		return qeuryConst;
	}

	/**
	 * Interest evaluation over Added triples of a changeset,
	 * 
	 * @param interest
	 * @param changeset
	 * @return
	 */
	public void evaluateAdded(final Interest interest, Changeset changeset) {
		
		List<Model> evaluationResult = evaluateOnAddedTriples(changeset.getAddedTriples(), interest);
		
		// Insert potentially interesting triples to local triple store
		StringBuffer insertPIQuery = QueryDecomposer.toUpdate(evaluationResult.get(1), interest.getPigraph(), true);
		boolean storepi = SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, insertPIQuery.toString());
		if(storepi)
			System.out.println("Potentially Interesting updates stored for changeset: "+ changeset.getSequenceNum());
		else
			System.out.println("Cannot store Potentially Interesting updates for changeset: "+ changeset.getSequenceNum());
		//check if the newly inserted potentially interesting triples becomes interesting
		Model gamma0 = getInterestingsFromPI(interest);
		if(!gamma0.isEmpty()){
			// remove the potentially interesting triples becoming interesting from interests' pi  graph
			StringBuffer removePIQuery = QueryDecomposer.toUpdate(gamma0, interest.getPigraph(), false);
			boolean removingPI = SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, removePIQuery.toString());
		}
	
		if(evaluationResult.get(0).isEmpty())
			return ;
		// propagate interesting triples to interest subscribers' target update uri (endpoint - in this case)
		StringBuffer insertTargetQuery = QueryDecomposer.toUpdate(evaluationResult.get(0), true);
		boolean storeI = SPARQLExecutor.executeUpdate(interest.getTargetUpdateUri(), insertTargetQuery.toString());
		if(storeI){
			System.out.println("Interesting triples stored on target");
		}else{
			System.out.println("Cannot store Interesting triples on target");
		}
	}
	
	private Model getInterestingsFromPI(Interest interest){
		List<TriplePath> paths = interest.getTriplePaths();

		Query interestQuery = QueryDecomposer.toConstructQuery(paths, interest.getPigraph());
		System.out.println("PI Full interest query: " + interestQuery);

		Model gamma0 = SPARQLExecutor.executeConstruct(Global.PI_SPARQL_ENDPOINT, interestQuery);
		return gamma0;
	}

	/**
	 * evaluate interest expression on addition triples of a changeset
	 * 
	 * @param model
	 * @param interest
	 * @return
	 */
	private List<Model> evaluateOnAddedTriples(Model model, Interest interest) {
		List<TriplePath> paths = interest.getTriplePaths();

		Query interestQuery = QueryDecomposer.toConstructQuery(paths);
		System.out.println("Full interest query: " + interestQuery);

		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		
		Model gamma0 = SPARQLExecutor.executeConstruct(model, interestQuery);		
		interestingTriples.add(gamma0);
		model.remove(gamma0);

		for (int i = paths.size() - 1; i > 0; i--) {
			List<Query> askQueries = QueryDecomposer.composeAskQueries(paths, i);
			for (Query q : askQueries) {
				if (SPARQLExecutor.executeAsk(model, q)) {
					List<TriplePath> askPaths = QueryPatternExtractor.getBGPTriplePaths(q);

					Query cq = QueryDecomposer.toConstructQuery(askPaths);
					Model r = SPARQLExecutor.executeConstruct(model, cq);
					if (!r.isEmpty()) {
						Model rwithMissingTriples = extractMissingFromTarget(paths, askPaths, r, interest);
						if (!rwithMissingTriples.isEmpty()) {
							rwithMissingTriples.write(System.out, "N-TRIPLE");
							System.out.println("Missing triples Matching found from target!");
							interestingTriples.add(rwithMissingTriples);
						} else {
							System.out.println("Missing triples Matching not found from target!");
							potentiallyInterestingTriples.add(r);
						}
						
					}

					model.remove(r);
				}
			}
		}
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}

	public Model extractMissingFromTarget(List<TriplePath> paths, List<TriplePath> askPaths, Model r, final Interest interest) {

		Query matchingQuery = QueryDecomposer.toSelectQuery(askPaths);
		// construct a Construct query of the overall interest expression query
		// with bounded VALUES of the matching Model
		Query boundQuery = bindValues(paths, matchingQuery, r);
		//System.out.println("method of bounding VALUES: " + boundQuery);
		
		Model matchingResults = ModelFactory.createDefaultModel();
		if(interest.getTargetType() == 0){
			Dataset dataset = TDBFactory.createDataset(interest.getTargetUri());
			matchingResults = SPARQLExecutor.executeConstruct(dataset, boundQuery);
		}else if(interest.getTargetType() == 1){
			matchingResults  = SPARQLExecutor.executeConstruct(interest.getTargetUri(), boundQuery);
		}
		return matchingResults;
	}
	
}
