/**
 * 
 */
package de.unibonn.iai.eis.irap.evaluator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.tdb.TDBFactory;

import de.unibonn.iai.eis.irap.helper.Global;
import de.unibonn.iai.eis.irap.helper.LoggerLocal;
import de.unibonn.iai.eis.irap.interest.InterestExprGraph;
import de.unibonn.iai.eis.irap.interest.InterestExprNode;
import de.unibonn.iai.eis.irap.model.Changeset;
import de.unibonn.iai.eis.irap.model.Interest;
import de.unibonn.iai.eis.irap.model.Subscriber;
import de.unibonn.iai.eis.irap.sparql.QueryDecomposer;
import de.unibonn.iai.eis.irap.sparql.QueryPatternExtractor;
import de.unibonn.iai.eis.irap.sparql.SPARQLExecutor;

/**
 * @author keme686
 *
 */
public class InterestEvaluation {

	private static Logger logger = LoggerLocal.getLogger(InterestEvaluation.class.getName());

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
		
	private Subscriber subscriber;
	private Changeset changeset;

	public InterestEvaluation(Subscriber subscriber, Changeset changeset) {
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
		List<Expr> exp = QueryPatternExtractor.getFilters(interest.getQuery());
		for(Expr e: exp){
			logger.info(e);
			logger.info(e.getFunction());
			for(Var v: e.getVarsMentioned()){
				logger.info(v);
				
			}
		}
		
		//evaluate deletion on target dataset
		 evaluateRemoved(interest, changeset);
		//evaluate addition
		 evaluateAdded(interest, changeset);
	}

	private boolean propagateToTarget(Model model, final Interest interest, boolean isInsert){
		logger.info(changeset.getSequenceNum()+ ":" + "Propagating triples to TARGET_ENDPOINT	....");
		StringBuilder removeTargetQuery = QueryDecomposer.toUpdate(model, isInsert);		
		return SPARQLExecutor.executeUpdate(interest.getTargetUpdateUri(), removeTargetQuery.toString());
	}
	/**
	 *  Interest evaluation over removed triples 
	 *  
	 * @param interest
	 * @param changeset
	 * @return
	 */
	public void evaluateRemoved(final Interest interest, Changeset changeset) {
		//first evaluate the removed triples on Potentially interesting triples of an interest

		logger.info(changeset.getSequenceNum()+ ":" + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		logger.info(changeset.getSequenceNum()+ ":" + "EVALUATION OVER REMOVED TRIPLES");
		logger.info(changeset.getSequenceNum()+ ":" + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		Model removed = ModelFactory.createDefaultModel().add(changeset.getRemovedTriples());

		// remove triples from potentially interesting graph of this interest, if they exists	
		logger.info(changeset.getSequenceNum()+ ":" + "Removing deleted triples from PI_ENDPOINT ....");
		if(propagateToPI(removed, interest.getPigraph(), false)){
			logger.info(changeset.getSequenceNum()+ ":" + "Potentially interesting removed triples has been deleted from PI_Endpoint!");
		}else{
			logger.warn(changeset.getSequenceNum()+ ":" + "Cannot remove potentially interesting removed triples from PI_Endpoint!");
		}
		
		// Evaluate interest expression directly on target endpoint of a subscriber
		logger.info(changeset.getSequenceNum()+ ":" + "Evaluating on TARGET_ENDPOINT ....");
		Model targetRemoved = ModelFactory.createDefaultModel().add(changeset.getRemovedTriples());
		List<Model>  removeResult = evaluateOnTarget(targetRemoved, interest, false);
		
		
		//store triples from target that become potentially interesting
		logger.info(changeset.getSequenceNum()+ ":" + "Storing potentially interesting triples in PI_ENDPOINT .... ");
		if(propagateToPI(removeResult.get(1), interest.getPigraph(), true)){
			logger.info(changeset.getSequenceNum()+ ":" + "Potenatilly interesting triples removed from target dataset!");
		}else{
			logger.debug(changeset.getSequenceNum()+ ":" + "Cannot store potenatilly interesting triples extracted from target dataset!");
		}
		
		// propagate interesting removed triples to interest subscribers' target update uri (endpoint - in this case)
		if( propagateToTarget(removeResult.get(0), interest, false)){
			logger.info(changeset.getSequenceNum()+ ":" + "Interesting removed triples propagated to target dataset: " + interest.getTargetUpdateUri());
		}else{
			logger.debug(changeset.getSequenceNum()+ ":" + "Cannot propagate interesting removed triples to target dataset:" + interest.getTargetUpdateUri());
		}
		logger.info(changeset.getSequenceNum()+ ":" + "-----------------END of EVALUATION ON REMOVED TRIPLES -------------");
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
		StringBuilder removePIQuery = QueryDecomposer.toUpdate(removedModel, interest.getPigraph(), false);
		return SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, removePIQuery.toString());		
	}

	/**
	 * Interest evaluation over Added triples of a changeset,
	 * 
	 * @param interest
	 * @param changeset
	 * @return
	 */
	public void evaluateAdded(final Interest interest, Changeset changeset) {
		Model added = ModelFactory.createDefaultModel().add(changeset.getAddedTriples());
		logger.info(changeset.getSequenceNum()+ ":" + "+++++++++++++++++++++++++++++++++++++++++");
		logger.info(changeset.getSequenceNum()+ ":" + "      EVALUATION OVER ADDED TRIPLES");
		logger.info(changeset.getSequenceNum()+ ":" + "+++++++++++++++++++++++++++++++++++++++++");
		
		List<Model> evaluationResult = evaluateOnTarget(added, interest, true);
		
		if(!evaluationResult.get(1).isEmpty()){
			// Insert potentially interesting triples to local triple store
			
			//StringBuilder insertPIQuery = QueryDecomposer.toUpdate(evaluationResult.get(1), interest.getPigraph(), true);
			if(propagateToPI(evaluationResult.get(1), interest.getPigraph(), true))
			//if(SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, insertPIQuery.toString()))
				logger.info(changeset.getSequenceNum()+ ":" + "Potentially Interesting updates stored for changeset: "+ changeset.getSequenceNum());
			else
				logger.info(changeset.getSequenceNum()+ ":" + "Cannot store Potentially Interesting updates for changeset: "+ changeset.getSequenceNum());
		}
		
		//TODO: check if this next step have any result.  already done on evaluateOnTarget() ?
		//check if the newly inserted potentially interesting triples becomes interesting
		logger.info(changeset.getSequenceNum()+ ":" + "CHECKING MISSING TRIPLES IN PI");
		Model gamma0 = getInterestingsFromPI(interest);
		if(!gamma0.isEmpty()){
			// remove the potentially interesting triples becoming interesting from interests' pi  graph
			//StringBuilder removePIQuery = QueryDecomposer.toUpdate(gamma0, interest.getPigraph(), false);
			if(propagateToPI(gamma0, interest.getPigraph(), false)){
			//if(SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, removePIQuery.toString())){
				logger.info(changeset.getSequenceNum()+ ":" + "Triples that become interesting has been removed from PI endpoint!");
			}
			evaluationResult.get(0).add(gamma0);
		}else{
			logger.info(changeset.getSequenceNum()+ ":" + "!!No matching for missing triples found.");
		}
		
		logger.info(changeset.getSequenceNum()+ ":" + "CHECKING ENDS HERE ------");
		
		if(evaluationResult.get(0).isEmpty())
			return ;
		
		
		// propagate interesting triples to interest subscribers' target update uri (endpoint - in this case)
		//StringBuilder insertTargetQuery = QueryDecomposer.toUpdate(evaluationResult.get(0), true);
		if(propagateToTarget(evaluationResult.get(0), interest, true)){
		//if(SPARQLExecutor.executeUpdate(interest.getTargetUpdateUri(), insertTargetQuery.toString())){
			logger.info(changeset.getSequenceNum()+ ":" + "Interesting triples stored on target");
		}else{
			logger.info(changeset.getSequenceNum()+ ":" + "Cannot store Interesting triples on target");
		}
		
		logger.info(changeset.getSequenceNum()+ ":" + "~~~~~~~~~~~~~~~~END of EVALUATING ADDED TRIPLES ~~~~~~~~~~~~~~~~~~");
	}
	
	private Model getInterestingsFromPI(Interest interest){
		List<TriplePath> paths = interest.getTriplePaths();

		Query interestQuery = QueryDecomposer.toConstructQuery(paths, interest.getPigraph());
		logger.info(changeset.getSequenceNum()+ ":" + "PI Full interest query: \n" + interestQuery);

		Model gamma0 = SPARQLExecutor.executeConstruct(Global.PI_SPARQL_ENDPOINT, interestQuery);
		
		return gamma0;
	}
	/**
	 * 
	 * @param model
	 * @param interest
	 * @param isAdded
	 * @return
	 */
	private List<Model> evaluateOnTarget(Model model, final Interest interest, boolean isAdded){
		List<TriplePath> paths = interest.getTriplePaths();
		List<TriplePath> optpaths = interest.getOptionalTriplePaths();
		
		Query interestQuery = QueryDecomposer.toConstructQuery(paths, optpaths);
		
		logger.info(changeset.getSequenceNum()+ ":" + "Full Interest query: \n" + interestQuery);
		
		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		
		// extract interesting triples from  model
		Model gamma = SPARQLExecutor.executeConstruct(model, interestQuery);
		
		if (!gamma.isEmpty()) {
			logger.info(changeset.getSequenceNum()+ ":" + gamma.size() +  " - Intersting triples found!");
			gamma.write(System.out, "N-TRIPLE");
			// TODO: if there is a filter expr, then apply filter before saving as
			// interesting triples		
			if(interest.getElementFilters() != null && !interest.getElementFilters().isEmpty()){
				gamma = applyFilter(gamma, interest);
				logger.info("Gamma after filter");
				gamma.write(System.out, "N-TRIPLE");
			}
			if (isAdded) {
				// Then: extract optionals in PI
				logger.info("Extracting optional for interesting triples: ");
				Model piOpts = extractPartialsInPI(paths, optpaths, paths, gamma, interest);
				if (!piOpts.isEmpty()) {
					logger.info("Optional from PI found!");
					interestingTriples.add(piOpts);
				} else {
					logger.info("!Optional from PI NOT found!");
					interestingTriples.add(gamma);
				}
			}else {
				interestingTriples.add(gamma);
			}
			model.remove(gamma);
		} else {
			logger.info(changeset.getSequenceNum()+ ":" + "!No interesting  triples found");
		}
		
		for(int i = paths.size()-1; i>0; i--){
			//2^b ask queries
			List<Query> askQueries = QueryDecomposer.composeAskQueries(paths, i);
			
			InterestExprGraph g = new InterestExprGraph();
			
			for (Query q : askQueries) {	
				List<TriplePath> askPaths = QueryPatternExtractor.getBGPTriplePaths(q);
				if(askPaths.size()==1 && isAllVars(askPaths.get(0))){
					logger.info(changeset.getSequenceNum()+ ":" + "SKIPPED: \n" + askPaths.get(0));
					continue;
				}
					
				//if q contains disjoint pattern				
				if(!g.isValid(q)){
					logger.info(changeset.getSequenceNum()+ ":" + "SKIPPED: Disjoint Query: \n" + q);
					continue;
				}
				logger.info(changeset.getSequenceNum()+ ":" + "Asking triples for query: \n"+ q);
				if (SPARQLExecutor.executeAsk(model, q)) {					
					Query cq = QueryDecomposer.toConstructQuery(askPaths, optpaths);
					
					boolean includeOpts = true;
					if (!this.isValidCombination(askPaths, optpaths)) {
						cq = QueryDecomposer.toConstructQuery(askPaths);
						includeOpts = false;
					}
					// extract c_i
					Model r = SPARQLExecutor.executeConstruct(model, cq);
					
					logger.info(changeset.getSequenceNum()+ ":" + "Partial matching found: " );
					r.write(System.out, "N-TRIPLE");
					
					if (!r.isEmpty()) {
						List<TriplePath> comb = new ArrayList<TriplePath>();						
						if(includeOpts){
							comb.addAll(optpaths);
						}		
						if (isAdded) {
							// find missings in PI
							Model piInteresting = extractPartialsInPI(paths, optpaths, askPaths, r, interest);
							if (!piInteresting.isEmpty()) {
								//filter
								if(interest.getElementFilters() != null && !interest.getElementFilters().isEmpty()){
									piInteresting = applyFilter(piInteresting, interest);
									logger.info("piInteresting after filter");
									piInteresting.write(System.out, "N-TRIPLE");
								}
								interestingTriples.add(piInteresting);
							
								logger.info(changeset.getSequenceNum()+ ": Missing found in PI!");
								piInteresting.write(System.out, "N-TRIPLE");
								
								//remove from PI
								Model m = ModelFactory.createDefaultModel().add(piInteresting);
								m.remove(r);
								
								logger.info(changeset.getSequenceNum()+ ": Removing partials in PI");
								m.write(System.out, "N-TRIPLE");
								
								evaluateRemovedForPI(m, interest);
								//remove r from changeset model
								model.remove(r);
								continue;
							}
							
							Model partials = getPartialEvaluationForAdded(paths, comb, askPaths, r, interest);
							if(!partials.isEmpty()){
								
								interestingTriples.add(partials);
								
								logger.info(changeset.getSequenceNum()+ ": Partials found in TARGET");
								partials.write(System.out, "N-TRIPLE");
								
								//remove from PI
								Model m = ModelFactory.createDefaultModel().add(partials);
								m.remove(r);
								
								logger.info(changeset.getSequenceNum()+ ": Removing partials in PI after finding missing in target ..");
								m.write(System.out, "N-TRIPLE");
								
								if(!m.isEmpty())
									evaluateRemovedForPI(m, interest);
							}
							
							if(partials.isEmpty())
								potentiallyInterestingTriples.add(r);
							else
								potentiallyInterestingTriples.add(r.remove(partials));
							
						}else{ //if removed							
							List<Model> partials = getPartialEvaluationForRemoved(paths, comb, askPaths, r, interest);
							
							interestingTriples.add(partials.get(0));
							
							potentiallyInterestingTriples.add(partials.get(1));
						}
					}
					 model.remove(r);
				}else{
					logger.info(changeset.getSequenceNum()+ ":" + ">!!No matching found for query asked query ");
				}
			}
		}
		
		//only optionals
		if (!isAdded) {
		
			Query optq = QueryDecomposer.toConstructQuery(optpaths);
			Model o = SPARQLExecutor.executeConstruct(model, optq);
			if (!o.isEmpty()) {
				logger.info("OPTIONAL only triples: ");
				interestingTriples.add(o);
				o.write(System.out,"N-TRIPLE");
				model.remove(o);
			}
			//TODO: if partial pattern matches in optional, then retrieve missing in target and delete all related optionals
			
		}else{
			Query optq = QueryDecomposer.toConstructQuery(optpaths);
			Model o = SPARQLExecutor.executeConstruct(model, optq);
			if (!o.isEmpty()) {
				logger.info("OPTIONAL only triples: ");
				interestingTriples.add(o);
				o.write(System.out,"N-TRIPLE");
				model.remove(o);
			}
			for (TriplePath tp : optpaths) {
				Query oq = QueryDecomposer.toConstructQuery(tp);
				Model r = SPARQLExecutor.executeConstruct(model, oq);
				if (!r.isEmpty()) {
					potentiallyInterestingTriples.add(r);
					model.remove(r);
				}
			}
		}
			
		
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}
	private Model applyFilter(Model model, Interest interest){
		List<TriplePath> paths = interest.getTriplePaths();
		List<TriplePath> optpaths = interest.getOptionalTriplePaths();
		List<ElementFilter> filters  = interest.getElementFilters();
		Query interestQuery = QueryDecomposer.toConstructQuery(paths, optpaths, filters);
		
		return SPARQLExecutor.executeConstruct(model, interestQuery);
		
	}
	private Model extractMissingFromTarget(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath> askPaths, Model r, final Interest interest) {

		// construct a Construct query of the overall interest expression query
		// with bounded VALUES of the matching Model		
		Query boundQuery = bindValues(paths, optpaths, askPaths, r, null);
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
	
	private Model getPartialEvaluationForAdded(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath> askPaths, Model r, final Interest interest){
		
		InterestExprGraph g = new InterestExprGraph();
		Model interestingTriples = ModelFactory.createDefaultModel();
		
		// find partially matching from PI and check the rest from target
		List<TriplePath> askDiff = new ArrayList<TriplePath>();
		askDiff.addAll(paths);
		askDiff.removeAll(askPaths);
		
		for (int j = askDiff.size()-1 ; j > 0; j--) {
			List<Query> consQueries = QueryDecomposer.composeConstructQueries(askDiff, j);

			for (Query qc : consQueries) {
				List<TriplePath> diffAndAsk = new ArrayList<TriplePath>();
				diffAndAsk.addAll(askPaths);
				diffAndAsk.addAll(QueryPatternExtractor.getBGPTriplePaths(qc));
				
				if(!g.isValid(QueryDecomposer.toAskQuery(diffAndAsk))){
					logger.info("DISJOINT delta: \n" + QueryDecomposer.toAskQuery(diffAndAsk));					
					continue;
				}
				//candidate C_i from Delta = A and pi
				Model piR = extractPartialsInPI(diffAndAsk,	optpaths, askPaths, r, interest);
				
				logger.info("Combined delta: \n" + QueryDecomposer.toAskQuery(diffAndAsk));
				piR.write(System.out, "N-TRIPLE");
				
				if (piR.isEmpty()) {
					logger.info("There was no matching in PI");
					piR.add(r);
					diffAndAsk = askPaths;
				}
				
				Model rwithMissingTriples = extractMissingFromTarget(paths, optpaths, diffAndAsk, piR,	interest);
				if (!rwithMissingTriples.isEmpty()) {
					logger.error("Just messing up with ....found missing from target" );
					logger.info(changeset.getSequenceNum()+ ":" + "MISSING triples Matching FOUND from TARGET!");
					rwithMissingTriples.write(System.out, "N-TRIPLE");
					//filter
					if(interest.getElementFilters() != null && !interest.getElementFilters().isEmpty()){
						rwithMissingTriples = applyFilter(rwithMissingTriples, interest);
						logger.info("rwithMissingTriples after filter");
						rwithMissingTriples.write(System.out, "N-TRIPLE");
						if(!rwithMissingTriples.isEmpty())
							interestingTriples.add(piR);
					}else
						interestingTriples.add(piR);
					
				} else {
					logger.info(changeset.getSequenceNum()+ ":" + "!!!Missing triples for added triples Matching NOT found from target!");					
				}
			}
		}
		return interestingTriples;
	}
	
	private List<Model> getPartialEvaluationForRemoved(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath> askPaths, Model r, final Interest interest){
		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		
		InterestExprGraph g = new InterestExprGraph();
				
		logger.info("Partial evaluation for removed: \n" + QueryDecomposer.toAskQuery(askPaths));
		
		Model rwithMissingTriples = extractMissingFromTarget(paths, optpaths, askPaths, r, interest); //extractRelatedFromTarget
		
		if (!rwithMissingTriples.isEmpty()) {
			
			logger.info(changeset.getSequenceNum()+ ":" + "Missing triples Matching FOUND from TARGET!");
			rwithMissingTriples.write(System.out, "N-TRIPLE");		
			if(interest.getElementFilters() != null && !interest.getElementFilters().isEmpty()){
				rwithMissingTriples = applyFilter(rwithMissingTriples, interest);				
				logger.info("rwithMissingTriples after filter");
				rwithMissingTriples.write(System.out, "N-TRIPLE");
			}
			// compute set difference from rwithMissingTriples and r, which gives potentially interesting triples
			Model diff = ModelFactory.createDefaultModel().add(rwithMissingTriples);
			diff = diff.remove(r);
			
			g.createGraph(QueryDecomposer.toAskQuery(askPaths));
			Map<Node, InterestExprNode> rtrees = g.getTrees();

			//determine missing triple pattern
			List<TriplePath> diffTp = new ArrayList<TriplePath>();
			diffTp.addAll(paths);
			diffTp.removeAll(askPaths);
			
			InterestExprGraph gDiff = new InterestExprGraph();
			gDiff.createGraph(QueryDecomposer.toAskQuery(diffTp));
			Map<Node, InterestExprNode> diffTrees = gDiff.getTrees();

			for (Node n : diffTrees.keySet()) {
				logger.info("Diff graph tree: \n" + QueryDecomposer.toAskQuery(diffTrees.get(n).triplePath) );
				if (rtrees.containsKey(n)){
					logger.info("Same tree found in result trees \n" + QueryDecomposer.toAskQuery(rtrees.get(n).triplePath));
					continue;
				}

				List<TriplePath> tp = diffTrees.get(n).triplePath;
				Query tq = QueryDecomposer.toConstructQuery(tp);
				Model tv = SPARQLExecutor.executeConstruct(diff, tq);
				
				Query bq = bindValues(paths,new ArrayList<TriplePath>(),tp, tv, null);
				Model alpha = SPARQLExecutor.executeConstruct(rwithMissingTriples, bq);
				Model beta = SPARQLExecutor.executeConstruct(interest.getTargetUri(), bq);

				if (alpha.size() < beta.size()) {
					diff.remove(tv);
					rwithMissingTriples.remove(tv);
				}
			}
			potentiallyInterestingTriples.add(diff);
			// TODO: check if the diff is related to r only.
			// If there are other triples connected a triple
			// in diff from target, then leave this triple
			// (remove from rwithMissingTriples)			
			interestingTriples.add(rwithMissingTriples);
			
		} else { //if related triples not found
			logger.info(changeset.getSequenceNum()+ ":" + "!!!~Missing triples Matching not found from target!");			
		}
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}
	private Model extractPartialsInPI(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath> comPaths, Model r, final Interest interest){
		// construct a Construct query of the overall interest expression query
		// with bounded VALUES of the matching Model		
		Query boundQuery = bindValues(paths, optpaths, comPaths, r, interest.getPigraph());
		logger.info("extractPartialsInPI: bounded query: \n" + boundQuery);
		//System.out.println("method of bounding VALUES: " + boundQuery);
		Model gamma = SPARQLExecutor.executeConstruct(Global.PI_SPARQL_ENDPOINT, boundQuery);
		
		return gamma;
	}
	
	/**
	 * store potentially interesting triples to local triple store
	 * 
	 * @param model
	 * @param graph
	 * @param isAdd
	 * @return
	 */
	private boolean propagateToPI(Model model, String graph, boolean isAdd){
		StringBuilder insertPIQuery = QueryDecomposer.toUpdate(model, graph, isAdd);
		return SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, insertPIQuery.toString());
	}
	
	
	private Query bindValues(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath>  queryPath,	Model resultModel, String graph) {
		List<TriplePath> tps = new ArrayList<TriplePath>();
		tps.addAll(queryPath);
		for(TriplePath tp: queryPath){
			if(isAllVars(tp))
				tps.remove(tp);
		}
		Query query = QueryDecomposer.toSelectQuery(tps);
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
		Query qeuryConst;
		if(graph != null && !graph.isEmpty())
			qeuryConst = QueryDecomposer.toConstructQuery(paths, optpaths, graph);
		else
			qeuryConst = QueryDecomposer.toConstructQuery(paths, optpaths);
		
		if (!optpaths.isEmpty() && !isValidCombination(paths, optpaths)) {
			if(graph != null && !graph.isEmpty())
				qeuryConst = QueryDecomposer.toConstructQuery(paths, graph);
			else
				qeuryConst = QueryDecomposer.toConstructQuery(paths);
		}

		qeuryConst.setValuesDataBlock(vars, prevBindings);
		return qeuryConst;
	}

	private boolean isValidCombination(List<TriplePath> paths, List<TriplePath> optpaths){
		InterestExprGraph g = new InterestExprGraph();
		if (!optpaths.isEmpty()) {
			List<TriplePath> comb = new ArrayList<TriplePath>();
			comb.addAll(paths);
			comb.addAll(optpaths);
			Query combq = QueryDecomposer.toAskQuery(comb);			
			if (!g.isValid(combq)) {
				return false;
			}
		}	
		return true;
	}
	private boolean isAllVars(TriplePath tp){
		if(!tp.getSubject().isVariable())
			return false;
		if(!tp.getPredicate().isVariable())
			return false;
		if(!tp.getObject().isVariable())
			return false;
		return true;
	}
}
