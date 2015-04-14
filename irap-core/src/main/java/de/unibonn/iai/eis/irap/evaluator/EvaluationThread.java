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
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.tdb.TDBFactory;

import de.unibonn.iai.eis.irap.helper.Global;
import de.unibonn.iai.eis.irap.helper.LoggerLocal;
import de.unibonn.iai.eis.irap.interest.InterestExprGraph;
import de.unibonn.iai.eis.irap.interest.InterestExprNode;
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
			logger.info("=====================================================================================");
			logger.info(i.getQuery());
			logger.info("BGP size: " + i.getTriplePaths().size());
			logger.info("OGP size: " + i.getOptionalTriplePaths().size());
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
	 *  Interest evaluation over removed triples 
	 *  
	 * @param interest
	 * @param changeset
	 * @return
	 */
	public void evaluateRemoved(final Interest interest, Changeset changeset) {
		//first evaluate the removed triples on Potentially interesting triples of an interest
		logger.info("EVALUATION ON REMOVED TRIPLES");
		logger.info("--------------------------");
		
		// remove triples from potentially interesting graph of this interest, if they exists
		Model removed = ModelFactory.createDefaultModel().add(changeset.getRemovedTriples());
		if(evaluateRemovedForPI(removed, interest)){
			logger.info("Potentially interesting deleted triples has been removed from PI dataset!");
		}else{
			logger.warn("Cannot remove potentially interesting deleted triples from PI dataset!");
		}
		
		// Evaluate interest expression directly on target endpoint of a subscriber
		Model targetRemoved = ModelFactory.createDefaultModel().add(changeset.getRemovedTriples());
		List<Model>  removeResult = evaluateOnTarget(targetRemoved, interest, false);
		//store triples from target that become potentially interesting
		if(updatePI(removeResult.get(1), interest.getPigraph(), true)){
			logger.info("Potenatilly interesting triples removed from target dataset!");
		}else{
			logger.debug("Cannot store potenatilly interesting triples extracted from target dataset!");
		}
		// propagate interesting removed triples to interest subscribers' target update uri (endpoint - in this case)		
		StringBuilder removeTargetQuery = QueryDecomposer.toUpdate(removeResult.get(0), false);
		if( SPARQLExecutor.executeUpdate(interest.getTargetUpdateUri(), removeTargetQuery.toString())){
			logger.info("Interesting removed triples propagated to target dataset: " + interest.getTargetUpdateUri());
		}else{
			logger.debug("Cannot propagate interesting removed triples to target dataset:" + interest.getTargetUpdateUri());
		}
		logger.info("-----------------END of EVALUATION ON REMOVED TRIPLES -------------");
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
	 * Evaluates interest expression over deleted triples of a changeset
	 * 
	 * @param removedModel
	 * @param interest
	 * @return
	 */
	private List<Model> evaluateOnRemovedOfTarget(Model removedModel, final Interest interest) {
		
		List<TriplePath> paths = interest.getTriplePaths();
		List<TriplePath> optpaths = interest.getOptionalTriplePaths();
		
		// List<Model> result = new ArrayList<Model>();
		// TODO: include OGP - optional graph patterns (optpaths) with BGP -
		// basic graph patterns (paths)
		
		// Query q = SPARQLUtils.toConstructQuery(paths, optpaths);
		Query interestQuery = QueryDecomposer.toConstructQuery(paths, optpaths);
		logger.info("Full Interest query: \n" + interestQuery);
		
		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		
		//extract interest removed triples from Removed model
		Model r0 = SPARQLExecutor.executeConstruct(removedModel, interestQuery);
		if(!r0.isEmpty()){
			logger.info("Intersting removed triples found!");
			r0.write(System.out, "N-TRIPLE");
		}else{
			logger.info("No interesting removed triples found");
		}
		//TODO: if there is a filter expr, then apply filter before saving as interesting triples
		interestingTriples.add(r0);
		removedModel.remove(r0);
		
		//TODO: if paths.size is 1, then skip this step
		
		logger.info("Now checking for potentially interesting triples ...");
		for (int i = paths.size() - 1; i > 0; i--) {
			List<Query> askQueries = QueryDecomposer.composeAskQueries(paths, i);
			InterestExprGraph g = new InterestExprGraph();
			for (Query q : askQueries) {			
				//if q contains disjoint pattern
				if(!g.isValid(q)){
					logger.info("SKIPPED: Disjoint Query: " + q);
					continue;
				}
				logger.info("Asking removed triples for query: \n"+ q);
				if (SPARQLExecutor.executeAsk(removedModel, q)) {
					List<TriplePath> askPaths = QueryPatternExtractor.getBGPTriplePaths(q);
					if(askPaths.size() ==1 && isAllVars(askPaths.get(0)))
						continue;
					Query cq = QueryDecomposer.toConstructQuery(askPaths);
					Model r = SPARQLExecutor.executeConstruct(removedModel, cq);
					logger.info("Partial matching found: " );
					r.write(System.out, "N-TRIPLE");
					// extract related triples with the matching partial triple
					// patterns of the interest query
					if (!r.isEmpty()) {
						Model rwithMissingTriples = extractMissingFromTarget(paths, optpaths, askPaths, r, interest); //extractRelatedFromTarget
						if (!rwithMissingTriples.isEmpty()) {
							logger.info("Missing triples found from target:");
							rwithMissingTriples.write(System.out, "N-TRIPLE");
							interestingTriples.add(rwithMissingTriples);
							
							//compute set difference from rwithMissingTriples and r, which gives us potentially interesting triples
							Model diff = ModelFactory.createDefaultModel();
							diff = rwithMissingTriples.remove(r);
							potentiallyInterestingTriples.add(diff);
						} else {
							logger.info("Missing triples Matching not found from target!");
						}
					}else{
						logger.debug("There should have been triples that were tested exists by query:" + q);
					}
						removedModel.remove(r);
				}else{
					logger.info("No matching found for query asked query ");
				}
			}
		}
		//TODO: do combination of candidates for only optional paths and put them as potentially interesting
		
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}

	private boolean isAllVars(TriplePath tp){
		Node s = tp.getSubject();
		if(!s.isVariable())
			return false;
		Node p = tp.getPredicate();
		if(!p.isVariable())
			return false;
		Node o = tp.getObject();
		if(!o.isVariable())
			return false;
		
		return true;
	}
	
	private List<Model> evaluateOnTarget(Model model, final Interest interest, boolean isAdded){
		List<TriplePath> paths = interest.getTriplePaths();
		List<TriplePath> optpaths = interest.getOptionalTriplePaths();
		
		Query interestQuery = QueryDecomposer.toConstructQuery(paths, optpaths);
		logger.info("Full Interest query: \n" + interestQuery);
		
		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		
		// extract interesting triples from  model
		Model gamma = SPARQLExecutor.executeConstruct(model, interestQuery);
		if (!gamma.isEmpty()) {
			logger.info("Intersting triples found!");
			gamma.write(System.out, "N-TRIPLE");
		} else {
			logger.info("No interesting  triples found");
		}
		// TODO: if there is a filter expr, then apply filter before saving as
		// interesting triples
		interestingTriples.add(gamma);
		model.remove(gamma);
		
		for(int i = paths.size()-1; i>0; i--){
			List<Query> askQueries = QueryDecomposer.composeAskQueries(paths, i);
			InterestExprGraph g = new InterestExprGraph();
			for (Query q : askQueries) {			
				//if q contains disjoint pattern
				if(!g.isValid(q)){
					logger.info("SKIPPED: Disjoint Query: " + q);
					continue;
				}
				logger.info("Asking triples for query: \n"+ q);
				if (SPARQLExecutor.executeAsk(model, q)) {
					List<TriplePath> askPaths = QueryPatternExtractor.getBGPTriplePaths(q);
					
					Query cq = QueryDecomposer.toConstructQuery(askPaths, optpaths);
					boolean isValid = true;
					if (!this.isValidCombination(askPaths, optpaths)) {
						cq = QueryDecomposer.toConstructQuery(askPaths);
						isValid = false;
					}
					Model r = SPARQLExecutor.executeConstruct(model, cq);
					logger.info("Partial matching found: " );
					r.write(System.out, "N-TRIPLE");
					if (!r.isEmpty()) {
						List<TriplePath> comb = new ArrayList<TriplePath>();						
						if(isValid){
							comb.addAll(optpaths);
						}
						
						Model rwithMissingTriples = extractMissingFromTarget(paths, comb, askPaths, r, interest); //extractRelatedFromTarget
						if (!rwithMissingTriples.isEmpty()) {
							logger.info("Missing triples Matching found from target!");
							//rwithMissingTriples.write(System.out, "N-TRIPLE");
							if(isAdded){	
								
								interestingTriples.add(r);
							}else{ //if isRemoved
															
								//compute set difference from rwithMissingTriples and r, which gives potentially interesting triples
								Model diff = ModelFactory.createDefaultModel().add(rwithMissingTriples);
								diff = diff.remove(r);
																
								Map<Node, InterestExprNode> rtrees = g.getTrees();
								
								InterestExprGraph gDiff = new InterestExprGraph();
								List<TriplePath> diffTp = new ArrayList<TriplePath>();
								diffTp.addAll(paths);								
								diffTp.removeAll(askPaths);
								gDiff.createGraph(QueryDecomposer.toAskQuery(diffTp));
								Map<Node, InterestExprNode> diffTrees = gDiff.getTrees();
								
								for(Node n: diffTrees.keySet()){
									if(rtrees.containsKey(n))
										continue;
									
									List<TriplePath> tp = diffTrees.get(n).triplePath;
									Query tq = QueryDecomposer.toConstructQuery(tp);
									Model tv = SPARQLExecutor.executeConstruct(diff, tq);
									
									Query bq = bindValues(paths,new ArrayList<TriplePath>(),QueryDecomposer.toSelectQuery(tp), tv);
									Model alpha = SPARQLExecutor.executeConstruct(rwithMissingTriples, bq);
									Model beta = SPARQLExecutor.executeConstruct(interest.getTargetUri(), bq);
									
									if(alpha.size() != beta.size()){
										diff.remove(tv);
										rwithMissingTriples.remove(tv);
									}
								}
																								
								potentiallyInterestingTriples.add(diff);
								//TODO: check if the diff is related to r only. If there are other triples connected a triple in diff from target, then leave this triple (remove from rwithMissingTriples)
								interestingTriples.add(rwithMissingTriples);
							}
						} else { //if related triples not found
							logger.info("Missing triples Matching not found from target!");
							if(isAdded){
								potentiallyInterestingTriples.add(r);
							}
						}
					}
					 model.remove(r);
				}else{
					logger.info("No matching found for query asked query ");
				}
			}
		}
		
		//only optionals
		for (TriplePath tp : optpaths) {
			Query oq = QueryDecomposer.toConstructQuery(tp);
			Model r = SPARQLExecutor.executeConstruct(model, oq);
			if (!r.isEmpty()) {
				potentiallyInterestingTriples.add(r);
				model.remove(r);
			}
		}
		
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}
	private Query bindValues(List<TriplePath> paths, List<TriplePath> optpaths, Query query,	Model resultModel) {
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
		Query qeuryConst = QueryDecomposer.toConstructQuery(paths, optpaths);

		if (!optpaths.isEmpty() && !isValidCombination(paths, optpaths)) {
			qeuryConst = QueryDecomposer.toConstructQuery(paths);
		}

		qeuryConst.setValuesDataBlock(vars, prevBindings);
		return qeuryConst;
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
		StringBuilder insertPIQuery = QueryDecomposer.toUpdate(model, graph, isAdd);
		return SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, insertPIQuery.toString());
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
		
		List<Model> evaluationResult = evaluateOnAddedTriples(added, interest);
		
		if(!evaluationResult.get(1).isClosed()){
			// Insert potentially interesting triples to local triple store
			StringBuilder insertPIQuery = QueryDecomposer.toUpdate(evaluationResult.get(1), interest.getPigraph(), true);
			if(SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, insertPIQuery.toString()))
				logger.info("Potentially Interesting updates stored for changeset: "+ changeset.getSequenceNum());
			else
				logger.info("Cannot store Potentially Interesting updates for changeset: "+ changeset.getSequenceNum());
		}
		logger.info("CHECKING MISSING TRIPLES IN PI");
		//check if the newly inserted potentially interesting triples becomes interesting
		Model gamma0 = getInterestingsFromPI(interest);
		if(!gamma0.isEmpty()){
			// remove the potentially interesting triples becoming interesting from interests' pi  graph
			StringBuilder removePIQuery = QueryDecomposer.toUpdate(gamma0, interest.getPigraph(), false);
			if(SPARQLExecutor.executeUpdate(Global.PI_UPDATE_ENDPOINT, removePIQuery.toString())){
				logger.info("Triples that become interesting has been removed from PI endpoint!");
			}
			evaluationResult.get(0).add(gamma0);
		}else{
			logger.info("No matching for missing triples found.");
		}
		logger.info("CHECKING ENDS HERE ------");
		if(evaluationResult.get(0).isEmpty())
			return ;
		// propagate interesting triples to interest subscribers' target update uri (endpoint - in this case)
		StringBuilder insertTargetQuery = QueryDecomposer.toUpdate(evaluationResult.get(0), true);
		if(SPARQLExecutor.executeUpdate(interest.getTargetUpdateUri(), insertTargetQuery.toString())){
			logger.info("Interesting triples stored on target");
		}else{
			logger.info("Cannot store Interesting triples on target");
		}
		logger.info("END of EVALUATING ADDED TRIPLES   -------");
	}
	
	private Model getInterestingsFromPI(Interest interest){
		List<TriplePath> paths = interest.getTriplePaths();

		Query interestQuery = QueryDecomposer.toConstructQuery(paths, interest.getPigraph());
		logger.info("PI Full interest query: \n" + interestQuery);

		Model gamma0 = SPARQLExecutor.executeConstruct(Global.PI_SPARQL_ENDPOINT, interestQuery);
		
		return gamma0;
	}
	private Model extractPartialsInPI(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath> comPaths, Model r, final Interest interest){
		Query matchingQuery = QueryDecomposer.toSelectQuery(comPaths);
		// construct a Construct query of the overall interest expression query
		// with bounded VALUES of the matching Model		
		Query boundQuery = bindValues(paths, optpaths, matchingQuery, r);
		//System.out.println("method of bounding VALUES: " + boundQuery);
		Model gamma = SPARQLExecutor.executeConstruct(Global.PI_SPARQL_ENDPOINT, boundQuery);
		
		return gamma;
	}
	
	/**
	 * evaluate interest expression on addition triples of a changeset
	 * 
	 * @param model
	 * @param interest
	 * @return
	 */
	private List<Model> evaluateOnAddedTriples(Model model, Interest interest) {
		logger.info("EVALUATING ADDED TRIPLES");
		logger.info("--------------------------");
		List<TriplePath> paths = interest.getTriplePaths();
		List<TriplePath> optpaths = interest.getOptionalTriplePaths();
		
		Query interestQuery = QueryDecomposer.toConstructQuery(paths, optpaths );
		logger.info("Full interest query:\n " + interestQuery);

		Model interestingTriples = ModelFactory.createDefaultModel();
		Model potentiallyInterestingTriples = ModelFactory.createDefaultModel();
		
		Model gamma0 = SPARQLExecutor.executeConstruct(model, interestQuery);		
		if(!gamma0.isEmpty()){
			logger.info("Interesting triples found");
			gamma0.write(System.out, "N-TRIPLE");
		}else{
			logger.info("No interesting triples found");
		}
		interestingTriples.add(gamma0);
		model.remove(gamma0);
		//TODO: 
		for (int i = paths.size() - 1; i > 0; i--) {
			List<Query> askQueries = QueryDecomposer.composeAskQueries(paths, i);
			InterestExprGraph g = new InterestExprGraph();
			for (Query q : askQueries) {
				//if q contains disjoint pattern
				if(!g.isValid(q)){
					logger.info("SKIPPED: Disjoint Query: " + q);
					continue;
				}
				logger.info("Asking for:\n"+ q);
				if (SPARQLExecutor.executeAsk(model, q)) {
					List<TriplePath> askPaths = QueryPatternExtractor.getBGPTriplePaths(q);
					Query cq = QueryDecomposer.toConstructQuery(askPaths);
					Model r = SPARQLExecutor.executeConstruct(model, cq);
					
					if (!r.isEmpty()) {
						//find missings in PI
						Model piInteresting = extractPartialsInPI(paths, optpaths, askPaths, r, interest);
						if(!piInteresting.isEmpty()){
							interestingTriples.add(piInteresting);
							model.remove(r);
							continue;
						}
						
						// find partially matching from PI and check the rest
						// from target
						List<TriplePath> askDiff = new ArrayList<TriplePath>();
						askDiff.addAll(paths);
						askDiff.removeAll(askPaths);
						for (int j = askDiff.size() - 1; j > 0; j--) {
							List<Query> consQueries = QueryDecomposer.composeConstructQueries(askDiff, j);

							for (Query qc : consQueries) {
								List<TriplePath> diffAndAsk = new ArrayList<TriplePath>();
								diffAndAsk.addAll(askPaths);
								diffAndAsk.addAll(QueryPatternExtractor	.getBGPTriplePaths(qc));

								Model piR = extractPartialsInPI(diffAndAsk,	optpaths, askPaths, r, interest);
								if (piR.isEmpty()) {
									piR.add(r);
									diffAndAsk = askPaths;
								}
								Model rwithMissingTriples = extractMissingFromTarget(paths, optpaths, diffAndAsk, piR,	interest);
								if (!rwithMissingTriples.isEmpty()) {
									logger.info("Missing triples Matching found from target!");
									rwithMissingTriples.write(System.out, "N-TRIPLE");
									interestingTriples.add(rwithMissingTriples);
								} else {
									logger.info("Missing triples for added triples Matching not found from target!");
									potentiallyInterestingTriples.add(r);
								}
							}
						}

					}
					model.remove(r);
				} else {
					logger.info("Triples not found for Asked query patterns");
				}
			}
		}
		//only optionals
		for(TriplePath tp: optpaths){
			Query oq = QueryDecomposer.toConstructQuery(tp);
			Model r = SPARQLExecutor.executeConstruct(model, oq);
			if(!r.isEmpty()){
				potentiallyInterestingTriples.add(r);
				model.remove(r);
			}
		}
		List<Model> result = new ArrayList<Model>();
		result.add(interestingTriples);
		result.add(potentiallyInterestingTriples);
		return result;
	}

	
	private Model extractMissingFromTarget(List<TriplePath> paths, List<TriplePath> optpaths, List<TriplePath> askPaths, Model r, final Interest interest) {

		Query matchingQuery = QueryDecomposer.toSelectQuery(askPaths);
		// construct a Construct query of the overall interest expression query
		// with bounded VALUES of the matching Model		
		Query boundQuery = bindValues(paths, optpaths, matchingQuery, r);
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
	
	
	private List<Model> getMissingsFromPI(List<TriplePath> askPaths, Model partialMatch, Interest interest){
		List<Model> missingResults = new ArrayList<Model>();
				
		List<TriplePath>  paths = interest.getTriplePaths();
		List<TriplePath>  optpaths = interest.getOptionalTriplePaths();
		
		List<TriplePath> missingPaths = new ArrayList<TriplePath>();
		missingPaths.addAll(paths);
		missingPaths.removeAll(askPaths);
		logger.info("missing triples");
		for(TriplePath t: missingPaths){
			logger.info(t.toString());
		}
		for(int i= missingPaths.size(); i>0; i--){
			List<List<TriplePath>> combinations = new ArrayList<List<TriplePath>>();
			QueryDecomposer.combineTriples(missingPaths, i, 0, combinations, new TriplePath[i],0);
		//	List<Query> askQueries = QueryDecomposer.composeAskQueries(missingPaths, i);
			logger.info(i + " patterns combined");
			int x=0;
			for (List<TriplePath> tp : combinations) {
				
				logger.info("combination: " + ++x);
				for(TriplePath t: tp){
					logger.info(x+ " - "+t.toString());
				}
				Query select = QueryDecomposer.toSelectQuery(askPaths);
				tp.addAll(askPaths);
				Query boundQuery = bindValues(tp, optpaths, select, partialMatch);
				logger.info("Bound query:\n" + boundQuery);
				
				Model m = ModelFactory.createDefaultModel();
				m = SPARQLExecutor.executeConstruct(Global.PI_SPARQL_ENDPOINT, boundQuery);
				if(!m.isEmpty()){
					logger.info("matching:");				
					m.write(System.out, "N-TRIPLE");
				}else
					logger.info("NOTHING MATCHES");
				missingResults.add(m);
			}						
		}
		
		return missingResults;
	}
	private Model getMissingsFromTarget(){
		Model m = ModelFactory.createDefaultModel();
		
		return m;
	}
}
