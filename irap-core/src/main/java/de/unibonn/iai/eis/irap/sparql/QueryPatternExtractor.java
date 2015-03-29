/**
 * 
 */
package de.unibonn.iai.eis.irap.sparql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementVisitorBase;
import com.hp.hpl.jena.sparql.syntax.ElementWalker;

/**
 * @author keme686
 *
 */
public class QueryPatternExtractor {

	/**
	 * extracts list of triple paths of a query without considering the constructs used in query block
	 * 
	 * @param query A SPARQL query to which this method extracts triple paths.
	 * @return List of TriplePath elements of the query
	 */
	public static List<TriplePath> getBGPTriplePaths(Query query){
		final List<TriplePath> paths= new ArrayList<TriplePath>();
		ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase(){
			@Override
			public void visit(ElementPathBlock el) {
				ListIterator<TriplePath> lit = el.getPattern().iterator();
				while(lit.hasNext()){
					TriplePath tp = lit.next();
					paths.add(tp);
				}
			}
			/*@Override
			public void visit(ElementSubQuery el) {
				ElementWalker.walk(el.getQuery().getQueryPattern(), this);
			}*/
		});
		return paths;
	}
	
	/**
	 * get list of triple paths with optional patterns
	 * @param query
	 * @return
	 */
	public static List<TriplePath> geTriplePathsWithtOptionals(Query query){
		final List<TriplePath> paths= new ArrayList<TriplePath>();
		ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase(){
		
			@Override
			public void visit(ElementOptional el) {
				ElementWalker.walk(el.getOptionalElement(), new ElementVisitorBase(){
					@Override
					public void visit(ElementPathBlock el) {
						ListIterator<TriplePath> lit = el.getPattern().iterator();
						while(lit.hasNext()){
							TriplePath tp = lit.next();
							paths.add(tp);
						}
					}
				});
			}					
		});
		return paths;
	}
	/**
	 * extract list of variables used in the query patterns of a query and return a distinct list of variables
	 * @param query A query to which the method extract list of variables
	 * @return List of variables in the query pattern.
	 */
	public static List<Var> getQueryPatternVars(Query query){
		List<TriplePath> paths = getBGPTriplePaths(query);
		Set<Var> varSets = new HashSet<Var>();
		for(TriplePath tp: paths){
			if(tp.getSubject().isVariable()){
				varSets.add((Var)tp.getSubject());
			}
			if(tp.getPredicate().isVariable()){
				varSets.add((Var)tp.getPredicate());
			}
			if(tp.getObject().isVariable()){
				varSets.add((Var)tp.getObject());
			}
		}
		List<Var> vars = new ArrayList<Var>(varSets);
		return vars;
	}
	
	/**
	 * 
	 * @param triplePath
	 * @return
	 */
	public static String generateHashForCache(TriplePath triplePath) {
		String subject = toStringForHashForCache(triplePath.getSubject());
		String predicate =   toStringForHashForCache(triplePath.getPredicate());
		String object = toStringForHashForCache(triplePath.getObject());
		return subject+" "+predicate+" "+object;
	}
	
	/**
	 * 
	 * @param node
	 * @return
	 */
	public static String toStringForHashForCache(Node node) {
		
		if(node.isURI() ) {
			return "<"+node.toString()+">";
		} else if(node.isBlank()) {
			return "[]";
		} else if(node.toString().startsWith("??")) {
			return "[]";
		} else if(node.isVariable()) {
			return "?";
		} else if(node.isLiteral()) {
			return "v";
		}
		return "o";
	}
	

}
