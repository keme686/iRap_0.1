/**
 * 
 */
package de.unibonn.iai.eis.irap.sparql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.Template;

/**
 * @author keme686
 *
 */
public class QueryDecomposer {

	/**
	 * compose an ASK query from a single triple path
	 * 
	 * @param path
	 * @return
	 */
	public static Query toAskQuery(TriplePath path){
		ElementPathBlock block = new ElementPathBlock();
		block.addTriple(path);
		
		ElementGroup group = new ElementGroup();
		group.addElement(block);
		
		Query query = QueryFactory.make();
		query.setQueryAskType();
		query.setQueryPattern(group);
		return query;
	}
	
	/**
	 * compose an ASK query from a list of triple paths
	 * 
	 * @param paths
	 * @return
	 */
	public static Query toAskQuery(List<TriplePath> paths){
		ElementPathBlock block = new ElementPathBlock();
		for(TriplePath path: paths)
			block.addTriple(path);
		
		ElementGroup group = new ElementGroup();
		group.addElement(block);
		
		Query query = QueryFactory.make();
		query.setQueryAskType();
		query.setQueryPattern(group);
		return query;
	}
	
	/**
	 * compose a list of ASK queries by combining  <code>length</code> triple paths from the larger paths list
	 * 
	 * @param paths list of TriplePath objects
	 * @param length length number of triple path(s) to be used for composing a combination from list of paths
	 * @return list of SPARQL ASK queries 
	 */
	public static List<Query> composeAskQueries(List<TriplePath> paths,	int length) {
		if (length <= 0) {
			length = 1;
		}
		List<List<TriplePath>> combinations = new ArrayList<List<TriplePath>>();
		combineTriples(paths, length, 0, combinations, new TriplePath[length],	0);
		List<Query> askQueries = new ArrayList<Query>();

		for (List<TriplePath> tp : combinations) {
			askQueries.add(toAskQuery(tp));
		}
		return askQueries;
	}	
	/**
	 * compose a list of CONSTRUCT queries by combining the a smaller number of triple paths from the larger paths list
	 *  
	 * @param paths  list of TriplePath objects
	 * @param length number of triple path(s) to be used for composing a combination from list of paths
	 * @return list of SPARQL CONSTRUCT queries 
	 */
	public static List<Query> composeConstructQueries(List<TriplePath> paths,	int length) {
		if (length <= 0) {
			length = 1;
		}
		List<List<TriplePath>> combinations = new ArrayList<List<TriplePath>>();
		combineTriples(paths, length, 0, combinations, new TriplePath[length], 0);
		List<Query> askQueries = new ArrayList<Query>();

		for (List<TriplePath> tp : combinations) {
			askQueries.add(toConstructQuery(tp));
		}
		return askQueries;
	}	
	
	private static int combineTriples(List<TriplePath> paths, int length, int startPosition, List<List<TriplePath>> result,	TriplePath[] partialRes, int count) {
		if (length == 0) {
			List<TriplePath> triples = new ArrayList<TriplePath>();
			for (TriplePath p : partialRes) {
				triples.add(p);
			}
			result.add(triples);

			return ++count;
		}
		for (int i = startPosition; i <= paths.size() - length; i++) {
			partialRes[partialRes.length - length] = paths.get(i);
			count = combineTriples(paths, length - 1, i + 1, result,
					partialRes, count);
		}
		return count;
	}
	
	/**
	 * compose a CONSTRUCT query from a single triple path
	 * 
	 * @param path a TriplePath object
	 * @return a CONSTRUCT sparql query composed by using the given path object as a template and query pattern
	 */
	public static Query toConstructQuery(TriplePath path){
		List<TriplePath> paths = new ArrayList<TriplePath>();
		paths.add(path);
		return toConstructQuery(paths);
	}
	/**
	 * compose a CONSTRUCT query from a list of triple paths
	 * 
	 * @param paths list of TriplePath objects
	 * @return a CONSTRUCT sparql query composed by using the given list of paths as a template and query pattern
	 */
	public static Query toConstructQuery(List<TriplePath> paths){
		ElementPathBlock block = new ElementPathBlock();
		List<Triple> triples = new ArrayList<Triple>();
		for(TriplePath path: paths){
			block.addTriple(path);
			triples.add(path.asTriple());
		}	
		ElementGroup group = new ElementGroup();
		group.addElement(block);
		
		Query decomposedQuery = QueryFactory.make();
		decomposedQuery.setQueryConstructType();
		BasicPattern bgp = BasicPattern.wrap(triples);
		Template templ = new Template(bgp);
		
		decomposedQuery.setQueryPattern(group);
		decomposedQuery.setConstructTemplate(templ);
		decomposedQuery.setResultVars();
		
		return decomposedQuery;
	}
	/**
	 * Compose CONSTRUCT query from a list of triple paths on a Named graph
	 * 
	 * @param paths
	 * @param graph named graph on which the construct query will be evaluated
	 * 
	 * @return 
	 */
	public static Query toConstructQuery(List<TriplePath> paths, String graph){
		ElementPathBlock block = new ElementPathBlock();
		List<Triple> triples = new ArrayList<Triple>();
		for(TriplePath path: paths){
			block.addTriple(path);
			triples.add(path.asTriple());
		}	
		ElementGroup group = new ElementGroup();
		group.addElement(block);
		
		Query decomposedQuery = QueryFactory.make();
		decomposedQuery.setQueryConstructType();
		BasicPattern bgp = BasicPattern.wrap(triples);
		Template templ = new Template(bgp);
		
		Node n =ResourceFactory.createResource(graph).asNode();
		ElementNamedGraph ng = new ElementNamedGraph(n, group);
			
		decomposedQuery.setQueryPattern(ng);
		decomposedQuery.setConstructTemplate(templ);
		decomposedQuery.setResultVars();
		
		return decomposedQuery;
	}
	/**
	 * compose a SELECT query from a single triple path
	 * 
	 * @param path
	 * @return
	 */
	public static Query toSelectQuery(TriplePath path){
		List<TriplePath> paths = new ArrayList<TriplePath>();
		paths.add(path);
		return toSelectQuery(paths);
	}
	/**
	 * compose a SELECT query from a list of triple paths
	 * 
	 * @param paths
	 * @return
	 */
	public static Query toSelectQuery(List<TriplePath> paths){
		ElementPathBlock block = new ElementPathBlock();
		List<Triple> triples = new ArrayList<Triple>();
		for(TriplePath path: paths){
			block.addTriple(path);
			triples.add(path.asTriple());
		}	
		
		ElementGroup group = new ElementGroup();
		group.addElement(block);
		
		Query decomposedQuery = QueryFactory.create("SELECT * {<http://example.com/s> <http://example.com/p> <http://example.com/o>}");	
		decomposedQuery.setQueryPattern(group);
		decomposedQuery.setResultVars();
		decomposedQuery.setQuerySelectType();
		//tQuery.addResultVar("*");			
		return decomposedQuery;
	}
	
	public static StringBuffer toUpdate(Model model, String graph, boolean toInsert){
		StringBuffer queryBuff = new StringBuffer(SPARQLExecutor.prefixes() +"  " + (toInsert? " INSERT DATA ": " DELETE DATA ") + " { GRAPH  <" + graph+ "> { " );
		//String query =SPARQLExecutor.prefixes() + (toInsert? " INSERT DATA ": " DELETE DATA ")+ "{ " ;
		StmtIterator iterator = model.listStatements();
		while(iterator.hasNext()){
			Statement stmt = iterator.nextStatement();
			Resource s = stmt.getSubject();
			queryBuff.append("  <"+ s.toString() +">  ");
			Property p = stmt.getPredicate();
			queryBuff.append(" <" + p.toString() + "> ");
			RDFNode o = stmt.getObject();
			if(o.isURIResource() ) {
				queryBuff.append(" <"+o.toString()+"> ");
			} else if(o.isAnon()) {
				queryBuff.append(o);
			} else {
				String l = o.toString();
				l = Matcher.quoteReplacement(l);
				queryBuff.append( "\""+ l +"\" ");
			}
			queryBuff.append(" .");
		}
		queryBuff.append(" } }");
		return queryBuff;
	}
	
	public static StringBuffer toUpdate(Model model, boolean toInsert){
		StringBuffer queryBuff = new StringBuffer(SPARQLExecutor.prefixes() +" " + (toInsert? " INSERT DATA ": " DELETE DATA ") + " {  " );
		//String query =SPARQLExecutor.prefixes() + (toInsert? " INSERT DATA ": " DELETE DATA ")+ "{ " ;
		StmtIterator iterator = model.listStatements();
		while(iterator.hasNext()){
			Statement stmt = iterator.nextStatement();
			Resource s = stmt.getSubject();
			queryBuff.append("  <"+ s.toString() +">  ");
			Property p = stmt.getPredicate();
			queryBuff.append(" <" + p.toString() + "> ");
			RDFNode o = stmt.getObject();
			if(o.isURIResource() ) {
				queryBuff.append(" <"+o.toString()+"> ");
			} else if(o.isAnon()) {
				queryBuff.append(o);
			} else {
				String l = o.toString();
				l = Matcher.quoteReplacement(l);
				queryBuff.append( "\""+ l +"\" ");
			}
			queryBuff.append(" . ");
		}
		queryBuff.append(" } ");
		return queryBuff;
	}
}
