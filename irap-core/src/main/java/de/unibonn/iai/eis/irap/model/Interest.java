/**
 * 
 */
package de.unibonn.iai.eis.irap.model;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;

import de.unibonn.iai.eis.irap.sparql.QueryPatternExtractor;


/**
 * @author keme686
 *
 */
public class Interest {

	public static final int SYNC_TYPE_FILE=0;
	public static final int SYNC_TYPE_SPARQL_ENDPOINT=1;
	public static final int TARGET_TYPE_FILE = 0;
	public static final int TARGET_TYPE_TDB = 0;
	public static final int TARGET_TYPE_ENDPOINT = 0;
	
	
	private String sourceUri;
	private String targetUri;
	private String targetUpdateUri;
	
	private Query query;
	private String changesetsUri;
	private String interestId;
	private String id;
	private String pigraph;
	private int targetType=1;
	private List<TriplePath> paths= new ArrayList<TriplePath>();
	private List<TriplePath> optPaths= new ArrayList<TriplePath>();
	
	public Interest(String uri) {
		this.sourceUri = uri;
	}

	public Interest(String uri, String changesetsUri){
		this.sourceUri = uri;
		this.changesetsUri = changesetsUri;
	}
	
	public Interest(String uri, String query, String changesetsUri){
		this.sourceUri = uri;
		this.changesetsUri = changesetsUri;
		this.query = QueryFactory.create(query);
		paths = QueryPatternExtractor.getBGPTriplePaths(this.query);
		optPaths = QueryPatternExtractor.geTriplePathsWithtOptionals(this.query);
	}
	
	public List<TriplePath> getTriplePaths(){
		return paths;
	}
	
	public List<TriplePath> getOptionalTriplePaths(){
		return optPaths;
	}
	
	public String getSourceUri() {
		return sourceUri;
	}
	
	public void setSourceUri(String uri) {
		this.sourceUri = uri;
	}
	
	public String getTargetUri() {
		return targetUri;
	}
	
	public void setTargetUri(String targetUri) {
		this.targetUri = targetUri;
	}
	
	public Query getQuery() {
		return query;
	}
	
	public void setQuery(Query query) {
		this.query = query;
		paths = QueryPatternExtractor.getBGPTriplePaths(this.query);
		optPaths = QueryPatternExtractor.geTriplePathsWithtOptionals(this.query);
	}
	
	public void setQuery(String queryString){
		this.query = QueryFactory.create(queryString);
		paths = QueryPatternExtractor.getBGPTriplePaths(this.query);
		optPaths = QueryPatternExtractor.geTriplePathsWithtOptionals(this.query);
	}
	
	public String getChangesetsUri() {
		return changesetsUri;
	}
	
	public void setChangesetsUri(String changesetsUri) {
		this.changesetsUri = changesetsUri;
	}
	
	public String getInterestId() {
		return interestId;
	}
	public void setInterestId(String interestId) {
		this.interestId = interestId;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPigraph() {
		return pigraph;
	}

	public void setPigraph(String pigraph) {
		this.pigraph = pigraph;
	}

	
	public int getTargetType() {
		return targetType;
	}

	public void setTargetType(int targetType) {
		this.targetType = targetType;
	}

	
	public String getTargetUpdateUri() {
		return targetUpdateUri;
	}

	public void setTargetUpdateUri(String targetUpdateUri) {
		this.targetUpdateUri = targetUpdateUri;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Interest){
			Interest interest = (Interest)obj;
			return (this.interestId.equals(interest.getInterestId()) );
		}else
			return super.equals(obj);
	}
	
	/**
	 * 
	 * @param interest
	 * @return
	 */
	public boolean equivalentTo(Interest interest){
		return (this.equalsQuery(interest));
	}
	
	private boolean equalsQuery(Interest interest){
		List<TriplePath> tps = interest.getTriplePaths();
		List<TriplePath> t = this.getTriplePaths();
		if(t.size() != tps.size())
			return false;
		//TODO: should not consider variable name differences and order of triple patterns
		boolean found = false;
		for(TriplePath tp: tps){
			for(TriplePath tt: t){
				if(tt.equals(tp)){
					found = true;
					break;
				}
			}		
		}
		if(!found){
			return false;
		}
		return true;
	}
}
