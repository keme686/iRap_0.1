/**
 * 
 */
package de.unibonn.iai.eis.irap.model;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;


/**
 * @author keme686
 *
 */
public class Changeset {
	
	/**
	 * Changeset base filder URI
	 */
	private String uri;
	/**
	 * Sequence number of a changeset
	 */
	private String sequenceNum;
	/**
	 * removed triples from source dataset
	 */
	private Model removedTriples;
	/**
	 * added triples to the source dataset
	 */
	private Model addedTriples;
	
	
	public Changeset() {
		removedTriples = ModelFactory.createDefaultModel();
		addedTriples = ModelFactory.createDefaultModel();
	}
	
	public Changeset(String uri, String sequenceNum) {
		this.uri = uri;
		this.sequenceNum = sequenceNum;
		this.removedTriples = ModelFactory.createDefaultModel();
		this.addedTriples = ModelFactory.createDefaultModel();
	}
	
	public Changeset(String uri, Model removed, Model added, String sequenceNum){
		this.uri = uri;
		this.removedTriples = removed;
		this.addedTriples = added;
		this.sequenceNum = sequenceNum;
	}

	
	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getSequenceNum() {
		return sequenceNum;
	}

	public void setSequenceNum(String sequenceNum) {
		this.sequenceNum = sequenceNum;
	}

	public Model getRemovedTriples() {
		return removedTriples;
	}

	public void setRemovedTriples(Model removedTriples) {
		this.removedTriples = removedTriples;
	}

	public Model getAddedTriples() {
		return addedTriples;
	}

	public void setAddedTriples(Model addedTriples) {
		this.addedTriples = addedTriples;
	}
	
	

}
