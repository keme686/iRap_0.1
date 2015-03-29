/**
 * 
 */
package de.unibonn.iai.eis.irap.changeset;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.irap.helper.LoggerLocal;
import de.unibonn.iai.eis.irap.model.Changeset;

/**
 * @author keme686
 *
 */
public class ChangesetManager {
		
	private static Logger logger = LoggerLocal.getLogger(ChangesetManager.class.getName());
	
	/**
	 * creates a changeset object by reading added and removed triples from a given file name (and path)
	 * 
	 * @param uri
	 * @param removedFile
	 * @param addedFile
	 * @param timestamp
	 * 
	 * @return 
	 */
	public static Changeset createChangeset(String uri, String removedFile, String addedFile, String timestamp){
		Model removedTriples = ModelFactory.createDefaultModel();
		
		try{
			logger.info("Reading removed triples from: " + removedFile);
			removedTriples.read(removedFile);
		}catch(Exception e){
			e.printStackTrace();
			logger.debug("Cannot read removed triples from:" + removedFile);
			logger.debug(e.getMessage());
			return null;
		}
		
		Model addedTriples = ModelFactory.createDefaultModel();
		
		try{
			logger.info("Reading added triples from: " + addedFile);
			addedTriples.read(addedFile);
		}catch(Exception e){
			e.printStackTrace();
			logger.debug("Cannot read added triples from: " + addedFile);
			logger.debug(e.getMessage());
			return null;
			
		}
		Changeset changeset = new Changeset(uri, removedTriples, addedTriples, timestamp);
		logger.info("Changeset number " + timestamp + " from " + uri + " created!");
		return changeset;
	}

	/**
	 * Read RDF data  model 
	 * @param filename
	 * @return
	 */
	public static Model readModel(String filename) {
		Model model = ModelFactory.createDefaultModel();
		model.read(filename);
		return model;
	}
}
