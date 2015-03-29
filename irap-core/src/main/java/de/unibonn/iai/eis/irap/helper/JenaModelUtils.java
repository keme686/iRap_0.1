/**
 * 
 */
package de.unibonn.iai.eis.irap.helper;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * @author keme686
 *
 */
public class JenaModelUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

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
