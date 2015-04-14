/**
 * 
 */
package de.unibonn.iai.eis.irap;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;

import de.unibonn.iai.eis.irap.evaluator.EvaluationManager;
import de.unibonn.iai.eis.irap.helper.Global;
import de.unibonn.iai.eis.irap.helper.JenaModelUtils;
import de.unibonn.iai.eis.irap.model.Changeset;
import de.unibonn.iai.eis.irap.sparql.QueryDecomposer;
import de.unibonn.iai.eis.irap.sparql.SPARQLExecutor;

/**
 * @author keme686
 *
 */
public class MainSync {

	 private static final Logger logger = LoggerFactory.getLogger(MainSync.class);
	
	 //private static final String LAST_DOWNLOAD = "lastDownloadDate.dat";
	 
	 //private static final String CHANGESET_DOWNLOAD_FOLDER = "data";
	 
	 /**
	 * @param args
	 */
	 public static void main(String[] args) {
		
			
		start();
	 }

	 private static void start(){
		 logger.info("Changeset counter starting at 0");
		 int lastChangeCount =0;
		//6 digit seq and type => 000000.added.nt or 000000.removed.nt
		
		String changesetDownloadFolder = "data/2014-10-02-21";
		File chfolder = new File(changesetDownloadFolder);
		if(!chfolder.isDirectory()){
			logger.debug("Invalid changeset folder name!");
			return;
		}
		
		emptyEndpoints();
		
		while(true){		
			int count = lastChangeCount;
			Collection<String> changesets = Arrays.asList(chfolder.list());
			while(changesets.size() > 0){
				String changesetAdded = getSequence(count) + ".added.nt";
				String changesetRemoved = getSequence(count) + ".removed.nt";
				if(!changesets.contains(changesetAdded) || !changesets.contains(changesetRemoved)){
					lastChangeCount = count;
					break;
				}
				Model addedTriples = ModelFactory.createDefaultModel();
				Model removedTriples = ModelFactory.createDefaultModel();
				logger.info("Reading added triples file: " + changesetAdded);
				if(changesets.contains(changesetAdded)){
					addedTriples = JenaModelUtils.readModel(changesetDownloadFolder+"/" + changesetAdded);
				}
				logger.info("Reading removed triples file:" + changesetRemoved);
				if(changesets.contains(changesetRemoved)){
					removedTriples = JenaModelUtils.readModel(changesetDownloadFolder+"/"+ changesetRemoved);
				}
				
				//System.out.println(changesetAdded + " => "+changesetRemoved);
				logger.info("Creating changeset object");
				Changeset changeset = new Changeset("http://live.dbpedia.org/changesets", removedTriples, addedTriples, getSequence(count));
				EvaluationManager evaluator = new EvaluationManager(changeset);
				logger.info("Starting evaluation...");
				evaluator.start();
				count++;
			}				
			//sleep for 2 sec before reading the next changeset
			try{
				Thread.sleep(2000);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	
	 }
	 private static String getSequence(int seq){
		 String sequence = "";
		 if(seq < 10){
			 sequence = "00000" + seq;
		 }else if(seq < 100){
			 sequence = "0000" + seq;
		 }else if(seq < 1000){
			 sequence = "000" + seq;
		 }else if(seq < 10000){
			 sequence = "00" + seq;
		 }else if(seq < 100000){
			 sequence = "0" + seq;
		 }else
			 sequence = "" + seq;
					 
		 return sequence;
	 }
	 
	 private static void emptyEndpoints(){
		 String target="http://localhost:3030/target/sparql";
		 String pi= Global.PI_SPARQL_ENDPOINT;
		 String consQ = "CONSTRUCT {?s ?p ?o} where {?s ?p ?o}";
		 Query tconst = QueryFactory.create(consQ);
		 tconst.setQueryConstructType();
		 Model targetData  = SPARQLExecutor.executeConstruct(target, tconst);
		 System.out.println("Target Data: \n ========================================================================================");
		 //targetData.write(System.out, "N-TRIPLE");
		 
		 StringBuilder targetQuery = QueryDecomposer.toUpdate(targetData, false);
		 
		 boolean targetR = SPARQLExecutor.executeUpdate( "http://localhost:3030/target/update", targetQuery.toString());
		
		 if(targetR){
			 System.out.println("Target dataset cleaned!");
		 }else{
			 System.out.println("cannot clean target dataset!");
		 }
		 String piconst = "CONSTRUCT {?s ?p ?o} where {GRAPH ?g {?s ?p ?o}}";
		 Query picon = QueryFactory.create(piconst);
		 //picon.setQueryConstructType();
		 Model piData = SPARQLExecutor.executeConstruct(pi, picon);
		 System.out.println("PI Data: \n ========================================================================================");
		// piData.write(System.out, "N-TRIPLE");
		 StringBuilder piQuery = QueryDecomposer.toUpdate(piData,"http://eis.iai.uni-bonn.de/irap/PI/i0001" , false);
		 
		 boolean piR = SPARQLExecutor.executeUpdate("http://localhost:3030/irap/update", piQuery.toString());
		 if(piR){
			 System.out.println("PI dataset cleaned!");
		 }
		 else{
			 System.out.println("Cannot clean PI dataset!");
		 }
	 }
}
