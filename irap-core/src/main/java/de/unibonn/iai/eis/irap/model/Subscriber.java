/**
 * 
 */
package de.unibonn.iai.eis.irap.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author keme686
 *
 */
public class Subscriber {

	private List<Interest> interests = new ArrayList<Interest>();
	private String outputUri;
	private int syncType;
	private String name;
	private String id;
	
	public Subscriber() {
	}
	
	public Subscriber(String id, String name){
		this.id = id;
		this.name = name;
	}
	public Subscriber(Interest interest, String outputUri, int syncType) {
		this.outputUri = outputUri;
		this.syncType = syncType;
		this.interests.add(interest);
	}
	
	public Subscriber(List<Interest> interests, String outputUri, int syncType){
		this.interests = interests;
		this.outputUri = outputUri;
		this.syncType = syncType;
	}
	
	public void addInterest(Interest interest){
		this.interests.add(interest);
	}
	
	public boolean removeInterest(Interest interest){
		return this.interests.remove(interest);
	}

	public List<Interest> getInterests() {
		return interests;
	}

	public void setInterests(List<Interest> interests) {
		this.interests = interests;
	}

	public String getOutputUri() {
		return outputUri;
	}

	public void setOutputUri(String outputUri) {
		this.outputUri = outputUri;
	}

	public int getSyncType() {
		return syncType;
	}

	public void setSyncType(int syncType) {
		this.syncType = syncType;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	
}
