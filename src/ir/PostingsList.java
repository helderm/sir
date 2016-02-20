/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 */  

package ir;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import java.io.Serializable;

/**
 *   A list of postings for a given word.
 */
public class PostingsList implements Serializable, Iterable<PostingsEntry>{
    
    /** The postings list as a linked list. */
    private LinkedList<PostingsEntry> list;
	//private ArrayList<PostingsEntry> list = new ArrayList<PostingsEntry>();
    
	public PostingsList() {
		list = new LinkedList<PostingsEntry>();
	}

	public PostingsList(List<PostingsEntry> subList) {
		list = new LinkedList<PostingsEntry>(subList);
	}
    
    /**  Number of postings in this list  */
    public int size() {
	return list.size();
    }

    /**  Returns the ith posting */
    public PostingsEntry get( int i ) {
	return list.get( i );
    }
    
    public PostingsList get(int i, int j){
    	return new PostingsList(list.subList(i, Math.min(j, list.size())));
    }

    public void add(PostingsEntry posting){
    	
    	ListIterator li = list.listIterator(list.size());

    	// check if the posting already exists in the list
    	while(li.hasPrevious()) {
    		PostingsEntry p = (PostingsEntry) li.previous();    		
    		if(p.docID < posting.docID)
    			break;
    		
    		if(p.docID == posting.docID){
    			// add the position to the list
    			for(Integer position : posting.positions){
    				if(p.positions.contains(position))
    					continue;
    				p.positions.add(position);
    			}
    			
    			// set the new score
    			p.score += posting.score;
    			
    			return;
    		}
    	}
    	
    	// add new posting to the list
    	this.list.add(posting);
    	Collections.sort(this.list);
    }

    public void add(PostingsList postings){    	
    	for(PostingsEntry posting : postings){
    		this.add(posting);
    	}
    }
    
	@Override
	public Iterator<PostingsEntry> iterator() {
		return list.iterator();
	}
	
	public LinkedList<PostingsEntry>getList(){
		//TODO: Lame, refactor that
		return list;
	}
}
	

			   
