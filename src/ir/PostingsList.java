/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 */  

package ir;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import java.io.Serializable;

/**
 *   A list of postings for a given word.
 */
public class PostingsList implements Serializable, Iterable<PostingsEntry>{
    
    /** The postings list as a linked list. */
    private LinkedList<PostingsEntry> list = new LinkedList<PostingsEntry>();
	//private ArrayList<PostingsEntry> list = new ArrayList<PostingsEntry>();

    /**  Number of postings in this list  */
    public int size() {
	return list.size();
    }

    /**  Returns the ith posting */
    public PostingsEntry get( int i ) {
	return list.get( i );
    }

    public void add(PostingsEntry posting){
    	
    	ListIterator li = list.listIterator(list.size());

    	// check if the posting already exists in the list
    	while(li.hasPrevious()) {
    		PostingsEntry p = (PostingsEntry) li.previous();    		
    		if(p.docID < posting.docID)
    			break;
    		
    		if(p.docID == posting.docID){
    			p.score++;
    			p.positions.addAll(posting.positions);
    			return;
    		}
    	}
    	
    	// add new posting to the list
    	this.list.add(posting);
    	//Collections.sort(this.list);
    }

	@Override
	public Iterator<PostingsEntry> iterator() {
		return list.iterator();
	}
}
	

			   
