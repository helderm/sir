package ir;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class PostingsTermsList implements Iterable<PostingsTermEntry>{
    /** The postings list as a linked list. */
    private LinkedList<PostingsTermEntry> list;
	//private ArrayList<PostingsEntry> list = new ArrayList<PostingsEntry>();
    
	public PostingsTermsList() {
		list = new LinkedList<PostingsTermEntry>();
	}

	public PostingsTermsList(List<PostingsTermEntry> subList) {
		list = new LinkedList<PostingsTermEntry>(subList);
	}
    
    /**  Number of postings in this list  */
    public int size() {
	return list.size();
    }

    /**  Returns the ith posting */
    public PostingsTermEntry get( int i ) {
	return list.get( i );
    }
    
    public PostingsTermsList get(int i, int j){
    	return new PostingsTermsList(list.subList(i, Math.min(j, list.size())));
    }

    public void add(PostingsTermEntry posting){
    	
    	ListIterator<PostingsTermEntry> li = list.listIterator();

    	// check if the posting already exists in the list
    	while(li.hasNext()) {
    		PostingsTermEntry p = li.next();    		
    		if (!p.term.equals(posting.term))
    			continue;
    		
			// set the new score
			p.score += posting.score;
			
			return;    		
    	}
    	
    	// add new posting to the list
    	this.list.add(posting);
    	
    }

    public void add(PostingsTermsList postings){    	
    	for(PostingsTermEntry posting : postings){
    		this.add(posting);
    	}
    }
    
	@Override
	public Iterator<PostingsTermEntry> iterator() {
		return list.iterator();
	}
	
	public LinkedList<PostingsTermEntry>getList(){
		//TODO: Lame, refactor that
		return list;
	}
}

