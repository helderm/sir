/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Hedvig Kjellström, 2012
 */  

package ir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.StringTokenizer;

public class Query {
    
    public LinkedList<String> terms = new LinkedList<String>();
    public LinkedList<Double> weights = new LinkedList<Double>();

    /**
     *  Creates a new empty Query 
     */
    public Query() {
    }
	
    /**
     *  Creates a new Query from a string of words
     */
    public Query( String queryString  ) {
	StringTokenizer tok = new StringTokenizer( queryString );
	while ( tok.hasMoreTokens() ) {
	    terms.add( tok.nextToken() );
	    weights.add( new Double(1) );
	}    
    }
    
    /**
     *  Returns the number of terms
     */
    public int size() {
	return terms.size();
    }
    
    /**
     *  Returns a shallow copy of the Query
     */
    public Query copy() {
	Query queryCopy = new Query();
	queryCopy.terms = (LinkedList<String>) terms.clone();
	queryCopy.weights = (LinkedList<Double>) weights.clone();
	return queryCopy;
    }
    
    /**
     *  Expands the Query using Relevance Feedback
     */
    public void relevanceFeedback( PostingsList results, boolean[] docIsRelevant, Indexer indexer ) {
    	// results contain the ranked list from the current search
    	// docIsRelevant contains the users feedback on which of the 10 first hits are relevant
	
	    double alpha = 0.5;
	    double beta = 0.5;
	    int maxNewTerms = 20;	    	
	    int i = 0;
		
	    // get the size of the query as if it were a doc
	    Double querySize = 0.0;
	    for(Double w : this.weights)
	    	querySize += w;
	    
	    // recalculate the query weights as tf-idf scores
	    LinkedList<String> newQueryTerms = new LinkedList<String>();
	    LinkedList<Double> newQueryWeights = new LinkedList<Double>();
	    
	    for(i=0; i<this.terms.size(); i++){
	    	String term = this.terms.get(i);
	    	Double tf = this.weights.get(i);
	    	
    		PostingsList postings = indexer.index.getPostings(term);
	    		
    		Double idf = indexer.corpus.idf(postings);
    		
    		// set the new score for the query term
    		Double score = tf * idf;
    		score = (score / Math.sqrt(querySize)) * alpha;    		
    		
    		newQueryTerms.add(term);
    		newQueryWeights.add(score);
    	}
	    
	    // get the scores for every term in every relevant document
	    int numRelevantDocs = 0;	    
	    ArrayList<Query.TermWeight> docTerms = new ArrayList<Query.TermWeight>();	    
	    for(i=0; i<docIsRelevant.length; i++){
	    	if(docIsRelevant[i] == false)
	    		continue;
	    	
	    	numRelevantDocs++;
	    	
	    	PostingsEntry pe = results.get(i);
	    	CorpusDocument doc = indexer.corpus.getDocument(pe.docID);
	    	
	    	// add the tf-idf for that term / doc to the new modified query
	    	for(PostingsTermEntry pte : doc.terms){
	    		
	    		// search for this term in the query
	    		int j;
	    		boolean found = false;
	    		for(j=0; j<docTerms.size(); j++){
	    			Query.TermWeight dt = docTerms.get(j);
	    			
	    			if(dt.term.equals(pte.term)){
	    				dt.weight += pte.score;
	    				
	    				found = true;
		    			break;
	    			}
	    		}
	    		
	    		// if not found, add new term
	    		if(found)
	    			continue;
	    		
	    		Query.TermWeight dt = new Query.TermWeight();
	    		dt.term = pte.term;
	    		dt.weight = pte.score;	    		
	    		docTerms.add(dt);
	    	}
	    	
		}
	    
	    // sort, truncate, rescore
	    Collections.sort(docTerms);
	    int maxSize = maxNewTerms < docTerms.size() ? maxNewTerms : docTerms.size();
	    docTerms = new ArrayList<Query.TermWeight>(docTerms.subList(0, maxSize ));
	    for(Query.TermWeight qt : docTerms){
	    	qt.weight = (qt.weight / numRelevantDocs) * beta;
	    }
	    
	    // merge the query with the new terms
	    for(Query.TermWeight dt : docTerms){
	    	
	    	boolean found = false;
	    	for(i=0; i<newQueryTerms.size(); i++){
	    		if(dt.term.equals(newQueryTerms.get(i))){
	    			Double score = newQueryWeights.get(i);
	    			score += dt.weight;
	    			newQueryWeights.set(i, score);
	    			found = true;
	    			break;
	    		}
	    	}
	    	
	    	if(found)
	    		continue;
	    	
	    	newQueryTerms.add(dt.term);
	    	newQueryWeights.add(dt.weight);
	    }
	
	    // switch to new query terms and weights
	    this.terms = newQueryTerms;
	    this.weights = newQueryWeights;	    
    }
    
    public class TermPostings implements Comparable<TermPostings>{
    	public String term;
    	public Double weight;
    	public PostingsList postings;
		
		@Override
		public int compareTo(TermPostings o) {
			return this.postings.size() - o.postings.size();
		}
    }
    
    private class TermWeight implements Comparable<TermWeight>{
    	public String term;
    	public Double weight;
		
    	@Override
		public int compareTo(TermWeight o) {
    		return Double.compare( o.weight, this.weight );
		}
    	
    }
}

    
