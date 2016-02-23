/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2012
 */  
package ir;

import java.util.*;
import java.io.*;

public class PageRank{

    /**  
     *   Maximal number of documents. We're assuming here that we
     *   don't have more docs than we can keep in main memory.
     */
    final static int MAX_NUMBER_OF_DOCS = 2000000;
   
    /**
     * Probability of a doc without any links, its just equal probabilities of all docs
     */
    public Double noLinksDocProb = 0.0;
    
    /**
     * Probability of getting bored at a doc and moving somewhere randomly
     */
    public Double boredProbability = 0.0;

    /**
     *   Mapping from document names to document numbers.
     */
    Hashtable<String,Integer> docNumber = new Hashtable<String,Integer>();

    /**
     *   Mapping from document numbers to document names
     */
    String[] docName = new String[MAX_NUMBER_OF_DOCS];

    /**  
     *   A memory-efficient representation of the transition matrix.
     *   The outlinks are represented as a Hashtable, whose keys are 
     *   the numbers of the documents linked from.<p>
     *
     *   The value corresponding to key i is a Hashtable whose keys are 
     *   all the numbers of documents j that i links to.<p>
     *
     *   If there are no outlinks from i, then the value corresponding 
     *   key i is null.
     */
    Hashtable<Integer,Hashtable<Integer,Double>> link = new Hashtable<Integer,Hashtable<Integer,Double>>();

    /**
     *   The number of outlinks from each node.
     */
    int[] out = new int[MAX_NUMBER_OF_DOCS];

    /**
     *   The number of documents with no outlinks.
     */
    int numberOfSinks = 0;

    /**
     *   The probability that the surfer will be bored, stop
     *   following links, and take a random jump somewhere.
     */
    final static double BORED = 0.5;

    /**
     *   Convergence criterion: Transition probabilities do not 
     *   change more that EPSILON from one iteration to another.
     */
    final static double EPSILON = 0.0001;

    /**
     *   Never do more than this number of iterations regardless
     *   of whether the transistion probabilities converge or not.
     */
    final static int MAX_NUMBER_OF_ITERATIONS = 1000;

    
    /* --------------------------------------------- */


    public PageRank( String filename ) throws Exception {
	int noOfDocs = readDocs( filename );
	computeTransitionProbabilities( noOfDocs );
	computePageRank( noOfDocs );
    }


    /* --------------------------------------------- */


	/**
     *   Reads the documents and creates the docs table. When this method 
     *   finishes executing then the @code{out} vector of outlinks is 
     *   initialised for each doc, and the @code{p} matrix is filled with
     *   zeroes (that indicate direct links) and NO_LINK (if there is no
     *   direct link. <p>
     *
     *   @return the number of documents read.
     */
    int readDocs( String filename ) {
	int fileIndex = 0;
	try {
	    System.err.print( "Reading file... " );
	    BufferedReader in = new BufferedReader( new FileReader( filename ));
	    String line;
	    while ((line = in.readLine()) != null && fileIndex<MAX_NUMBER_OF_DOCS ) {
		int index = line.indexOf( ";" );
		String title = line.substring( 0, index );
		Integer fromdoc = docNumber.get( title );
		//  Have we seen this document before?
		if ( fromdoc == null ) {	
		    // This is a previously unseen doc, so add it to the table.
		    fromdoc = fileIndex++;
		    docNumber.put( title, fromdoc );
		    docName[fromdoc] = title;
		}
		// Check all outlinks.
		StringTokenizer tok = new StringTokenizer( line.substring(index+1), "," );
		while ( tok.hasMoreTokens() && fileIndex<MAX_NUMBER_OF_DOCS ) {
		    String otherTitle = tok.nextToken();
		    Integer otherDoc = docNumber.get( otherTitle );
		    if ( otherDoc == null ) {
			// This is a previousy unseen doc, so add it to the table.
			otherDoc = fileIndex++;
			docNumber.put( otherTitle, otherDoc );
			docName[otherDoc] = otherTitle;
		    }
		    // Set the probability to 0 for now, to indicate that there is
		    // a link from fromdoc to otherDoc.
		    if ( link.get(fromdoc) == null ) {
			link.put(fromdoc, new Hashtable<Integer,Double>());
		    }
		    if ( link.get(fromdoc).get(otherDoc) == null ) {
			link.get(fromdoc).put( otherDoc, 0.0 );
			out[fromdoc]++;
		    }
		}
	    }
	    if ( fileIndex >= MAX_NUMBER_OF_DOCS ) {
		System.err.print( "stopped reading since documents table is full. " );
	    }
	    else {
		System.err.print( "done. " );
	    }
	    // Compute the number of sinks.
	    for ( int i=0; i<fileIndex; i++ ) {
		if ( out[i] == 0 )
		    numberOfSinks++;
	    }
	}
	catch ( FileNotFoundException e ) {
	    System.err.println( "File " + filename + " not found!" );
	}
	catch ( IOException e ) {
	    System.err.println( "Error reading file " + filename );
	}
	System.err.println( "Read " + fileIndex + " number of documents" );
	return fileIndex;
    }


    /* --------------------------------------------- */


    /*
     *   Computes the pagerank of each document.
     */
    void computeTransitionProbabilities( int numberOfDocs ) throws Exception {

    	// 'if a row of A has no 1s, replace each element by 1/N'
    	this.noLinksDocProb = 1.0 / numberOfDocs;
    	
    	this.boredProbability = BORED / numberOfDocs;
    	
    	// calculate the page rank for every doc
    	for(Map.Entry<Integer, Hashtable<Integer,Double>> map : this.link.entrySet()){
    		Hashtable<Integer,Double> docLinks = map.getValue();

    		// 'divide each 1 in A by the number of 1s in its row'
    		Double linkProb = 1.0 / docLinks.size();
    		
    		// 'multiply the resulting A by 1 - alpha'	
    		linkProb *= (1.0 - BORED);
    		
    		// 'add alpha / N to every entry of the resulting matrix'
    		linkProb += this.boredProbability;
    		
    		for(Map.Entry<Integer, Double> map2 : docLinks.entrySet()){
    			Integer docId = map2.getKey();
    			docLinks.put(docId, linkProb);
    		}
    		
    		Double totalBoredProb = this.boredProbability * (numberOfDocs - (docLinks.size()));
    		Double totalLinkProb = linkProb * (docLinks.size());
    		
    		if(almostEqual(1.0, totalBoredProb + totalLinkProb, 0.000001) == false)
    			throw new Exception("Assertion failed!");
    	}
    	
    }



    private void computePageRank(Integer numberOfDocs) throws Exception {
		ArrayList<PostingsEntry> docs = new ArrayList<PostingsEntry>(numberOfDocs);
		ArrayList<Double> currState = new ArrayList<Double>(numberOfDocs);
		Integer iter = 0;
		
		// initial state prob
		for(Integer docId = 0; docId < numberOfDocs; docId++){
			PostingsEntry pe = new PostingsEntry();
			pe.docID = docId;
			pe.score = 0.0;
			docs.add(pe);
			currState.add(docId, 0.0);
		}
		currState.set(0, 1.0);
		
		while(iter < 1){
			Double totalSum = 0.0;			
			
			for(Integer i=0; i < numberOfDocs; i++){
				Double sum = 0.0;
				
				for(Integer j=0; j < numberOfDocs; j++){					
					Double currStateProb = currState.get(j);					
					
					Hashtable<Integer, Double> probs = link.get(j);
					Double transProb = 0.0;
					if(probs == null){
						transProb = this.noLinksDocProb;
					}else if(probs.containsKey(i)){
						transProb = probs.get(i);
					}else
						transProb = this.boredProbability;
						
					sum += currStateProb * transProb;
				}
				docs.get(i).score = sum;
				totalSum += sum;				
			}
			
			
			if(this.almostEqual(totalSum, 1.0, 0.000001) == false)
				throw new Exception("Assertion failed!");
				
			for(PostingsEntry doc : docs){
				currState.set(doc.docID, doc.score);
			}
			
			iter++;
			System.out.println("Iteration " + iter);
		}
    	
		Collections.sort(docs, PostingsEntry.SCORE_ORDER);
		Integer count = 50;
		for(PostingsEntry pe : docs){
			System.out.println("doc [" + this.docName[pe.docID] + "] = ["+ pe.score +"]");
			if(count == 0)
				break;
			count--;
		}
		
    	System.err.println("Finished!");
		
	}
    

    /* --------------------------------------------- */


    public static void main( String[] args ) throws Exception {
	if ( args.length != 1 ) {
	    System.err.println( "Please give the name of the link file" );
	}
	else {
	    new PageRank( args[0] );
	}
    }
    
    public boolean almostEqual(double a, double b, double eps){
    	return Math.abs(a-b)<eps;
    }
}
