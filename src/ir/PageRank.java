/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2012
 */  
package ir;

import java.util.*;
import java.util.concurrent.Callable;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoDatabase;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;

public class PageRank implements Callable<ArrayList<CorpusDocument>>{

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
     *   Mapping from document numbers to document titles
     */
    String[] docTitle = new String[MAX_NUMBER_OF_DOCS];

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

	private String linksFilename;
	private String titlesFilename;

	private static final int SAVE_ITERS = 1000;
	
	private int type;

    /**
     *   The probability that the surfer will be bored, stop
     *   following links, and take a random jump somewhere.
     */
    final static double BORED = 0.15;

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
    
    final static int STD_PR = 0;
    final static int MC1_PR = 1;
    final static int MC2_PR = 2;
    final static int MC3_PR = 3;
    final static int MC4_PR = 4;
    final static int MC5_PR = 5;

    
    /* --------------------------------------------- */

    public PageRank( String linksFilename, String titlesFilename, int type ){
    	this.linksFilename = linksFilename;
    	this.titlesFilename = titlesFilename;
    	this.type = type;
    }
    
    public PageRank( String linksFilename, String titlesFilename ){
    	this(linksFilename, titlesFilename, STD_PR);
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
	    
	    // get doc titles from file
	    Integer numDocs = 0;
	    in = new BufferedReader( new FileReader( this.titlesFilename ));
	    while ((line = in.readLine()) != null && numDocs<MAX_NUMBER_OF_DOCS ){
			String[] aux = line.split( ";" );
			String docName = aux[0];
			String docTitle = aux[1];
			if(this.docNumber.containsKey(docName) == false){
				System.err.println("Unknown doc name found! = " + docName);
				continue;
			}	
			Integer docNumber = this.docNumber.get(docName);
			this.docTitle[docNumber] = docTitle;
			numDocs++;
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
    			throw new Exception("Assertion failed! totalBoredProb + totalLinkProb = "+ (totalBoredProb + totalLinkProb));
    	}
    	
    }



    private ArrayList<CorpusDocument> computePageRank(Integer numberOfDocs) throws Exception {
		ArrayList<CorpusDocument> docs = new ArrayList<CorpusDocument>(numberOfDocs);
		ArrayList<Double> currState = new ArrayList<Double>(numberOfDocs);
		Integer iter = 0;
		
		// initial state prob
		for(Integer did = 0; did < numberOfDocs; did++){
			CorpusDocument doc = new CorpusDocument();
			doc.did = Integer.getInteger(this.docName[did]);
			doc.name = this.docTitle[did];
			doc.rank = 0.0;
			docs.add(doc);
			currState.add(did, 0.0);
		}
		currState.set(0, 1.0);
		
		while(iter < 50){
			Double totalSum = 0.0;			
			
			// do the matrix multiplcation
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
				docs.get(i).rank = sum;
				totalSum += sum;				
			}
			
			
			if(this.almostEqual(totalSum, 1.0, 0.000000001) == false)
				throw new Exception("Assertion failed!");
				
			// copy the new state to the current state
			Double diff = 0.0; 
			for(Integer i=0; i<docs.size(); i++){
				CorpusDocument doc = docs.get(i);
				diff += Math.abs(doc.rank - currState.get(i));
				currState.set(i, doc.rank);
			}
			
			// break if the state did not change much between iterations
			if(diff < EPSILON)
				break;
			
			iter++;
			System.out.println("Iteration " + iter + ": diff = " + diff);
		}
    	
		Collections.sort(docs);
		Integer count = 50;
		for(CorpusDocument doc : docs){
			System.out.println("doc [" + this.docName[doc.did] + "] = ["+ doc.rank +"]");
			if(count == 0)
				break;
			count--;
		}
		System.out.println("-------------------");
		
    	return docs;
		
	}
    

	private ArrayList<CorpusDocument> computePageRankMonteCarlo1(int noOfDocs, int noWalks, ArrayList<CorpusDocument> stddocs) throws Exception {
		ArrayList<CorpusDocument> docs = new ArrayList<CorpusDocument>(noOfDocs);
		Hashtable<Integer, Integer> counts = new Hashtable<Integer, Integer>(noOfDocs);
		
		Random rand = new Random();
		
		for(Integer wlk=0; wlk<noWalks; wlk++){			
				
			// get a random doc
			Integer did = rand.nextInt(noOfDocs);
			
			while(true){
				
				// until i should stop (p(1-c))
				if(rand.nextDouble() < BORED)
					break;
						
				// follow one of the links with eq probability
				Hashtable<Integer, Double> probs = link.get(did);
				if(probs == null){
					// it is a sink! jump!
					did = rand.nextInt(noOfDocs);
					continue;
				}
				
				// jump to one of the links
				Integer linkIdx = rand.nextInt(probs.size());
				List<Integer> keys = new ArrayList<Integer>(probs.keySet());
				did = keys.get(linkIdx);
			}
			
			Integer count = counts.get(did);
			if(count == null)
				count = 0;

			counts.put(did, ++count);
			
			if( wlk > 0 && wlk % SAVE_ITERS == 0){
				docs.clear();
				for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
				    Integer didd = entry.getKey();
				    Integer ccount = entry.getValue();
				
				    CorpusDocument doc = new CorpusDocument();
				    doc.did = didd;
				    doc.rank = (double)ccount / (double)wlk; 
				    docs.add(doc);
				 
				}
				
				saveSquaredErrors(docs, stddocs, "mc1top.txt", 0);
				saveSquaredErrors(docs, stddocs, "mc1bot.txt", 1);
				docs.clear();
			}
			
		}
		
		Double probTotal = 0.0;
		for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
		    Integer did = entry.getKey();
		    Integer count = entry.getValue();
		
		    CorpusDocument doc = new CorpusDocument();
		    doc.did = did;
		    doc.rank = (double)count / (double)noWalks; 
		    docs.add(doc);
		    probTotal += doc.rank;
		}
		
		if(almostEqual(1.0, probTotal, 0.000001) == false)
			throw new Exception("Assertion failed! probTotal = "+ probTotal);
		
		Collections.sort(docs);

		//printTopDocs(docs, 50);
		
		//saveDocsFile(docs, 50, 1);
		return docs;		
	}
    
	private ArrayList<CorpusDocument> computePageRankMonteCarlo2(int noOfDocs, int walksPerDoc, ArrayList<CorpusDocument> stddocs) throws Exception {
		Hashtable<Integer, Integer> counts = new Hashtable<Integer, Integer>(noOfDocs);
		ArrayList<CorpusDocument> docs = new ArrayList<CorpusDocument>(noOfDocs);
		
		Random rand = new Random();
		
		Integer numWalks = 0;
		for(Integer wlk=0; wlk<walksPerDoc; wlk++){
			for(Integer stdid=0; stdid<noOfDocs; stdid++){						
				
				Integer did = stdid;
				while(true){					
					// until i should stop (p(1-c))
					if(rand.nextDouble() < BORED)
						break;
							
					// follow one of the links with eq probability
					Hashtable<Integer, Double> probs = link.get(did);
					if(probs == null){
						// it is a sink! jump!
						did = rand.nextInt(noOfDocs);
						continue;
					}
					
					// jump to one of the links
					Integer linkIdx = rand.nextInt(probs.size());
					List<Integer> keys = new ArrayList<Integer>(probs.keySet());
					did = keys.get(linkIdx);
				}
				
				Integer count = counts.get(did);
				if(count == null)
					count = 0;
	
				counts.put(did, ++count);
				
				numWalks++;
				if( numWalks > 0 && numWalks % SAVE_ITERS == 0){
					docs.clear();
					for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
					    Integer didd = entry.getKey();
					    Integer ccount = entry.getValue();
					
					    CorpusDocument doc = new CorpusDocument();
					    doc.did = didd;
					    doc.rank = (double)ccount / (double)(numWalks); 
					    docs.add(doc);
					 
					}
					
					saveSquaredErrors(docs, stddocs, "mc2top.txt", 0);
					saveSquaredErrors(docs, stddocs, "mc2bot.txt", 1);
					docs.clear();
				}
				
				
			}		
		}
		
		Double probTotal = 0.0;
		for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
		    Integer did = entry.getKey();
		    Integer count = entry.getValue();
		
		    CorpusDocument doc = new CorpusDocument();
		    doc.did = did;
		    doc.rank = (double)count / (double)(noOfDocs * walksPerDoc); 
		    docs.add(doc);
		    probTotal += doc.rank;
		}
		
		if(almostEqual(1.0, probTotal, 0.000001) == false)
			throw new Exception("Assertion failed! probTotal = "+ probTotal);
		
		
		Collections.sort(docs);

		//printTopDocs(docs, 50);		
		return docs;		
	}

	private ArrayList<CorpusDocument> computePageRankMonteCarlo3(int noOfDocs, Integer walksPerDoc, ArrayList<CorpusDocument> stddocs) throws Exception {
		Hashtable<Integer, Integer> counts = new Hashtable<Integer, Integer>(noOfDocs);
		ArrayList<CorpusDocument> docs = new ArrayList<CorpusDocument>(noOfDocs);
		
		Random rand = new Random();
		Integer totalSteps = 0;
		Integer numWalks = 0;		
		
		for(Integer wlk=0; wlk<walksPerDoc; wlk++){
			for(Integer stdid=0; stdid<noOfDocs; stdid++){						
				
				Integer did = stdid;
				while(true){					
					totalSteps += 1;
					
					Integer count = counts.get(did);
					if(count == null)
						count = 0;
		
					counts.put(did, ++count);
					
					// until i should stop (p(1-c))
					if(rand.nextDouble() < BORED)
						break;
							
					// follow one of the links with eq probability
					Hashtable<Integer, Double> probs = link.get(did);
					if(probs == null){
						// it is a sink! jump!
						did = rand.nextInt(noOfDocs);
						continue;
					}
					
					// jump to one of the links
					Integer linkIdx = rand.nextInt(probs.size());
					List<Integer> keys = new ArrayList<Integer>(probs.keySet());
					did = keys.get(linkIdx);
				}
				
				numWalks++;
				if( numWalks > 0 && numWalks % SAVE_ITERS == 0){
					docs.clear();
					for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
					    Integer didd = entry.getKey();
					    Integer ccount = entry.getValue();
					
					    CorpusDocument doc = new CorpusDocument();
					    doc.did = didd;
					    doc.rank = (double)ccount / (double)(totalSteps); 
					    docs.add(doc);
					 
					}
					
					saveSquaredErrors(docs, stddocs, "mc3top.txt", 0);
					saveSquaredErrors(docs, stddocs, "mc3bot.txt", 1);
					docs.clear();
				}

			}		
		}
		
		Double probTotal = 0.0;
		for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
		    Integer did = entry.getKey();
		    Integer count = entry.getValue();
		
		    CorpusDocument doc = new CorpusDocument();
		    doc.did = did;
		    doc.rank = (double)count / (double)totalSteps; 
		    docs.add(doc);
		    probTotal += doc.rank;
		}
		
		if(almostEqual(1.0, probTotal, 0.000001) == false)
			throw new Exception("Assertion failed! probTotal = "+ probTotal);
		
		
		Collections.sort(docs);

		//printTopDocs(docs, 50);
		return docs;		
	}


	private ArrayList<CorpusDocument> computePageRankMonteCarlo4(int noOfDocs, Integer walksPerDoc, ArrayList<CorpusDocument> stddocs) throws Exception {
		Hashtable<Integer, Integer> counts = new Hashtable<Integer, Integer>(noOfDocs);
		ArrayList<CorpusDocument> docs = new ArrayList<CorpusDocument>(noOfDocs);
		
		Random rand = new Random();
		Integer totalSteps = 0;
		Integer numWalks = 0;
		
		for(Integer wlk=0; wlk<walksPerDoc; wlk++){
			for(Integer stdid=0; stdid<noOfDocs; stdid++){						
				
				Integer did = stdid;
				while(true){					
					totalSteps += 1;
					
					Integer count = counts.get(did);
					if(count == null)
						count = 0;
		
					counts.put(did, ++count);
					
					// until i should stop (p(1-c))
					if(rand.nextDouble() < BORED)
						break;
							
					// follow one of the links with eq probability
					Hashtable<Integer, Double> probs = link.get(did);
					if(probs == null){
						// it is a dangling node! terminate!
						break;
					}
					
					// jump to one of the links
					Integer linkIdx = rand.nextInt(probs.size());
					List<Integer> keys = new ArrayList<Integer>(probs.keySet());
					did = keys.get(linkIdx);
				}

				numWalks++;
				if( numWalks > 0 && numWalks % SAVE_ITERS == 0){
					docs.clear();
					for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
					    Integer didd = entry.getKey();
					    Integer ccount = entry.getValue();
					
					    CorpusDocument doc = new CorpusDocument();
					    doc.did = didd;
					    doc.rank = (double)ccount / (double)(totalSteps); 
					    docs.add(doc);
					 
					}
					
					saveSquaredErrors(docs, stddocs, "mc4top.txt", 0);
					saveSquaredErrors(docs, stddocs, "mc4bot.txt", 1);
					docs.clear();
				}
				
			}		
		}
		
		Double probTotal = 0.0;
		for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
		    Integer did = entry.getKey();
		    Integer count = entry.getValue();
		
		    CorpusDocument doc = new CorpusDocument();
		    doc.did = did;
		    doc.rank = (double)count / (double)totalSteps; 
		    docs.add(doc);
		    probTotal += doc.rank;
		}
		
		if(almostEqual(1.0, probTotal, 0.000001) == false)
			throw new Exception("Assertion failed! probTotal = "+ probTotal);
		
		
		Collections.sort(docs);

		//printTopDocs(docs, 50);
		return docs;
	}	

	private ArrayList<CorpusDocument> computePageRankMonteCarlo5(int noOfDocs, Integer noWalks, ArrayList<CorpusDocument> stddocs) throws Exception {
		Hashtable<Integer, Integer> counts = new Hashtable<Integer, Integer>(noOfDocs);
		ArrayList<CorpusDocument> docs = new ArrayList<CorpusDocument>(noOfDocs);
		
		Random rand = new Random();
		Integer totalSteps = 0;
		Integer numWalks = 0;
		
		for(Integer wlk=0; wlk<noWalks; wlk++){
			
			Integer did = rand.nextInt(noOfDocs);
			while(true){					
				totalSteps += 1;
				
				Integer count = counts.get(did);
				if(count == null)
					count = 0;
	
				counts.put(did, ++count);
				
				// until i should stop (p(1-c))
				if(rand.nextDouble() < BORED)
					break;
						
				// follow one of the links with eq probability
				Hashtable<Integer, Double> probs = link.get(did);
				if(probs == null){
					// it is a dangling node! terminate!
					break;
				}
				
				// jump to one of the links
				Integer linkIdx = rand.nextInt(probs.size());
				List<Integer> keys = new ArrayList<Integer>(probs.keySet());
				did = keys.get(linkIdx);

				numWalks++;
				if( wlk > 0 && wlk % SAVE_ITERS == 0){
					docs.clear();
					for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
					    Integer didd = entry.getKey();
					    Integer ccount = entry.getValue();
					
					    CorpusDocument doc = new CorpusDocument();
					    doc.did = didd;
					    doc.rank = (double)ccount / (double)(totalSteps); 
					    docs.add(doc);
					 
					}
					
					saveSquaredErrors(docs, stddocs, "mc5top.txt", 0);
					saveSquaredErrors(docs, stddocs, "mc5bot.txt", 1);
					docs.clear();
				}
				
			}		
		}
		
		Double probTotal = 0.0;
		for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
		    Integer did = entry.getKey();
		    Integer count = entry.getValue();
		
		    CorpusDocument doc = new CorpusDocument();
		    doc.did = did;
		    doc.rank = (double)count / (double)totalSteps; 
		    docs.add(doc);
		    probTotal += doc.rank;
		}
		
		if(almostEqual(1.0, probTotal, 0.000001) == false)
			throw new Exception("Assertion failed! probTotal = "+ probTotal);
		
		
		Collections.sort(docs);

		//printTopDocs(docs, 50);
		return docs;
	}		
	
	private void printTopDocs(ArrayList<CorpusDocument> docs, Integer maxDocs){
		Integer count = maxDocs;
		for(CorpusDocument doc : docs){
			System.out.println("doc [" + this.docName[doc.did] + "] = ["+ doc.rank +"]");
			if(count == 0)
				break;
			count--;
		}	
	}
	
	
	
	private void saveDocsFile(ArrayList<CorpusDocument> mcdocs, ArrayList<CorpusDocument> docs, Integer maxDocs, Integer type, int N) throws Exception{
		String filename = "mc" + type + ".txt";
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
		Double error = 0.0;
		
		HashMap<Integer, CorpusDocument> hash = new HashMap<>();
		for(CorpusDocument doc: mcdocs)
			hash.put(doc.did, doc);
		
		
		Boolean converged = true;
		Integer count = maxDocs;
		for(CorpusDocument doc : docs){
			
			CorpusDocument mcdoc = hash.get(doc.did);
			if(mcdoc == null){
				error += Math.pow(Math.abs(0.0 - doc.rank), 2);
				count--;
				continue;
			}
				
			if(Math.abs(doc.rank - mcdoc.rank) > 0.001)
				converged = false;
			
			error += Math.pow(Math.abs(doc.rank - mcdoc.rank), 2);
			count--;
			
			if(count == 0)
				break;
		}
		
		/*if(converged)
			System.out.println("*** N = " + N + ", CONVERGED!");
		else
			System.out.println("N = " + N + ", didnt converge...");*/
		
		writer.println(error);
		writer.close();
	}
	
	private void saveSquaredErrors(ArrayList<CorpusDocument> mcdocs, ArrayList<CorpusDocument> docs, String filename, int direction) throws Exception{
		ArrayList<CorpusDocument> newmcdocs = new ArrayList<>(mcdocs);
		ArrayList<CorpusDocument> newdocs = new ArrayList<>(docs);
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
		Double error = 0.0;
		
		if(direction == 0){
			Collections.sort(newmcdocs);
			Collections.sort(newdocs);
		}else{
			Collections.sort(newmcdocs, CorpusDocument.RANK_DESC);
			Collections.sort(newdocs, CorpusDocument.RANK_DESC);			
		}

		HashMap<Integer, CorpusDocument> hash = new HashMap<>();
		for(CorpusDocument doc: mcdocs)
			hash.put(doc.did, doc);

		Integer count = 50;
		for(CorpusDocument doc : newdocs){
			
			CorpusDocument mcdoc = hash.get(doc.did);
			if(mcdoc == null){
				error += Math.pow(Math.abs(0.0 - doc.rank), 2);
				count--;
				continue;
			}
			
			error += Math.pow(Math.abs(doc.rank - mcdoc.rank), 2);
			count--;
			
			if(count == 0)
				break;
		}
		
		writer.println(error);
		writer.close();		
		
	}
	
	/* --------------------------------------------- */

	public static void main( String[] args ) throws Exception {
	if ( args.length != 2 ) {
	    System.err.println( "Please give the name of the link file" );
	}
	else {
	    new PageRank( args[0] , args[1], STD_PR).call();
	}
    }
    
    public boolean almostEqual(double a, double b, double eps){
    	return Math.abs(a-b)<eps;
    }


	@Override
	public ArrayList<CorpusDocument> call() throws Exception {
		int noOfDocs = readDocs( linksFilename );
		Integer M = 2;
		Integer N = noOfDocs * M;
		ArrayList<CorpusDocument> stdprDocs;
		ArrayList<CorpusDocument> docs = new ArrayList<>();
		
		switch(this.type){
		case MC1_PR:
				
			stdprDocs = loadPageRank("pr.ser");
			new File("mc1top.txt").delete();
			new File("mc1bot.txt").delete();
						
			//for(Integer n=noOfDocs; n<=N; n += noOfDocs){				
			//	docs.clear();
			docs = computePageRankMonteCarlo1( noOfDocs, N , stdprDocs);
				//saveSquaredErrors(docs, stdprDocs, "mc1.txt", 0);
				//saveDocsFile(docs, stdprDocs, 50, this.type, n);
			//}
			
			printTopDocs(docs, 50);
			
			return docs;
		case MC2_PR:
					
			stdprDocs = loadPageRank("pr.ser");
			new File("mc2top.txt").delete();
			new File("mc2bot.txt").delete();
			
			//for(Integer m=1; m<=M; m += 1){				
			//	docs.clear();
			docs = computePageRankMonteCarlo2(noOfDocs, M, stdprDocs);
				//saveDocsFile(docs, stdprDocs, 50, this.type, m);
			//}
			
			printTopDocs(docs, 50);
			
			return docs;
		case MC3_PR:
			
			stdprDocs = loadPageRank("pr.ser");
			new File("mc3top.txt").delete();
			new File("mc3bot.txt").delete();
			
			//for(Integer m=1; m<=M; m += 1){				
			docs = computePageRankMonteCarlo3(noOfDocs, M, stdprDocs);
			//saveDocsFile(docs, stdprDocs, 50, this.type, m);
			//}
			
			printTopDocs(docs, 50);
			
			return docs;
		case MC4_PR:
			
			stdprDocs = loadPageRank("pr.ser");
			new File("mc4top.txt").delete();
			new File("mc4bot.txt").delete();
			
			docs = computePageRankMonteCarlo4(noOfDocs, M, stdprDocs);
			//saveDocsFile(docs, stdprDocs, 50, this.type, m);

			
			printTopDocs(docs, 50);
			
			return docs;
		case MC5_PR:
			
			stdprDocs = loadPageRank("pr.ser");
			new File("mc5top.txt").delete();
			new File("mc5bot.txt").delete();
				
			docs = computePageRankMonteCarlo5(noOfDocs, N, stdprDocs);
			//saveDocsFile(docs, stdprDocs, 50, this.type, m);
			
			
			printTopDocs(docs, 50);
			
			return docs;
		
		case STD_PR:
		default:
			computeTransitionProbabilities( noOfDocs );
			docs = computePageRank( noOfDocs );
			printTopDocs(docs, 50);
			new File("pr.ser").delete();			
			savePageRank(docs, "pr.ser");
			
			return docs;
		}
	}

	private ArrayList<CorpusDocument> loadPageRank(String filename) throws Exception {
        FileInputStream fis = new FileInputStream(filename);
        ObjectInputStream ois = new ObjectInputStream(fis);
        ArrayList<CorpusDocument> docs = (ArrayList) ois.readObject();
        ois.close();
        fis.close();
        return docs;
	}

	public void savePageRank(ArrayList<CorpusDocument> docs, String filename) throws IOException{
		FileOutputStream fos= new FileOutputStream(filename);
        ObjectOutputStream oos= new ObjectOutputStream(fos);
        oos.writeObject(docs);
        oos.close();
        fos.close();
	}

}
