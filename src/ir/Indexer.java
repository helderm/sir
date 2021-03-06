/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 */  


package ir;

import static com.mongodb.client.model.Filters.eq;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.*;
import org.apache.pdfbox.util.PDFTextStripper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.apache.pdfbox.pdmodel.PDDocument;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 *   Processes a directory structure and indexes all PDF and text files.
 */
public class Indexer {

    private static final int SUBPHRASE_MIN_DOCs = 100;

	/** The index to be built up by this indexer. */
    //private Index index;
    private HashMap<Integer, Index> indexes;

	public Corpus corpus;
    
	private MongoClient client;
	private MongoDatabase db;
	private Options opt;

	private ExecutorService worker;



    /* ----------------------------------------------- */

    /** Generates a new document identifier based on the file name. */
    private int generateDocID( String s ) {
	return s.hashCode();
    }


    /* ----------------------------------------------- */


    /**
     *  Initializes the index as a HashedIndex.
     * @param opt 
     */
    public Indexer(Options opt) {
    	PostingsListCodec plc = new PostingsListCodec();
    	PostingsEntryCodec pec = new PostingsEntryCodec();
    	IndexEntryCodec iec = new IndexEntryCodec();
    	CorpusDocumentCodec doe = new CorpusDocumentCodec();
		
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
				MongoClient.getDefaultCodecRegistry(), CodecRegistries.fromCodecs(plc), 
				CodecRegistries.fromCodecs(pec), CodecRegistries.fromCodecs(iec),
				CodecRegistries.fromCodecs(doe));
		
		MongoClientOptions options = MongoClientOptions.builder().codecRegistry(codecRegistry)
		        .build();
		
    	this.client = new MongoClient("localhost:27017", options);
    	this.db = client.getDatabase("findr");	
    	this.opt = opt;
    	this.corpus = new Corpus(this.db, this.opt);
    	
    	Index unindex = new HashedIndex(this.db, this.corpus, this.opt);
    	Index bindex = new BiwordIndex(this.db, this.corpus, this.opt);
    	
    	this.indexes = new HashMap<>();
    	this.indexes.put(Index.HASHED_INDEX, unindex);
    	this.indexes.put(Index.BIWORD_INDEX, bindex);

    	this.worker = Executors.newFixedThreadPool(1);
    }


    /* ----------------------------------------------- */

    /**
     * Build a new index and store it into disk
     * @param f
     */
    public void buildIndex(File f){    	
    	
    	//spwan the pagerank calculation thread
    	PageRank pr = new PageRank("data/pagerank/linksDavis.txt", "data/pagerank/articleTitles.txt");
    	
    	System.out.println("Dispatching PageRank job...");
    	Future<ArrayList<CorpusDocument>> future = this.worker.submit(pr);
    	
    	// parse the files and build the index
    	System.out.println("Processing files...");
    	processFiles(f);
    	
    	// export the remaining index entries and docs info
	    System.out.println("Exporting remaining index postings to db...");
    	for(Map.Entry<Integer, Index> map : this.indexes.entrySet()){	    	
    		//if(map.getKey() == Index.BIWORD_INDEX)
    		//	continue;    		
    		
    		Index idx = map.getValue();
	    	idx.save();
	    	
    	} 
    	
    	// calculate the tf-idf scores of every term-document pair
    	for(Map.Entry<Integer, Index> map : this.indexes.entrySet()){	
    		//if(map.getKey() == Index.BIWORD_INDEX)
    		//	continue;   
    		
    		String idxType = map.getKey()==Index.HASHED_INDEX?"Hashed Index":"Biword Index";
    		System.out.println("Calculating scores for ["+ idxType +"]...");
    		Index idx = map.getValue();
    		idx.calculateScores();
    		
       		// save all docs in the db
    		if(map.getKey()==Index.HASHED_INDEX){
    			System.out.println("Saving documents...");
    			this.corpus.saveDocuments();
    		}
    		
    	}
   		
    	//Options opt = this.opt;
    	//opt.recreateIndex = false;
    	
    	// update the docs with their pageranks
    	try { 
			ArrayList<CorpusDocument> docs = future.get();
			System.out.println("Updating ["+docs.size()+"] docs with their pageranks...");
			this.corpus.savePageranks(docs);
		} catch (Exception e) {
			System.err.println("Pagerank failed!");
			e.printStackTrace();
		}
    	
    }

	/**
     *  Tokenizes and indexes the file @code{f}. If @code{f} is a directory,
     *  all its files and subdirectories are recursively processed.
     */
    public void processFiles( File f ) {
	// do not try to index fs that cannot be read
    	
	if ( f.canRead() ) {
	    if ( f.isDirectory() ) {
		String[] fs = f.list();
		// an IO error could occur
		if ( fs != null ) {
		    for ( int i=0; i<fs.length; i++ ) {
			processFiles( new File( f, fs[i] ));
		    }
		}
	    } else {
		//System.err.println( "Indexing " + f.getPath() );
		// First register the document and get a docID
		CorpusDocument doc = corpus.getDocument();
		Integer idx1, idx2;
		idx1 = f.getPath().lastIndexOf('/')+1;
		idx2 = f.getPath().length() - 2;
		doc.name = f.getPath().substring(idx1, idx2);
		doc.rank = 0.0;
		
		System.out.println("- Parsing file [" + doc.name + "]");
		
		try {
		    //  Read the first few bytes of the file to see if it is 
		    // likely to be a PDF 
		    Reader reader = new FileReader( f );
		    char[] buf = new char[4];
		    reader.read( buf, 0, 4 );
		    if ( buf[0] == '%' && buf[1]=='P' && buf[2]=='D' && buf[3]=='F' ) {
			// We assume this is a PDF file
			try {
			    String contents = extractPDFContents( f );
			    reader = new StringReader( contents );
			}
			catch ( IOException e ) {
			    // Perhaps it wasn't a PDF file after all
			    reader = new FileReader( f );
			}
		    }
		    else {
			// We hope this is ordinary text
			reader = new FileReader( f );
		    }
		    SimpleTokenizer tok = new SimpleTokenizer( reader );
		    int offset = 0;
		    while ( tok.hasMoreTokens() ) {
			String token = tok.nextToken();
			
			insertIntoIndex( doc.did, token, offset++ );
		    }
		    
		    doc.lenght = offset;
		    corpus.insertDocument(doc);
		    
		    //index.docLengths.put( "" + docID, offset );
		    //index.addDocLenght( "" + docID, offset );
		    reader.close();
		}
		catch ( IOException e ) {
		    e.printStackTrace();
		}
	    }
	}
    }

    
    /* ----------------------------------------------- */


    /**
     *  Extracts the textual contents from a PDF file as one long string.
     */
    public String extractPDFContents( File f ) throws IOException {
	FileInputStream fi = new FileInputStream( f );
	PDFParser parser = new PDFParser( fi );   
	parser.parse();   
	fi.close();
	COSDocument cd = parser.getDocument();   
	PDFTextStripper stripper = new PDFTextStripper();   
	String result = stripper.getText( new PDDocument( cd ));  
	cd.close();
	return result;
    }


    /* ----------------------------------------------- */


    /**
     *  Indexes one token.
     */
    public void insertIntoIndex( int docID, String token, int offset ) {
    	
    	// insert this token in all indexes
    	for(Map.Entry<Integer, Index> map : this.indexes.entrySet()){
    		//if(map.getKey() == Index.BIWORD_INDEX)
    		//	continue;   
    		
    		Index idx = map.getValue();
    		idx.insert( token, docID, offset );
    	}
    }
    
    /**
     * Search wrapper method, will fwrd the call to the correct index
     * @return
     */
    public PostingsList search(Query query, int queryType, int rankingType, int structureType){
    	if(structureType == Index.SUBPHRASE){
    		PostingsList res = new PostingsList();
    		
    		//do a 2-gram search, and if the returned num of docs is lesser
    		// than a threshold, do a 1-gram search also
    		Index idx = getIndex(Index.BIWORD_INDEX);
   			res = idx.search(query, queryType, rankingType);
    		
    		if(res.size() < SUBPHRASE_MIN_DOCs){
    			query = new Query(query.queryString, this, Index.UNIGRAM);
    			idx = getIndex(Index.HASHED_INDEX);    			
    			res.add(idx.search(query, queryType, rankingType));
    			Collections.sort(res.getList(), PostingsEntry.SCORE_ORDER);
    		}
    		return res;
    	}
    	
    	Index idx = getIndex(structureType);    	
    	return idx.search(query, queryType, rankingType);
    }
    
    public void cleanup(){
    	for(Map.Entry<Integer, Index> map : this.indexes.entrySet()){
    		Index idx = map.getValue();
    		idx.cleanup();
    	}
    }
    
    public PostingsList getPostings(String term, int structureType, boolean useChampions){
    	Index idx = getIndex(structureType);
    	return idx.getPostings(term, useChampions);
    }
    
    public Index getIndex(int structureType){
    	Index idx;
    	
    	switch (structureType) {
		case Index.BIGRAM:
		case Index.SUBPHRASE:
			idx = this.indexes.get(Index.BIWORD_INDEX);
			break;

		case Index.UNIGRAM:
		default:
			idx = this.indexes.get(Index.HASHED_INDEX);
			break;
		}
    	
    	return idx;
    }
    
    public void recreateDB(){
    	
    	MongoCollection<IndexEntry> idxCol = this.db.getCollection("index", IndexEntry.class);
    	MongoCollection<IndexEntry> bidxCol = this.db.getCollection("bindex", IndexEntry.class);
    	MongoCollection<CorpusDocument> docCol = this.db.getCollection("docs", CorpusDocument.class);
    	MongoCollection<Document> scCol = this.db.getCollection("scores");
    	MongoCollection<Document> chCol = this.db.getCollection("champions");
    	
    	// clean the db
    	idxCol.drop();
    	bidxCol.drop();
    	
    	docCol.drop();
    	scCol.drop();
    	chCol.drop();
    	
    	// create indexes
    	idxCol.createIndex(new Document("term", 1));
    	bidxCol.createIndex(new Document("term", 1));
    	docCol.createIndex(new Document("did", 1));    	
    }
}
	
