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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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


/**
 *   Processes a directory structure and indexes all PDF and text files.
 */
public class Indexer {

    /** The index to be built up by this indexer. */
    public Index index;
    
	public Corpus corpus;
    
	private MongoClient client;
	private MongoDatabase db;
	private Options opt;

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
    	this.index = new HashedIndex(this.db, this.opt);
    	this.corpus = new Corpus(this.db, this.opt);
    }


    /* ----------------------------------------------- */

    /**
     * Build a new index and store it into disk
     * @param f
     */
    public void buildIndex(File f){    	
    	
    	// parse the files and build the index
    	processFiles(f);
    	
    	// export the remaining index entries and docs info
	    for(Map.Entry<String, PostingsList> map : (HashedIndex)this.index){
	    	((HashedIndex)this.index).savePostings(map.getKey(), map.getValue());
    	} 
	    this.index = new HashedIndex(this.db, this.opt);	    

    	// calculate the tf-idf scores of every term-document pair
    	if(this.opt.offlineTfIdf)
    		calculateScores();
    		
    	Options opt = this.opt;
    	opt.recreateIndex = false;
    	this.index = new HashedIndex(this.db, opt);
    }
    
	/**
	 * Calculate the TF-IDF scores for every term in the db
	 */
    private void calculateScores() {
		
		MongoCollection<IndexEntry> idxCol = this.db.getCollection("index", IndexEntry.class);
    	MongoCursor<IndexEntry> it = idxCol.find().iterator();

    	HashSet<String> updatedTerms = new HashSet<>();
    	
    	// for every term in the db
    	while(it.hasNext()){
    		IndexEntry ie = it.next();        		
    		
    		if(updatedTerms.contains(ie.token))
    			continue;    		
    		
    		PostingsList postings = ((HashedIndex)this.index).getPostings(ie.token);
    		
    		Double idf = this.corpus.idf(postings);
    		for(PostingsEntry pe : postings){
    			CorpusDocument doc = this.corpus.getDocument(pe.docID);
    			pe.score = this.corpus.tf(pe) * idf;
    			pe.score /= doc.lenght;    			
    		}
    		
    		((HashedIndex)this.index).savePostings(ie.token, postings);
    		updatedTerms.add(ie.token);
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
		doc.name = f.getPath();

		//index.docIDs.put( "" + docID, f.getPath() );
		//index.addDocID( "" + docID, f.getPath() );
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
		    corpus.saveDocument(doc);
		    
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
    	index.insert( token, docID, offset );
    }
    
    public void recreateDB(){
    	
    	MongoCollection<IndexEntry> idxCol = this.db.getCollection("index", IndexEntry.class);
    	MongoCollection<CorpusDocument> docCol = this.db.getCollection("docs", CorpusDocument.class);
    	
    	// clean the db
    	idxCol.drop();
    	docCol.drop();
    	
    	// create indexes
    	idxCol.createIndex(new Document("term", 1));
    	docCol.createIndex(new Document("did", 1));    	
    }
}
	
