/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 */  


package ir;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
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
import com.mongodb.client.MongoDatabase;

import org.apache.pdfbox.pdmodel.PDDocument;


/**
 *   Processes a directory structure and indexes all PDF and text files.
 */
public class Indexer {

    /** The index to be built up by this indexer. */
    public Index index;
    
    /** The next docID to be generated. */
    private int lastDocID = 0;

	private MongoClient client;

	private MongoDatabase db;

	private Options opt;


    /* ----------------------------------------------- */


    /** Generates a new document identifier as an integer. */
    private int generateDocID() {
	return lastDocID++;
    }

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
		
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
				MongoClient.getDefaultCodecRegistry(), CodecRegistries.fromCodecs(plc), 
				CodecRegistries.fromCodecs(pec), CodecRegistries.fromCodecs(iec));
		
		MongoClientOptions options = MongoClientOptions.builder().codecRegistry(codecRegistry)
		        .build();
		
    	this.client = new MongoClient("localhost:27017", options);
    	this.db = client.getDatabase("sir");	
    	this.opt = opt;
    	this.index = new HashedIndex(this.db, this.opt);
    }


    /* ----------------------------------------------- */

    public void buildIndex(File f){    	
    	Options opt = new Options();
    	opt.cacheSize = -1;
    	this.index = new HashedIndex(this.db, opt);   	
    	
    	processFiles(f);
    	exportIndexToDB();  
    	
    	if(this.opt.memoryOnly == false)
    		this.index = new HashedIndex(this.db, this.opt);
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
		int docID = generateDocID();

		index.docIDs.put( "" + docID, f.getPath() );
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
			insertIntoIndex( docID, token, offset++ );
		    }
		    index.docLengths.put( "" + docID, offset );
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
    
    public void exportIndexToDB(){
    	
    	// clean the db
    	MongoCollection<IndexEntry> idxCol = this.db.getCollection("index", IndexEntry.class);
    	MongoCollection<Document> docCol = this.db.getCollection("docs");
    	
    	// export index entries
    	if(this.opt.memoryOnly == false){
        	idxCol.drop();
        	docCol.drop();
        	idxCol.createIndex(new Document("token", 1));
        	docCol.createIndex(new Document("did", 1)); 		
   
	    	for(Map.Entry<String, PostingsList> map : (HashedIndex)this.index){
	    		IndexEntry ie = new IndexEntry();
		    	ie.token = map.getKey();
		    	ie.postings = map.getValue();
		    	//Document doc = new Document("token", new BsonString(token))
		    	//				.append("postings", postings);
		    	idxCol.insertOne(ie);
	    	}
    	}
    	
    	// export doc names and lenghts
    	HashMap<String, String> docIDs = this.index.docIDs;
    	HashMap<String, Integer> docLenghts = this.index.docLengths;
    	
    	for(Map.Entry<String, String> map : docIDs.entrySet()){
    		String docID = map.getKey();
    		String docName = map.getValue();
    		Integer docLenght = docLenghts.get(docID);
    		
    		Document doc = new Document("did", docID)
    							.append("name", docName)
    							.append("lenght", docLenght);
    		docCol.insertOne(doc);
    	}
    }
}
	
