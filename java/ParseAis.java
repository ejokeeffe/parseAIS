

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eok.database.DatabasePG;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;


/**
 * @brief imports the xml/csv from exact earth and parses it. The purpose for version 1 is to just get the unique imo's to make sure we're properly
 * importing it using the xml->access->postgres approach. Extended in version 2.0 so this class actually imports the xml data to postgres. The convoluted and nasty xml->access->postgres that i 
 * had going before seems to lose data on the way. Amazing that, what a shocker. It now also imports csv to postgres.
 * 
 * @author Eoin O'Keeffe
 * 
 * @version 1.0
 * <br /> 2.0: IMporting to postgres now
 * <br /> 3.0: Added ability to import csv files, renamed to ParseAis
 * 
 * @date 02/10/2012
 * <br /> 08/10/2012
 * <br /> 14/08/2013
 *
 */
public class ParseAis {
	/**
	 * @param 
	 */
    static ArrayList<Double> uniqueImos;
	static ArrayList<AisMessage> ais;
	static String currentTable;
	static int rows;
	static DatabasePG db;
	public static void test(String[] args) {
		
		//uniqueImos = new ArrayList<Double>();

		 db = new DatabasePG();
			db.setDb("ExactEarth");
		// Loop through the folders and read in each sequentially
		
			//String path = "//Users/ucfteoo/Documents/ExactEarth/01072010_31122010/";
		//	String path = "//Users/ucfteoo/Documents/ExactEarth/testcsv/";
		 //String outputPath = "//Users/ucfteoo/Documents/ExactEarth/aug_sep_uniq_imos.csv";
		  //String path = "C:/Users/transport_group/Documents/ExactEarth/01072010_31122010/";
		//	String path = "C:/Users/transport_group/Documents/ExactEarth/01012012_30062012/";
			String path = "C:/Users/transport_group/Documents/DataBackUp/ExactEarth/2012/20120801_20120807.zip/20120801_20120807";
			String files;
		  File folder = new File(path);
		  File[] listOfFiles = folder.listFiles(); 
		  for (int i = 0; i < listOfFiles.length; i++) 
		  {
		 if (listOfFiles[i].isDirectory()){
			 System.out.println("Looking at " + listOfFiles[i].getName());
			 //so we're in one of the month  xml folders
			 //assume the folder names are the same as the tablenames
			 File[] xmlFiles = listOfFiles[i].getAbsoluteFile().listFiles();
			 for (int j=0;j<xmlFiles.length;j++){
				   if (xmlFiles[j].isFile()) 
				   {
						   files = xmlFiles[j].getName();
						   System.out.println(files);
						   //parse xml
						   if (files.contains(".xml")){
							   //if (ais != null) ais.clear();
							   ais = new ArrayList<AisMessage>();
							   //set the number of rows to 0
							   ParseAis.rows =0;
							   //first parse the xml file
							   //and set the name of the current table
							   ParseAis.currentTable = listOfFiles[i].getName();
							   ParseAis.importXmlToPostgres(xmlFiles[j].getAbsolutePath());
							   //Now write the messages to file
							   //Even though loads have already been written, this should do the final bunch
							   ParseAis.writeMessagesToDB(listOfFiles[i].getName());
				   			}else if(files.contains(".csv")){
				   				ais = new ArrayList<AisMessage>();
				   				//set the number of rows to 0
							   ParseAis.rows =0;
							   //first parse the csv file
							   //and set the name of the current table
							   ParseAis.currentTable = listOfFiles[i].getName();
							   ParseAis.importCsvToPostgres(xmlFiles[j].getAbsolutePath());
							   //Now write the messages to file
							   //Even though loads have already been written, this should do the final bunch
							   ParseAis.writeMessagesToDB(listOfFiles[i].getName());
				   				
				   			}//elif
				      }//if
			 }//for j
		 }//if
		  }//for i
	       //save the results to file
	       //ParseExactEarth.generateCsvFile(outputPath);
		
		  System.out.println("All files parsed");
	}
	
	private static void parseXmlFile(String filename){
		try {
			 
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
		 
			DefaultHandler handler = new DefaultHandler() {
		 
			boolean imo = false;
		 
			public void startElement(String uri, String localName,String qName, 
		                Attributes attributes) throws SAXException {
		 
				//System.out.println("Start Element :" + qName);
		 
				if (qName.equalsIgnoreCase("imo")) {
					imo = true;
				}
		 
		 
			}
		 
			public void endElement(String uri, String localName,
				String qName) throws SAXException {
		 
				//System.out.println("End Element :" + qName);
		 
			}
		 
			public void characters(char ch[], int start, int length) throws SAXException {
		 
				if (imo) {
					//System.out.println("New imo : " + new String(ch, start, length));
					//add it to our field
					double tmpImo = Double.parseDouble(new String(ch, start, length));
			    	if (!uniqueImos.contains(tmpImo)){
			    		
					    uniqueImos.add(tmpImo);
				    }//if
					imo = false;
				}
		 
		 
			}
		 
		     };
		 
		       saxParser.parse(filename, handler);
//		 

		       
		     } catch (Exception e) {
		       e.printStackTrace();
		     }
		 
		   }
	private static void generateCsvFile(String sFileName)


	
	   {
		try
		{
		    FileWriter writer = new FileWriter(sFileName);

		    writer.append("imo");
		    writer.append('\n');
		    //loop through imos adding it to the csv file and then to uniqueImos, if the value already exists in 
		    //uniqueImos then don't add it
		    for (int i=1;i<uniqueImos.size();i++){

			    writer.append(String.valueOf(uniqueImos.get(i)));
			    writer.append('\n');
			    //now add to uniqueImos

		    }//for i
	 
		    writer.flush();
		    writer.close();
		}
		catch(IOException e)
		{
		     e.printStackTrace();
		} 
	    }
	
	private static void importXmlToPostgres(String filename){
		try {
			 
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
		 
			DefaultHandler handler = new DefaultHandler() {
		 
			boolean imo = false;
			boolean mmsi = false;
			boolean  date_time = false;
			boolean  msg_type = false; 
			boolean  nav_status = false;
			boolean  sog = false;
			boolean  lon = false;
			boolean  lat = false;
			boolean  course = false;
			boolean  heading = false;
			boolean  vessel_name = false;
			boolean  eta_month = false;
			boolean  eta_day = false;
			boolean  eta_hour = false;
			boolean  eta_minute = false;
			boolean  draught = false;
			boolean  destination = false;
			 int counter = 0;
		 
			public void startElement(String uri, String localName,String qName, 
		                Attributes attributes) throws SAXException {
		 
				//System.out.println("Start Element :" + qName);
				if (qName.equalsIgnoreCase("aismessage")){
					//Change of plan, we're not storing the full AisMessage in memory 
					//when the size reaches 10,000, we do a write 
					if (ParseAis.ais.size()%50000==0 && ParseAis.ais.size()>0){
						//write to db and set this to zero
						ParseAis.writeMessagesToDB(ParseAis.currentTable);
						//Now set the arraylist to empy
						//ParseAis.ais.clear();
						ParseAis.ais = new ArrayList<AisMessage>();
					}//if
					
					//create new ais object and add to the arraylist
					AisMessage newAis = new AisMessage();
					ParseAis.ais.add(newAis);
					ParseAis.rows+=1;
					counter +=1;
					//System.out.println(counter);
				}//if
				if (qName.equalsIgnoreCase("imo")) {
					imo = true;
				}//if
				if (qName.equalsIgnoreCase("mmsi")) {
					mmsi = true;
				}//if
				if (qName.equalsIgnoreCase("date_time")) {			
					date_time = true;
				}//if
				if (qName.equalsIgnoreCase("msg_type")) {
					msg_type = true;
				}//if
				if (qName.equalsIgnoreCase("nav_status")) {
					nav_status = true;
				}//if
				if (qName.equalsIgnoreCase("sog")) {
					sog = true;
				}//if
				if (qName.equalsIgnoreCase("lon")) {
					lon = true;
				}//if
				if (qName.equalsIgnoreCase("lat")) {
					lat = true;
				}//if
				if (qName.equalsIgnoreCase("course")) {
					course = true;
				}//if
				if (qName.equalsIgnoreCase("heading")) {
					heading = true;
				}//if
				if (qName.equalsIgnoreCase("vessel_name")) {
					vessel_name = true;
				}//if
				if (qName.equalsIgnoreCase("eta_month")) {
					eta_month = true;
				}//if
				if (qName.equalsIgnoreCase("eta_day")) {
					eta_day = true;
				}//if
				if (qName.equalsIgnoreCase("eta_hour")) {
					eta_hour = true;
				}//if
				if (qName.equalsIgnoreCase("eta_minute")) {
					eta_minute = true;
				}//if
				if (qName.equalsIgnoreCase("draught")) {
					draught = true;
				}//if
				if (qName.equalsIgnoreCase("destination")) {
					destination = true;
				}//if
		 
			}//startElement
		 
			public void endElement(String uri, String localName,
				String qName) throws SAXException {
		 
				//System.out.println("End Element :" + qName);
		 
			}//endElement
		 
			public void characters(char ch[], int start, int length) throws SAXException {
		 
				if (imo) {
					ais.get(ais.size()-1).imo = Double.parseDouble(new String(ch,start,length));
					imo = false;
				}//if
				if (mmsi) {
					ais.get(ais.size()-1).mmsi = Double.parseDouble(new String(ch,start,length));
					mmsi = false;
				}//if
				if (date_time) {
					ais.get(ais.size()-1).date_time = new String(ch,start,length);
					date_time = false;
				}//if
				if (msg_type) {
					ais.get(ais.size()-1).msg_type = Double.parseDouble(new String(ch,start,length));
					msg_type = false;
				}//if
				if (nav_status) {
					ais.get(ais.size()-1).nav_status = Double.parseDouble(new String(ch,start,length));
					nav_status = false;
				}//if
				if (sog) {
					ais.get(ais.size()-1).sog = Double.parseDouble(new String(ch,start,length));
					sog = false;
				}//if
				if (lon) {
					ais.get(ais.size()-1).lon = Double.parseDouble(new String(ch,start,length));
					lon = false;
				}//if
				if (lat) {
					ais.get(ais.size()-1).lat = Double.parseDouble(new String(ch,start,length));
					lat = false;
				}//if
				if (course) {
					ais.get(ais.size()-1).course = Double.parseDouble(new String(ch,start,length));
					course = false;
				}//if
				if (heading) {
					ais.get(ais.size()-1).heading = Double.parseDouble(new String(ch,start,length));
					heading = false;
				}//if
				if (vessel_name) {
					ais.get(ais.size()-1).vessel_name = new String(ch,start,length);
					vessel_name = false;
				}//if
				if (eta_month) {
					ais.get(ais.size()-1).eta_month = Double.parseDouble(new String(ch,start,length));
					eta_month = false;
				}//if
				if (eta_day) {
					ais.get(ais.size()-1).eta_day = Double.parseDouble(new String(ch,start,length));
					eta_day = false;
				}//if
				if (eta_hour) {
					ais.get(ais.size()-1).eta_hour = Double.parseDouble(new String(ch,start,length));
					eta_hour = false;
				}//if
				if (eta_minute) {
					ais.get(ais.size()-1).eta_minute = Double.parseDouble(new String(ch,start,length));
					eta_minute = false;
				}//if
				if (draught) {
					ais.get(ais.size()-1).draught = Double.parseDouble(new String(ch,start,length));
					draught = false;
				}//if
				if (destination) {
					ais.get(ais.size()-1).destination = new String(ch,start,length);
					destination = false;
				}//if
			}//characters
		 
		     };
		 
		       saxParser.parse(filename, handler);
		       //Pop out the nubmer of rows
		       //System.out.println("No of rows: "+ String.valueOf(handler.rows));

		       
		     } catch (Exception e) {
		       e.printStackTrace();
		     }//try
		
	}//importXmlToPostgres

	/**
	 * @brief similar to importXmlToPostgres, it just imports the csv file instead
	 * 
	 * @param table
	 */
	private static void importCsvToPostgres(String filename){
		
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\",\"";
	 
		try {
	 
			//First we do our mapping from field names to column headers
			
			
			br = new BufferedReader(new FileReader(filename));
			Integer counter=0;
			//skip the first line
			br.readLine();
			//int indx = 0;
			while ((line = br.readLine()) != null) {
	 //System.out.println(indx);
	 //indx++;
			        // use comma as separator
				String[] entry = line.split(cvsSplitBy);
				if (ParseAis.ais.size()%50000==0 && ParseAis.ais.size()>0){
					//write to db and set this to zero
					ParseAis.writeMessagesToDB(ParseAis.currentTable);
					//Now set the arraylist to empy
					//ParseAis.ais.clear();
					ParseAis.ais = new ArrayList<AisMessage>();
				}//if
				
				//create new ais object and add to the arraylist
				AisMessage newAis = new AisMessage();
				ParseAis.ais.add(newAis);
				ParseAis.rows+=1;
				
				//Fill in the message
				//mmsi
				newAis.mmsi = ParseAis.getInternalValue(Double.class, entry, newAis.mmsi_indx);
				
				//imo
				newAis.imo = ParseAis.getInternalValue(Double.class, entry, newAis.imo_indx);
				
				//date_time
				newAis.date_time = ParseAis.getInternalValue(String.class, entry, newAis.date_time_indx);
				
				//Vessel name
				newAis.vessel_name = ParseAis.getInternalValue(String.class, entry, newAis.vessel_name_indx);
				
				//message type
				newAis.msg_type = ParseAis.getInternalValue(Double.class, entry, newAis.msg_type_indx);
				
				//draught
				newAis.draught = ParseAis.getInternalValue(Double.class, entry, newAis.draught_indx);
		
				//dest
				newAis.destination = ParseAis.getInternalValue(String.class, entry, newAis.destination_indx);
				
				//speed
				newAis.sog = ParseAis.getInternalValue(Double.class, entry, newAis.sog_indx);
		
				//long
				newAis.lon = ParseAis.getInternalValue(Double.class, entry, newAis.lon_indx);

				//lat
				newAis.lat = ParseAis.getInternalValue(Double.class, entry, newAis.lat_indx);
				
				//cog
				newAis.course = ParseAis.getInternalValue(Double.class, entry, newAis.course_indx);
			
				//heading
				newAis.heading = ParseAis.getInternalValue(Double.class, entry, newAis.heading_indx);
				
				//eta_month
				newAis.eta_month = ParseAis.getInternalValue(Double.class, entry, newAis.eta_month_indx);
				//if (!entry[newAis.eta_month_indx].replace("\"", "").equals("")){
				//	newAis.eta_month = Double.parseDouble(entry[newAis.eta_month_indx].replace("\"", ""));
				//}
				//eta_day
				newAis.eta_day = ParseAis.getInternalValue(Double.class, entry, newAis.eta_day_indx);

				//eta_hour
				newAis.eta_hour = ParseAis.getInternalValue(Double.class, entry, newAis.eta_hour_indx);
				
				//eta_minute
				newAis.eta_minute = ParseAis.getInternalValue(Double.class, entry, newAis.eta_minute_indx);
				
				//nav status
				newAis.nav_status = ParseAis.getInternalValue(Double.class, entry, newAis.nav_status_indx);

				
				
				counter +=1;
				
	 
			}
			
			//We should ahve some ais left
			//write to db and set this to zero
			ParseAis.writeMessagesToDB(ParseAis.currentTable);
			//Now set the arraylist to empy
			//ParseAis.ais.clear();
			ParseAis.ais = new ArrayList<AisMessage>();
	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		
		
	}//importCsvsToPostgres
	/**
	 * @brief Because of the problems with dodgy values in the cells and also the index not being there this function encapsulates all that tidying up
	 * @param row
	 * @param indx
	 * @return
	 */
	private static <T> T getInternalValue(Class<T> returnClass,String[] row,int indx){
		//First check if that indx exists
		if(row.length>indx){
			if (returnClass.getCanonicalName()=="java.lang.String"){
				return (T) row[indx];
			}//if
			if (!row[indx].replace("\"", "").equals("")){
				if(!row[indx].replace("\"", "").toUpperCase().equals("NONE")){
					Double res;
					res = Double.parseDouble(row[indx].replace("\"", ""));
					return returnClass.cast(res);
				}//if
			}
		}//if
		return null;
	}

	private static void writeMessagesToDB(String table){
		
		//Loop through the messages and package it into groups of insert statements
		double insertGroup = 1000;
		String sqlStr = "";
		for (int i=0;i<ParseAis.ais.size();i++){
			sqlStr = sqlStr + ParseAis.ais.get(i).getSql("\"" + table + "\"");
			if (i%insertGroup == 0){
				//run sql insert
				db.executeStatement(sqlStr);
				sqlStr = "";
			}else if (i==ParseAis.ais.size()-1){
				//run sql insert
				db.executeStatement(sqlStr);
				sqlStr = "";
			}//if
		}//for i
	}//writeMessagesToDB
}//class ParseExactEarth
	
	
