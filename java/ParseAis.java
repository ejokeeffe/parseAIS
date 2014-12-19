

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eok.database.DatabasePG;
import eok.generics.Useful;

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
	public static String Z=System.getProperty("file.separator");
	static String NL=System.getProperty("line.separator");
	static ArrayList<Double> uniqueImos;
	static ArrayList<AisMessage> ais;
	static String currentTable;
	static int rows;
	static DatabasePG db;
	static HashMap<String, HashMap<Integer, ArrayList<String[]>>> mmsiMessages;
	static String current_ais_filename;
	static String ais_source;
	public static void main(String[] args) {

		//uniqueImos = new ArrayList<Double>();

		db = new DatabasePG();
		db.setDb("ExactEarth");
		// Loop through the folders and read in each sequentially

		//String path = "//Users/ucfteoo/Documents/ExactEarth/01072010_31122010/";
		//	String path = "//Users/ucfteoo/Documents/ExactEarth/testcsv/";
		//String outputPath = "//Users/ucfteoo/Documents/ExactEarth/aug_sep_uniq_imos.csv";
		//String path = "C:/Users/transport_group/Documents/ExactEarth/01072010_31122010/";
		//	String path = "C:/Users/transport_group/Documents/ExactEarth/01012012_30062012/";
		String path = "C:/Users/transport_group/Documents/DataBackUp/ExactEarth/2013/";
		ais_source="ExactEarth";
		
		path= "C:/Users/transport_group/Documents/DataBackUp/VesselTracker";
		ais_source="VesselTracker";
		
		String files;
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles(); 
		File aisFolder=null;
		for (int i = 0; i < listOfFiles.length; i++) 
		{
			if (listOfFiles[i].isFile()){
				System.out.println("Looking at " + listOfFiles[i].getName());
				//Once unzipped, we can now cycle through all the files in the unzipped folder
				String aisFolderName = listOfFiles[i].getAbsolutePath();
				aisFolder = new File(path + Z + listOfFiles[i].getName());
				aisFolderName = aisFolder.getAbsolutePath().replace(".zip", "");
				aisFolder = new File(aisFolderName);
				extractZip(listOfFiles[i].getAbsolutePath(),aisFolderName);
				//get the list of files in the folder that we've just unzipped
				File[] aisFiles = aisFolder.listFiles();
				//if the size of aisFiles is 1, then we've unzipped to a nested folder so jump into that
				if (aisFiles.length==1 && aisFiles[0].isDirectory()){
					aisFiles = aisFiles[0].listFiles();
					//One more time
					if(aisFiles.length==1){
						aisFiles = aisFiles[0].listFiles();
					}//if
				}//if

				mmsiMessages = new HashMap<String, HashMap<Integer, ArrayList<String[]>>>(); 

				//Loop through each of the ais files
				for (int ii=0;ii<aisFiles.length;ii++){
					//for (int ii=0;ii<1;ii++){
					if (aisFiles[ii].isFile()) 
					{
						current_ais_filename = aisFiles[ii].getName();
						files = current_ais_filename;
						System.out.println(files);
						int offset=0;
						
						if (ais_source.equalsIgnoreCase("ExactEarth")){
						if (current_ais_filename.substring(current_ais_filename.length()-6, current_ais_filename.length()-5).equalsIgnoreCase("_")){
							offset=2;
						}
						String mon = current_ais_filename.toString().replace("-",
								"").substring(current_ais_filename.toString().replace("-", 
										"").length()-8-offset,current_ais_filename.toString().replace("-", "").length()-6-offset);
						String yr = current_ais_filename.toString().replace("-",
								"").substring(current_ais_filename.toString().replace("-", 
										"").length()-12-offset,current_ais_filename.toString().replace("-", "").length()-8-offset);
						ParseAis.currentTable = "AIS_" + yr + "_"+ mon + "_SelRaw";
						//parse xml
						if (current_ais_filename.contains(".xml")){
							//if (ais != null) ais.clear();
							ais = new ArrayList<AisMessage>();
							//set the number of rows to 0
							ParseAis.rows =0;
							//first parse the xml file
							ParseAis.importXmlToPostgres(aisFiles[ii].getAbsolutePath());
							//Now write the messages to file
							//Even though loads have already been written, this should do the final bunch
							ParseAis.writeMessagesToDB(listOfFiles[i].getName());
						}else if(current_ais_filename.contains(".csv")){
							ais = new ArrayList<AisMessage>();
							//set the number of rows to 0
							ParseAis.rows =0;
							//first parse the csv file
							ParseAis.importCsvToPostgres(aisFiles[ii].getAbsolutePath());
							//Now write the messages to file
							//Even though loads have already been written, this should do the final bunch
							ParseAis.writeMessagesToDB(listOfFiles[i].getName());

						}//elif
						}else if(ais_source.equalsIgnoreCase("VesselTracker")){
							String yr = current_ais_filename.toString().substring(4, 8);
							String mon = current_ais_filename.toString().substring(9,11);
							ParseAis.currentTable = "AIS_VT_" + yr + "_"+ mon + "_SelRaw";
							ais = new ArrayList<AisMessage>();
							//set the number of rows to 0
							ParseAis.rows =0;
							//first parse the csv file
							ParseAis.importCsvToPostgres(aisFiles[ii].getAbsolutePath());
							//Now write the messages to file
							//Even though loads have already been written, this should do the final bunch
							ParseAis.writeMessagesToDB(listOfFiles[i].getName());
						}
					}//if
				}//for ii
				//Delete unzipped folder
				try {
					FileUtils.deleteDirectory(aisFolder);
				} catch (IOException e) {
					e.printStackTrace();
				}
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
							System.out.println(ParseAis.rows);
							//write to db and set this to zero
							ParseAis.writeMessagesToDB(ParseAis.currentTable);
							//Now set the arraylist to empy
							//ParseAis.ais.clear();
							ParseAis.ais = new ArrayList<AisMessage>();
						}//if

						//create new ais object and add to the arraylist
						AisMessage newAis = new AisMessage(ais_source);
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
			int indx = 0;
			while ((line = br.readLine()) != null) {
				//System.out.println(indx);
				indx++;
				// use comma as separator
				String[] entry = line.split(cvsSplitBy);
				if (ParseAis.ais.size()%50000==0 && ParseAis.ais.size()>0){
					System.out.println(indx);
					//write to db and set this to zero
					ParseAis.writeMessagesToDB(ParseAis.currentTable);
					//Now set the arraylist to empy
					//ParseAis.ais.clear();
					ParseAis.ais = new ArrayList<AisMessage>();
				}//if

				//create new ais object and add to the arraylist
				AisMessage newAis = new AisMessage(ais_source);
				ParseAis.ais.add(newAis);
				ParseAis.rows+=1;

				//Fill in the message
				//mmsi
				newAis.mmsi = ParseAis.getInternalValue(Double.class, entry, newAis.mmsi_indx);
				if (newAis.mmsi!=null){
					if (Double.isInfinite(Math.abs(newAis.mmsi))){
						newAis.mmsi=-1.0;
					}
				}
				//imo
				newAis.imo = ParseAis.getInternalValue(Double.class, entry, newAis.imo_indx);
				if (newAis.imo!=null){
					if (Double.isInfinite(Math.abs(newAis.imo))){
						newAis.imo=null;
					}
				}
				//date_time
				newAis.date_time = ParseAis.getInternalValue(String.class, entry, newAis.date_time_indx);

				//Vessel name
				newAis.vessel_name = ParseAis.getInternalValue(String.class, entry, newAis.vessel_name_indx);

				//message type
				try{
					newAis.msg_type = ParseAis.getInternalValue(Double.class, entry, newAis.msg_type_indx);
					if (Math.abs(newAis.msg_type)>1000){
						newAis.msg_type=-1.0;
					}
				}catch(Exception e){
					newAis.msg_type= -1.0;
				}

				//draught
				newAis.draught = ParseAis.getInternalValue(Double.class, entry, newAis.draught_indx);

				//dest
				newAis.destination = ParseAis.getInternalValue(String.class, entry, newAis.destination_indx);

				//speed
				newAis.sog = ParseAis.getInternalValue(Double.class, entry, newAis.sog_indx);
				if (newAis.sog!=null){
					if (Double.isInfinite(Math.abs(newAis.sog))){
						newAis.sog=-1.0;
					}
				}
				//long
				newAis.lon = ParseAis.getInternalValue(Double.class, entry, newAis.lon_indx);
				if (newAis.lon!=null){
					if (Math.abs(newAis.lon)>300){
						newAis.lon=500.0;
					}
				}
				//lat
				newAis.lat = ParseAis.getInternalValue(Double.class, entry, newAis.lat_indx);
				if (newAis.lat!=null){
					if (Math.abs(newAis.lat)>300){
						newAis.lat=500.0;
					}
				}
				//cog
				newAis.course = ParseAis.getInternalValue(Double.class, entry, newAis.course_indx);
				if (newAis.course!=null){
					if (Math.abs(newAis.course)>1e9){
						newAis.course=null;
					}
				}
				//heading
				newAis.heading = ParseAis.getInternalValue(Double.class, entry, newAis.heading_indx);
				if (newAis.heading!=null){
					if (Math.abs(newAis.heading)>1e9){
						newAis.heading=null;
					}
				}
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
				try{
					newAis.nav_status = ParseAis.getInternalValue(Double.class, entry, newAis.nav_status_indx);
					if (Math.abs(newAis.nav_status)>1000){
						newAis.nav_status=-1.0;
					}
				}catch(Exception e){
					newAis.nav_status= -1.0;
				}


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
	@SuppressWarnings("unchecked")
	private static <T> T getInternalValue(Class<T> returnClass,String[] row,int indx){
		//First check if that indx exists
		if(row.length>indx){
			if (returnClass.getCanonicalName()=="java.lang.String"){
				if (indx==-1){
					return (T) "";
				}
				//Remove double quotes
				String str = row[indx].replace("\"", "");
				
				
				//Trim to 255 to allow input into database
				if (str.length()>255){
					str=str.substring(0, 254);
				}
				return (T) str;
			}//if
			if (indx==-1){
				return null;
			}
			if (!row[indx].replace("\"", "").equals("")){
				if(!row[indx].replace("\"", "").toUpperCase().equals("NONE")){
					Double res;
					String val = row[indx].replace("\"", "").replace(",","").replace("_","");
					if (Useful.isDouble(val)){
						res = Double.parseDouble(val);
					}else{
						return null;
					}
					return returnClass.cast(res);
				}//if
			}
		}//if
		return null;
	}

	private static void writeMessagesToDB(String table){

		//Loop through the messages and package it into groups of insert statements
		double insertGroup = 2000;
		String sqlStr = "";
		for (int i=0;i<ParseAis.ais.size();i++){

			sqlStr = sqlStr + ParseAis.ais.get(i).getSql("\"" + table + "\"");
			if (i%insertGroup == 0){
				//System.out.println(i);
				//run sql insert
				db.executeStatement(sqlStr);
				sqlStr = "";
			}else if (i==ParseAis.ais.size()-1){
				//System.out.println(i);
				//run sql insert
				db.executeStatement(sqlStr);
				sqlStr = "";
			}//if
		}//for i
	}//writeMessagesToDB

	/**
	 * Extract zip file at the specified destination path. 
	 * NB:archive must consist of a single root folder containing everything else
	 * 
	 * @param archivePath path to zip file
	 * @param destinationPath path to extract zip file to. Created if it doesn't exist.
	 */
	public static void extractZip(String archivePath, String destinationPath) {
		File archiveFile = new File(archivePath);
		File unzipDestFolder = null;

		try {
			unzipDestFolder = new File(destinationPath);
			String[] zipRootFolder = new String[]{null};
			unzipFolder(archiveFile, archiveFile.length(), unzipDestFolder, zipRootFolder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * Unzips a zip file into the given destination directory.
	 *
	 * The archive file MUST have a unique "root" folder. This root folder is 
	 * skipped when unarchiving.
	 * 
	 * @return true if folder is unzipped correctly.
	 */
	@SuppressWarnings("unchecked")
	private static boolean unzipFolder(File archiveFile,
			long compressedSize,
			File zipDestinationFolder,
			String[] outputZipRootFolder) {

		ZipFile zipFile = null;
		try {
			zipFile = new ZipFile(archiveFile);
			byte[] buf = new byte[65536];

			Enumeration entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry zipEntry = (ZipEntry) entries.nextElement();
				String name = zipEntry.getName();
				//Only continue if csv or xml
				if (name.contains(".csv") || name.contains(".xml")){
					name = name.replace('\\', '/');
					int i = name.indexOf('/');
					if (i > 0) {
						outputZipRootFolder[0] = name.substring(0, i);
						//}
						name = name.substring(i + 1);
					}

					File destinationFile = new File(zipDestinationFolder, name);
					/*
				                if (name.endsWith("/")) {
				                    if (!destinationFile.isDirectory() && !destinationFile.mkdirs()) {
				                        //log("Error creating temp directory:" + destinationFile.getPath());
				                        return false;
				                    }
				                    continue;
				                } else if (name.indexOf('/') != -1) {
				                    // Create the the parent directory if it doesn't exist
				                    File parentFolder = destinationFile.getParentFile();
				                    if (!parentFolder.isDirectory()) {
				                        if (!parentFolder.mkdirs()) {
				                            //log("Error creating temp directory:" + parentFolder.getPath());
				                            return false;
				                        }
				                    }
				                }
					 */
					//Make missing folders
					new File(destinationFile.getParent()).mkdirs();

					System.out.println("Unzipping " + destinationFile.getAbsolutePath());

					FileOutputStream fos = null;
					try {
						fos = new FileOutputStream(destinationFile);
						int n;
						InputStream entryContent = zipFile.getInputStream(zipEntry);
						while ((n = entryContent.read(buf)) != -1) {
							if (n > 0) {
								fos.write(buf, 0, n);
							}
						}
					} finally {
						if (fos != null) {
							fos.close();
						}//if
					}//try
				}//if
			}
			return true;

		} catch (IOException e) {
			e.printStackTrace();
			//log("Unzip failed:" + e.getMessage());
		} finally {
			if (zipFile != null) {
				try {
					zipFile.close();
				} catch (IOException e) {
					e.printStackTrace();
					//log("Error closing zip file");
				}
			}
		}

		return false;
	}
}//class ParseExactEarth


