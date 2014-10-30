

/**
 * @brief Create text files from AIS zip files. The folder structure of the input data is parent folder>year folder>zip files. 
 * The output data is then stored in a folder within the parent folder called parseData_<Date>. See DataStorage.docx in 
 * outputs>>Research and Lit reviews>> for more info on philosophy behind it.
 * 
 * It can parse both xml and csv files but check to make sure selected fields are correct.
 * 
 * @author Eoin O'Keeffe
 * 
 * @version 1.0
 * 
 * @date 12/11/2013
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;







import java.util.Iterator;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eok.generics.Useful;



public class ParseAISToTextFiles {
	static ArrayList<AISMessageLight> ais;
	static int rows;
public static void main(String[] args) {
		
	String parentPath  = "";
	if (args==null){
		parentPath = System.getProperty("user.dir");
	}else{
		for (String s: args) {
            parentPath = s + "/";
        }
	}//if
		
			String yearFolders;
		  File parentFolder = new File(parentPath);
		  System.out.println("Looking at base folder: " + parentFolder.getAbsolutePath());  
		  //Create the output folder to store resultss
		  //This shouldn't already be there
		  SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");//
		    Date now = new Date();
		  File outputFolder = new File(parentFolder.getAbsolutePath() + "/parseData_" + sdfDate.format(now));
		  outputFolder.mkdirs();
		  
		  
		  //cycle through folders
		  File[] listOfFolders = parentFolder.listFiles(); 
		  for (int ll = 0; ll < listOfFolders.length; ll++) 
		  {
			  //Only look inside if's its format is a year format
			if (listOfFolders[ll].getName().matches("^\\d{4}$")){
				//Now check within that folder for files
				File[] listOfFiles = listOfFolders[ll].listFiles();
				File aisFolder=null;
				HashMap<String,String> mmsiLocator = new HashMap<String,String>(); //stores locations
				for (int jj=0;jj<listOfFiles.length;jj++){
					 if (listOfFiles[jj].isFile()){
						 System.out.println("Looking at zip file: " + listOfFiles[jj].getName());
						 
						 //
						 //First we need to unzip the file
						 //
						 
						 //Once unzipped, we can now cycle through all the files in the unzipped folder
						 String aisFolderName = listOfFiles[jj].getAbsolutePath();
						 aisFolderName = aisFolderName.replace(".zip", "");
						 aisFolder = new File(aisFolderName);
					    //ParseAISToTextFiles.unZipIt(listOfFiles[jj].getAbsolutePath(),aisFolder.getAbsolutePath());
					    ParseAISToTextFiles.extractZip(listOfFiles[jj].getAbsolutePath(),aisFolder.getAbsolutePath());
					    //get the list of files in the folder that we've just unzipped
					    File[] aisFiles = aisFolder.listFiles();
					    //if the size of aisFiles is 1, then we've unzipped to a nested folder so jump into that
					    if (aisFiles.length==1){
					    	aisFiles = aisFiles[0].listFiles();
					    	//One more time
					    	if(aisFiles.length==1){
					    		aisFiles = aisFiles[0].listFiles();
					    	}//if
					    }//if
					    
					    
					    //
					    // Parse Individual files within folder
					    //
					    
					    
						 //Now have the list of files, so create variables to hold the data 
					    
					    HashMap<String,ArrayList<String[]>> mmsiMess5 = new HashMap<String,ArrayList<String[]>>(); //stores message 5 data (static)
						HashMap<String,ArrayList<String[]>> mmsiMess1 = new HashMap<String,ArrayList<String[]>>(); //stores message 1/3 data (dynamic)
						ArrayList<String[]> data = new ArrayList<String[]>(); // stores the data itself
						Integer outputDataFileIndex = 0;
						String outputFilePath = outputFolder.getAbsolutePath() + "//" + aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
						String outputFileName = aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
						//Loop through each of the ais files
						for (int ii=0;ii<aisFiles.length;ii++){
						//for (int ii=0;ii<1;ii++){
							 String aisFileName = null;
							   if (aisFiles[ii].isFile()) 
							   {
								   aisFileName = aisFiles[ii].getName();
								   System.out.println("Parsing " + aisFileName);
								   
								   
								   //parse xml
								   if (aisFileName.contains(".xml")){
									   
									   ///
									   // XML
									   ///
									   ParseAISToTextFiles.ais = new ArrayList<AISMessageLight>();
									   ParseAISToTextFiles.importXml(aisFiles[ii].getAbsolutePath());
										 
									   // Sort the data between message 1's and message 5's
									   Iterator<AISMessageLight> iter = ParseAISToTextFiles.ais.iterator();
									   int xmlIndx = 0;
									   while(iter.hasNext()){
										AISMessageLight entry = iter.next();
										if (Runtime.getRuntime().freeMemory()<10000){
											//Write data to file  - this should reset mmsiMess1 and mmsiMess5 and data
											System.out.println("Early parse due to reduced memory: " + Runtime.getRuntime().freeMemory()/1e6);
											ParseAISToTextFiles.parseinputtedDataToCorrectFormat(mmsiMess1,mmsiMess5,mmsiLocator,data,aisFileName,ii,outputFileName);
											System.out.println("Free memory after parsing: " + Runtime.getRuntime().freeMemory()/1e6);
											ParseAISToTextFiles.writeDataFile(data,outputFilePath);
											System.out.println("Free memory before writing to data file: " + Runtime.getRuntime().freeMemory()/1e6);
											data = new ArrayList<String[]>();
											mmsiMess5 = new HashMap<String,ArrayList<String[]>>(); //stores message 5 data (static)
											mmsiMess1 = new HashMap<String,ArrayList<String[]>>(); //stores message 1/3 data (dynamic)
											//now alter the outputfilename
											outputDataFileIndex++;
											outputFilePath = outputFolder.getAbsolutePath() + "//" + aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
											outputFileName = aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
										}//if
										   if (entry.msg_type==5 ||
													entry.msg_type==24){
												//Static Message type
												String[] newEntry = new String[4];
												newEntry[0] = String.valueOf(entry.mmsi); //mmsi
												newEntry[1] = entry.date_time; //timestamp
												newEntry[2] = String.valueOf(entry.imo); //imo
												newEntry[3] = String.valueOf(entry.draught); //draught
												//check date format
												if(newEntry[1].matches("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]")){
													//Add 
													ArrayList<String[]> mmsiList = mmsiMess5.get(newEntry[0]);
													if (mmsiList==null){
														mmsiList = new ArrayList<String[]>();
													}//if
													mmsiList.add(newEntry);
													mmsiMess5.put(newEntry[0], mmsiList);
												}else{
													System.out.println("Incorrect date format in message 5. Index: " + xmlIndx + ". Line: " + entry.toString());
												}//if
											}else if(entry.msg_type==1 ||
													entry.msg_type==3 ||
													entry.msg_type==18){
												//Message type 1 or 3
												String[] newEntry = new String[5];
												
												newEntry[0] = String.valueOf(entry.mmsi); //mmsi
												newEntry[1] = entry.date_time; //timestamp
												//Add only if newEntry[2] is not an empty string
												//check timestamp format
												if (newEntry[1].matches("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]")){
	
															newEntry[2] = String.valueOf(entry.lon); // longitude
															newEntry[3] = String.valueOf(entry.lat); //latitude
															newEntry[4] =String.valueOf(entry.sog); //sog
															ArrayList<String[]> mmsiList = mmsiMess1.get(newEntry[0]);
															if (mmsiList==null){
																mmsiList = new ArrayList<String[]>();
															}//if
															//Add entry
															mmsiList.add(newEntry);
															mmsiMess1.put(newEntry[0], mmsiList);
												}else{
													System.out.println("Incorrect date format in message 1. Index: " + xmlIndx + ". Line: " + entry.toString());
												}
											}//if
										   //Remove entry
										   xmlIndx++;
										   iter.remove();
									   }//while iter

										
						   			}else if(aisFileName.contains(".csv")){
						   				
						   				///
						   				// CSV
						   				///
						   				
						   				String line = "";
						   				BufferedReader br = null;
						   				float[] newLocation;
						   				float[] oldLocation;
										try {
									 
											br = new BufferedReader(new FileReader(aisFiles[ii].getAbsolutePath()));
											//skip the first line
											line = br.readLine();
											String[] tmp = line.split(",");
											int indx =0;
											
											
											while ((line = br.readLine()) != null) {
												
												indx++;
												//System.out.println(indx);
												String[] entry = line.split("\",\"");
												
												//Check to see how much free memory, if not much, say less than 1000, then do a write
												
												if (Runtime.getRuntime().freeMemory()<10000){
													System.out.println("Early parse due to reduced memory: " + Runtime.getRuntime().freeMemory()/1e6);
													//Write data to file  - this should reset mmsiMess1 and mmsiMess5 and data
													ParseAISToTextFiles.parseinputtedDataToCorrectFormat(mmsiMess1,mmsiMess5,mmsiLocator,data,aisFileName,ii,outputFileName);
													System.out.println("Free memory after parsing: " + Runtime.getRuntime().freeMemory()/1e6);
													ParseAISToTextFiles.writeDataFile(data,outputFilePath);
													System.out.println("Free memory before writing to data file: " + Runtime.getRuntime().freeMemory()/1e6);
													data = new ArrayList<String[]>();
													mmsiMess5 = new HashMap<String,ArrayList<String[]>>(); //stores message 5 data (static)
													mmsiMess1 = new HashMap<String,ArrayList<String[]>>(); //stores message 1/3 data (dynamic)
													//now alter the outputfilename
													outputDataFileIndex++;
													outputFilePath = outputFolder.getAbsolutePath() + "//" + aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
													outputFileName = aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
												}//if
												
												//
												//Get message data
												//if (indx==321772){
												//	System.out.println("stop here");
												//}
												//entry has to be greater than 1
												if (entry.length>22){
													if (!entry[1].isEmpty() && !(entry[1]=="") && !(entry[1].contains(",")) && !(entry[1].contains("."))){

														if (Integer.parseInt(entry[1].replace("\"",""))==5 ||
																Integer.parseInt(entry[1].replace("\"",""))==24){
															//Static Message type
															String[] newEntry = new String[4];
															newEntry[0] = entry[0].replace("\"",""); //mmsi
															newEntry[1] = entry[3].replace("\"",""); //timestamp
															newEntry[2] = entry[15].replace("\"",""); //imo
															newEntry[3] = entry[21].replace("\"",""); //draught
															//check date format
															if(newEntry[1].matches("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]")){
																//Add 
																ArrayList<String[]> mmsiList = mmsiMess5.get(newEntry[0]);
																if (mmsiList==null){
																	mmsiList = new ArrayList<String[]>();
																}//if
																mmsiList.add(newEntry);
																mmsiMess5.put(newEntry[0], mmsiList);
															}else{
																System.out.println("Incorrect date format in message 5. Index: " + indx + ". Line: " + line);
															}//if
														}else if((Integer.parseInt(entry[1].replace("\"",""))==1 ||
																Integer.parseInt(entry[1].replace("\"",""))==3 ||
																Integer.parseInt(entry[1].replace("\"",""))==18) &&
																entry.length>30){
															//Message type 1 or 3
															String[] newEntry = new String[5];
															
															newEntry[0] = entry[0].replace("\"",""); //mmsi
															newEntry[1] = entry[3].replace("\"",""); //timestamp
															//Add only if newEntry[2] is not an empty string
															//check timestamp format
															if (newEntry[1].matches("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]")){
																	if(Useful.isDouble(entry[28]) && Useful.isDouble(entry[29])) {

																		newEntry[2] = entry[28]; // longitude
																		newEntry[3] = entry[29]; //latitude
																		if (Useful.isDouble(entry[26])){
																			newEntry[4] =entry[26]; //sog
																		}else{
																			newEntry[4]="-1.0";
																		}//if
																		
																		ArrayList<String[]> mmsiList = mmsiMess1.get(newEntry[0]);
																		if (mmsiList==null){
																			mmsiList = new ArrayList<String[]>();
																		}//if
																		//Make sure entries in correct format
																		if (!newEntry[2].contains(",") && !newEntry[3].contains(",")){
																			mmsiList.add(newEntry);
																			mmsiMess1.put(newEntry[0], mmsiList);
																		}else{
																			System.out.println("Incorrect line format. Index: " + indx + ". Line: " + line);
																		}
																	}else{
																		System.out.println("Empty Entries for long/lat for message type: " + entry[1] 
																				+ ". MMSI: " + entry[0] + ". Timestamp: " + entry[3] + ". Index " + indx + ". line: " + line);
																			
																	}//if
															}else{
																System.out.println("Incorrect date format in message 1. Index: " + indx + ". Line: " + line);
															}
														}else{
															//System.out.println("Unexpected Message type or number of fields. Index: " + indx + ". Line: " + line);
														}
													}else{
														System.out.println("Empty message type or incorrect string format. Index: " + indx + ". Line: " + line);
													}//if
												}else{
													System.out.println("Incorrect line format - no commas. Index: " + indx + ". Line: " + line);
												}
											}//while
											
											
											
											
											
											
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
												}//try
											}//if
										}//try/catch/finally
						   			}//if xml or csv
								   //Parse here for anything left over
								 //Write data to file  - this should reset mmsiMess1 and mmsiMess5 and data
								   System.out.println("Free memory before parsing: " + Runtime.getRuntime().freeMemory()/1e6);
									ParseAISToTextFiles.parseinputtedDataToCorrectFormat(mmsiMess1,mmsiMess5,mmsiLocator,data,aisFileName,ii,outputFileName);
									System.out.println("Free memory after parsing and before writing: " + Runtime.getRuntime().freeMemory()/1e6);
									ParseAISToTextFiles.writeDataFile(data,outputFilePath);
									//clear data
									data = new ArrayList<String[]>();
									mmsiMess5 = new HashMap<String,ArrayList<String[]>>(); //stores message 5 data (static)
									mmsiMess1 = new HashMap<String,ArrayList<String[]>>(); //stores message 1/3 data (dynamic)
									System.out.println("Free memory after writing data to file: " + Runtime.getRuntime().freeMemory()/1e6);
									//now alter the outputfilename
									outputDataFileIndex++;
									outputFilePath = outputFolder.getAbsolutePath() + "//" + aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
									outputFileName = aisFolder.getName() + "_out_" + outputDataFileIndex + ".csv";
							   }//if file

						 }//for ii (list of csv/xml files)
						
						//
						//Delete the folder that was generated
						//
						System.out.println("Delete unzipped folder " + aisFolder.getName());
						//Useful.deleteFolder(aisFolder);
					 }//if
				}//for jj (list of zip files)
				//write indexer to file
			 	ParseAISToTextFiles.writeIndexer(aisFolder,outputFolder.getAbsolutePath(),mmsiLocator);
			 	//Delete unzipped folder
			 	try {
					FileUtils.deleteDirectory(aisFolder);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}//if folder that is a year
		 
	}//for ll - looping through annual folders
		  System.out.println("All files parsed");
}//main


		  /**
			 * @brief Calculates the geodesic distance between points
			 * 
			 * @param oldLocation
			 * @param oldLocation2
			 * @param newLocation
			 * @param newLocation2
			 * @return
			 */
//			public static float distFrom(float oldLocation, float oldLocation2, float newLocation, float newLocation2) {
//			    double earthRadius = 3958.75;
//			    double dLat = Math.toRadians(newLocation-oldLocation);
//			    double dLng = Math.toRadians(newLocation2-oldLocation2);
//			    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
//			               Math.cos(Math.toRadians(oldLocation)) * Math.cos(Math.toRadians(newLocation)) *
//			               Math.sin(dLng/2) * Math.sin(dLng/2);
//			    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
//			    double dist = earthRadius * c;
//
//			    int meterConversion = 1609;
//
//			    return new Float(dist * meterConversion).floatValue();
//			 }
			
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
		 
		            Enumeration entries = zipFile.getEntries();
		            while (entries.hasMoreElements()) {
		                ZipArchiveEntry zipEntry = (ZipArchiveEntry) entries.nextElement();
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
		 
		private static void importXml(String filename){
			try {
				 
				SAXParserFactory factory = SAXParserFactory.newInstance();
				SAXParser saxParser = factory.newSAXParser();
			 
				DefaultHandler handler = new DefaultHandler() {
			 
				boolean imo = false;
				boolean mmsi = false;
				boolean  date_time = false;
				boolean  msg_type = false; 
				
				boolean  sog = false;
				boolean  lon = false;
				boolean  lat = false;
				boolean draught =false;
				 int counter = 0;
			 
				public void startElement(String uri, String localName,String qName, 
			                Attributes attributes) throws SAXException {
			 
					//System.out.println("Start Element :" + qName);
					if (qName.equalsIgnoreCase("aismessage")){
	
						//create new ais object and add to the arraylist
						AISMessageLight newAis = new AISMessageLight();
						ParseAISToTextFiles.ais.add(newAis);
						ParseAISToTextFiles.rows+=1;
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
					
					if (qName.equalsIgnoreCase("sog")) {
						sog = true;
					}//if
					if (qName.equalsIgnoreCase("lon")) {
						lon = true;
					}//if
					if (qName.equalsIgnoreCase("lat")) {
						lat = true;
					}//if
					if (qName.equalsIgnoreCase("draught")) {
						draught = true;
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
					
					if (draught) {
						ais.get(ais.size()-1).draught = Double.parseDouble(new String(ch,start,length));
						draught = false;
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
		 * @brief Parse and write to file. Allows us to clear the buffer
		 * 
		 * 
		 * 
		 */
		public static void parseinputtedDataToCorrectFormat(HashMap<String,ArrayList<String[]>> mmsiMess1,HashMap<String,ArrayList<String[]>> mmsiMess5,
				HashMap<String,String> mmsiLocator,ArrayList<String[]> data,String aisFileName,int ii,String outputFileName){
			//Now loop through the hashmaps by mmsi, to arrange them and 
			Object[] keys = mmsiMess1.keySet().toArray();
			
			//Stores starting index in file location
			Integer indxFileLoc = 1;
			for(int pp=0;pp<keys.length;pp++){
				//System.out.println("Key " + pp + " of " +keys.length);
				ArrayList<String[]> mess1 = mmsiMess1.get(keys[pp]);
				ArrayList<String[]> mess5 = mmsiMess5.get(keys[pp]);
				
				//Arrange Message 5's by time
				//only if we actually have some message 5's
				Date[] mess5Times = null;
				HashMap<Date,Integer> mess5TimeRef = null;
				if (mess5!=null){
					mess5Times = new Date[mess5.size()];
					mess5TimeRef = new HashMap<Date,Integer>();
					for (int kk=0;kk<mess5.size();kk++){
						//convert time to double
						Date date = null;
						try {
							date = new SimpleDateFormat("yyyyMMDD_hhmmss").parse(mess5.get(kk)[1]);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						mess5Times[kk] = date;
						mess5TimeRef.put(date, kk);
					}//for kk
					
					//Now sort the dates
					Arrays.sort(mess5Times);
				}//if
				//Loop through the message 1s and sorting by date also, because we need to calculate distance
				Date[] mess1Times = new Date[mess1.size()];
				HashMap<Date,Integer> mess1TimeRef = new HashMap<Date,Integer>();
				for (int kk=0;kk<mess1.size();kk++){
					//convert time to double
					Date date = null;
					try {
						date = new SimpleDateFormat("yyyyMMDD_hhmmss").parse(mess1.get(kk)[1]);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					mess1Times[kk] = date;
					mess1TimeRef.put(date, kk);
				}//for kk
				Arrays.sort(mess1Times);
				
				//
				//Loop through the message 1's adding entry as we go
				//
				
				float newLocation[] = new float[2];
				float oldLocation[] = new float[2];
				oldLocation[0] = (float) -1e6;
				oldLocation[1] = (float) -1e6;
				int indxOfMess5 = 0;
				for (int kk=0;kk<mess1.size();kk++){
					String[] newEntry = new String[6];
					Integer mess1Indx = mess1TimeRef.get(mess1Times[kk]);
					
					newEntry[0] = mess1.get(mess1Indx)[0];//mmsi
					newEntry[1] = mess1.get(mess1Indx)[1];//time
					
					newEntry[3] = mess1.get(mess1Indx)[4];//speed
					
					if (mess1.get(mess1Indx)[2].isEmpty()){
						System.out.print("Stop");
					}
					if (mess1.get(mess1Indx)[3].isEmpty()){
						System.out.print("Problem here");
					}
					newLocation[0] = Float.parseFloat(mess1.get(mess1Indx)[2]);
					newLocation[1] = Float.parseFloat(mess1.get(mess1Indx)[3]);
					if (oldLocation[0]!=-1e6){
						newEntry[4] = String.valueOf(Useful.getGreatCircle((double) oldLocation[0],(double)  oldLocation[1] ,
								(double) newLocation[0],(double) newLocation[1]));//distance from last point
					}//if
					oldLocation = newLocation.clone();
					
					//Now for draught
					if (mess5Times !=null){
						if (mess5Times[indxOfMess5].before(mess1Times[kk])){
							if (indxOfMess5<mess5Times.length-1){
								//we're not at last index
								
								//SHould we increment the index
								if (mess5Times[indxOfMess5+1].before(mess1Times[kk])){
									//increment index
									indxOfMess5++;
									//Should we increment again?
									if (indxOfMess5<mess5Times.length-1){
										if (mess5Times[indxOfMess5+1].before(mess1Times[kk])){
											//increment index
											indxOfMess5++;
											//Can't imagine we'd have to increment more than twice
										}//if
									}//if
								}//if
							}//if
							//set the draught
							newEntry[5] = mess5.get(mess5TimeRef.get(mess5Times[indxOfMess5]))[3];//draught
							newEntry[2] = mess5.get(mess5TimeRef.get(mess5Times[indxOfMess5]))[2];//imo
						}//if
					}//if
					
					
					//
					// Add the entry to the pile only if it's not a duplicate
					//
					Boolean addEntry = true;
					if (data.size()>0){
						if (newEntry.equals(data.get(data.size()-1))){
							addEntry=false;
						}//if
					}//if
					if (addEntry==true){
						data.add(newEntry);
						
						if (kk==0){
							
								// add entry point
								String str = mmsiLocator.get(newEntry[0]);
								if (str==null){
									//Add the mmsi at the start
									str = newEntry[0];
								}//if
								
								//starting point to new one
								// should return string of form mmsi|file_name;start_line;end_line|file_name;start_line;end_line|.....
								str  = str + "|" + outputFileName + ";" + String.valueOf(indxFileLoc);
								mmsiLocator.put(newEntry[0],str);
						}//if
						//Don't do as else if because there may be only 1 message
						 if(kk==mess1.size()-1) {
								//close it off
								//String str = "";
								//str = mmsiLocator.get(data.get(data.size()-2)[0]);
								String str = mmsiLocator.get(newEntry[0]);
								str = str + ";" + String.valueOf(indxFileLoc) + "|";
								mmsiLocator.put(newEntry[0], str);
							}//if
						 
							//Increment the index of the file location for the indexer
							indxFileLoc++;
					}//if 
				}//for kk
		}//for pp
		}//parseinputtedDataToCorrectFormat
		

		
		private static void writeIndexer(File aisFolder,String outputFolderPath,HashMap<String,String> mmsiLocator){
			//Add the indexer
			  BufferedWriter bufferWriter = null;
			  try {
				File indexer =new File(outputFolderPath + "//" + aisFolder.getName() + "_Indexer.txt");
	
			    if(!indexer.exists()){
			    	indexer.createNewFile();
	    		}
			    FileWriter fw = new FileWriter(indexer,true);
	  	         bufferWriter = new BufferedWriter(fw);
	  	        //Loop through the mmsiLocator and add
	  	        Object[] key = mmsiLocator.keySet().toArray();
	  	        for(int ii=0;ii<mmsiLocator.size();ii++){
	  	        	String mmsiIndex = mmsiLocator.get(key[ii]);
	  	        	//Write entry to file
	  	        	
	  	  	        bufferWriter.write(mmsiIndex);
	  	  	        if (ii!=mmsiLocator.size()-1){
	  	  	        	bufferWriter.newLine();
	  	  	        }
	  	        }//for ii
			  } catch (IOException ex){
				  // report
				} finally {
				   try {
					   
					   bufferWriter.close();
					   } catch (Exception ex) {}
				}//try
		}//writeIndexer
		
		/**
		 * @brief writes the data arraylist to file
		 * @param aisFolder
		 * @param data
		 */
		private static void writeDataFile(ArrayList<String[]> data,String outputFilePath){
			// Output the data file
			BufferedWriter writer = null;
			try {
				
			    writer = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(outputFilePath)));
			    //write headers
			    writer.write("MMSI" + "," + "Time" + "," + "IMO" + "," + "SPEED" + "," + "Dist From Previous" + "," + "draught");
			    writer.newLine();
			    for (int kk=0;kk<data.size();kk++){
			    	String str = data.get(kk)[0] + "," + data.get(kk)[1] + "," + data.get(kk)[2] + "," + data.get(kk)[3]
			    			 + "," + data.get(kk)[4] + "," + data.get(kk)[5];
				    writer.write(str);
				    writer.newLine();
			    }//for jj
			} catch (IOException ex){
			  // report
			} finally {
			   try {
				   
				   writer.close();
				   } catch (Exception ex) {}
			}//try
		}//writeDataFile
		}//ParseAISToTextFiles
