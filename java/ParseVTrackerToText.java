

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

public class ParseVTrackerToText {
	/*public static void main(String[] args) {
		
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
			  
			  HashMap<String,String> mmsiLocator = new HashMap<String,String>(); //stores locations
				
			  //cycle through folders
			  File[] listOfFiles = parentFolder.listFiles(); 
			  
				  //Only look inside if it contains ucl in the file name format
					for (int jj=0;jj<listOfFiles.length;jj++){
						 if (listOfFiles[jj].isFile()){
							 System.out.println("Looking at zip file: " + listOfFiles[jj].getName());

							 
							 
							 //
							 //First we need to unzip the file
							 //
							 
							    //ParseAISToTextFiles.extractZip(listOfFiles[jj].getAbsolutePath(),aisFolder.getAbsolutePath());

							//once unzipped we should have .csv file of the same name
							
						    //
						    // Parse Individual files within folder
						    //
						    
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
							
							 
									}
									
						    	
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

							
							
							}
						 }
					}*/
					
}
