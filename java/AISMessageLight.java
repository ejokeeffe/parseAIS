

public class AISMessageLight {
	public Double mmsi = null;
	public Double imo;
	public String date_time;
	public Double sog;
    public Double lon;
    public Double lat;
    public Double draught;
	public Double msg_type; 
    
    public Integer mmsi_indx;
	public Integer imo_indx;
	public Integer date_time_indx;
	public Integer sog_indx;
    public Integer lon_indx;
    public Integer lat_indx;
    public Integer draught_indx;
    public Integer msg_type_indx; 
    
    public AISMessageLight(){
  
    	//Manually set the indexes
    	mmsi_indx=0;
    	imo_indx=15;
    	date_time_indx=3;
        sog_indx=26;
        lon_indx=28;
        lat_indx=29;
        draught_indx=21;
        msg_type_indx=1;
        
        
    }//constructor
    public String toString(){
    	return "MMSI: " + this.mmsi +
    			" ;Message Type: " + this.msg_type +
    			" ;IMO: " + this.imo +
    			" ;Date_time: "+ this.date_time + 
    			" ;SOG: " + this.sog + 
    			" ;LON: " + this.lon + 
    			" ;LAT: " + this.lat + 
    			" ;DRAUGHT: "+this.draught;
    }
}
