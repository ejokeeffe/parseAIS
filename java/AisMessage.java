public class AisMessage extends AISMessageLight {


	public Double nav_status;
    
    public Double course;
    public Double heading;
    public String vessel_name;
    public Double eta_month;
    public Double eta_day;
    public Double eta_hour;
    public Double eta_minute;
  
    public String destination;
    

	
	public Integer nav_status_indx;
    
    public Integer course_indx;
    public Integer heading_indx;
    public Integer vessel_name_indx;
    public Integer eta_month_indx;
    public Integer eta_day_indx;
    public Integer eta_hour_indx;
    public Integer eta_minute_indx;
   
    public Integer destination_indx;
    
    public AisMessage(String ais_source){
    	//
    	super();
    	if (ais_source.equalsIgnoreCase("ExactEarth")){
    	//Manually set the indexes
    	date_time_indx=3;
    	nav_status_indx=24;
        sog_indx=26;
        lon_indx=28;
        lat_indx=29;
        course_indx=30;
        heading_indx=31;
        vessel_name_indx=13;
        eta_month_indx=45;
        eta_day_indx=46;
        eta_hour_indx=47;
        eta_minute_indx=48;
        draught_indx=21;
        destination_indx=22;
    	}else if(ais_source.equalsIgnoreCase("VesselTracker")){
    		date_time_indx=6;
    		vessel_name_indx=0;
            mmsi_indx=2;
        	imo_indx=1;
        	lon_indx=7;
            lat_indx=8;
            sog_indx=9;
            course_indx=10;
            heading_indx=11;
            draught_indx=12;
            destination_indx=-1;
            eta_month_indx=-1;
            eta_day_indx=-1;
            eta_hour_indx=-1;
            eta_minute_indx=-1;
    	}
    }//constructor
    
    
    public String getSql(String table){
    	String sql = "insert into " + table + " (";
    	String vals = "(";
    	if (super.mmsi != null){
    		sql = sql + "mmsi,";
    		vals = vals + String.valueOf(this.mmsi) + ",";
    	}//if
    	if (this.imo != null){
    		sql = sql + "imo,";
    		vals = vals + String.valueOf(this.imo) + ",";
    	}//if
    	if (this.date_time != null){
    		sql = sql + "date_time,";
    		vals = vals + "'" + String.valueOf(this.date_time) + "',";
    	}//if
    	if (this.msg_type != null){
    		sql = sql + "msg_type,";
    		vals = vals + String.valueOf(this.msg_type) + ",";
    	}//if
    	if (this.nav_status != null){
    		sql = sql + "nav_status,";
    		vals = vals + String.valueOf(this.nav_status) + ",";
    	}//if
    	if (this.sog != null){
    		sql = sql + "sog,";
    		vals = vals + String.valueOf(this.sog) + ",";
    	}//if
    	if (this.lon != null){
    		sql = sql + "lon,";
    		vals = vals + String.valueOf(this.lon) + ",";
    	}//if
    	if (this.lat != null){
    		sql = sql + "lat,";
    		vals = vals + String.valueOf(this.lat) + ",";
    	}//if
    	if (this.course != null){
    		sql = sql + "course,";
    		vals = vals + String.valueOf(this.course) + ",";
    	}//if
    	if (this.heading != null){
    		sql = sql + "heading,";
    		vals = vals + String.valueOf(this.heading) + ",";
    	}//if
    	if (this.vessel_name != null){
    		sql = sql + "vessel_name,";
    		this.vessel_name = this.vessel_name.replaceAll("'", "''");
    		vals = vals + "'" + this.vessel_name + "',";
    	}//if
    	if (this.eta_month != null){
    		sql = sql + "eta_month,";
    		vals = vals + String.valueOf(this.eta_month) + ",";
    	}//if
    	if (this.eta_day != null){
    		sql = sql + "eta_day,";
    		vals = vals + String.valueOf(this.eta_day) + ",";
    	}//if
    	if (this.eta_hour != null){
    		sql = sql + "eta_hour,";
    		vals = vals + String.valueOf(this.eta_hour) + ",";
    	}//if
    	if (this.eta_minute != null){
    		sql = sql + "eta_minute,";
    		vals = vals + String.valueOf(this.eta_minute) + ",";
    	}//if
    	if (this.draught != null){
    		sql = sql + "draught,";
    		vals = vals + String.valueOf(this.draught) + ",";
    	}//if
    	if (this.destination != null){
    		sql = sql + "destination,";
    		this.destination = this.destination.replaceAll("'", "''");
    		vals = vals + "'" + this.destination + "',";
    	}//if
    	return sql.substring(0, sql.length()-1) + ") values " + vals.substring(0, vals.length()-1) + ");";
    }//getSql
}
