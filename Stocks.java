import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.io.*;

/* Austin Corotan
 * Description: Data Mining using SQL
 */

class Assignment3 {
    static Connection conn = null;
    static Connection conn2 = null;

    public static void main(String[] args) throws Exception {
	// Get connection properties
	String paramsFile = "readerparams.txt";
	String paramsFile2 = "writerparams.txt";
	// readerparams args[0] writerparams args[1]
	if (args.length >= 2) {
	    paramsFile = args[0];
	    paramsFile2 = args[1];
	}
	Properties connectprops = new Properties();
	Properties connectprops2 = new Properties();
	connectprops.load(new FileInputStream(paramsFile));
	connectprops2.load(new FileInputStream(paramsFile2));
	try {
	    // Get connection
	    Class.forName("com.mysql.jdbc.Driver");
	    String dburl = connectprops.getProperty("dburl");
	    String username = connectprops.getProperty("user");
	    String dburl2 = connectprops2.getProperty("dburl");
	    String username2 = connectprops2.getProperty("user");
	    conn = DriverManager.getConnection(dburl, connectprops);
	    conn2 = DriverManager.getConnection(dburl2, connectprops2); 
	    System.out.printf("Database connection %s %s established.%n", dburl, username);
	    System.out.printf("Database connection %s %s established.%n", dburl2, username2);
	    System.out.printf("Processing Data....\n");
	    processData();
	    conn.close();
	    conn2.close();
	    System.out.print("Database connections closed.\n");

	} catch (SQLException ex) {
	    System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
			      ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
	}
    }
    
    //main funcation that processes the data base using SQL queries
    static void processData() throws SQLException{
	ResultSet IndustryRange = getIndustryIntStartEnd();
	while(IndustryRange.next()){
	    String Industry = IndustryRange.getString(1);
	    String start = IndustryRange.getString(2);
	    String end = IndustryRange.getString(3);
	    System.out.printf("Processing %s from %s to %s\n", Industry, start, end);
	    ResultSet TickerRangeData = getTickerRangeData(Industry, start, end);
	    while(TickerRangeData.next()){
		String Ticker = TickerRangeData.getString(1);
		ResultSet TickerIntervals = getIntervals(Ticker, start, end);
		while(TickerIntervals.next()){
		    String StartDate = TickerIntervals.getString(2);
		    String EndDate = TickerIntervals.getString(3);
		    Double TickerRet = getTickerReturnData(Ticker, StartDate, EndDate) - 1;
		    Double IndRet = getIndustryBasketData(Industry, Ticker, StartDate, EndDate);
		    //Insert Data into Performance
		    insertData(Industry, Ticker, StartDate, EndDate, TickerRet, IndRet);
		}
	    }
	}
    }
    
    //gets the min(max TransDate) and max(min TransDate) for all industries
    static ResultSet getIndustryIntStartEnd() throws SQLException{
	ResultSet rs = null;
	PreparedStatement pstmt = conn.prepareStatement("select Industry, max(minDate), min(maxDate) from (select Industry, Ticker, min(TransDate) as minDate, " +
							"  max(TransDate) as maxDate, count(distinct TransDate) as TradingDays from Company left outer join " +
							"  PriceVolume using(Ticker) group by Ticker having Tradingdays >= 150 order by Ticker) maxmin group " +
							"  by Industry");
	rs = pstmt.executeQuery();
	return rs;
    }

    //getMaxMinRange for all tickers
    static ResultSet getTickerRangeData(String Industry, String Start, String End) throws SQLException{
	ResultSet rs = null;
	PreparedStatement pstmt = conn.prepareStatement(" select Ticker, min(TransDate), max(TransDate), count(distinct TransDate) as TradingDays " +
							" from Company left outer join PriceVolume using(Ticker) " +
							" where Industry = ? and TransDate between ? and ? " +
							" group by Ticker having TradingDays >= 150 order by Ticker");
	pstmt.setString(1, Industry);
	pstmt.setString(2, Start);
	pstmt.setString(3, End);
	rs = pstmt.executeQuery();
	return rs;
    }

    //Get trading intervals for all tickers in industry (using first ticker to determine)
    static ResultSet getIntervals(String Ticker, String minDate, String maxDate) throws SQLException{ 
        ResultSet rs = null;
        PreparedStatement pstmt = conn.prepareStatement("select row, Start_Date, End_Date " +
							"  from (select @row_start := @row_start+1 row, Ticker, TransDate as Start_Date " +
							"          from (select @rownum_start := @rownum_start+1 rank, Ticker, " +
							"                  TransDate from PriceVolume, (select @rownum_start := 0) p " +
							"                  where Ticker = ? and TransDate between ? and ? order by TransDate) s, " +
							"                  (select @row_start :=0) r where rank % 60 = 1) start_dates " + 
                                                        "  left join " +
							"  (select @row_end:=@row_end+1 row, TransDate as End_Date " +
							"     from (select @rownum_end:=@rownum_end+1 rank, Ticker, TransDate " +
							"             from PriceVolume, (select @rownum_end := 0) p where Ticker = ? and " +
							"             TransDate between ? and ? order by TransDate) e, (select @row_end :=0) r " +
							"             where rank % 60 = 0) end_dates using(row) where End_Date is not null ");
	pstmt.setString(1, Ticker);
	pstmt.setString(2, minDate);
	pstmt.setString(3, maxDate);
	pstmt.setString(4, Ticker);
	pstmt.setString(5, minDate);
	pstmt.setString(6, maxDate);
	rs = pstmt.executeQuery();
	return rs;
    }	
    
    //calculates the Ticker Return for each Ticker for a given interval
    static Double getTickerReturnData(String Ticker, String startDate, String endDate) throws SQLException{
	ResultSet rs = null;
	try{
	    PreparedStatement pstmt = conn.prepareStatement("select Ticker, TransDate, openPrice, closePrice " +
							    "  from Company join PriceVolume using(Ticker) " +
							    "  where Ticker = ? " +
							    "  and TransDate between ? and ? " +
							    "  order by TransDate DESC, Ticker ");
	 pstmt.setString(1, Ticker);
	 pstmt.setString(2, startDate);
	 pstmt.setString(3, endDate);
	 rs = pstmt.executeQuery();
	} catch (SQLException e){
	    e.printStackTrace();
	}
	double openPrice = 0;
	double closePrice = 0;
	double split21, split31, split32, tickerRet;
	double divisor = 1.0;
        double retClosePrice = 0;
	while(rs.next()){
	    closePrice = rs.getDouble(4);
	    if(retClosePrice == 0){
		retClosePrice = closePrice;}
	    split21 = Math.abs((closePrice/openPrice) - 2.0);
	    split31 = Math.abs((closePrice/openPrice) - 3.0);
	    split32 = Math.abs((closePrice/openPrice) - 1.5);

	    if(openPrice != 0 && split21 < 0.20){
		divisor *=2;
	    }

	    if(openPrice != 0 && split31 < 0.30){
		divisor *= 3;
	    }

	    if(openPrice != 0 && split32 < 0.15){
		divisor *= 1.5;
	    }
	    
	    openPrice = rs.getDouble(3);
	}
	tickerRet = (retClosePrice/(openPrice/divisor));
	return tickerRet;
    }

    //compute IndustryRetData
    static Double getIndustryBasketData(String Industry, String Ticker, String startDate, String endDate) throws SQLException{
	ResultSet rs = null;
	PreparedStatement pstmt = conn.prepareStatement("select Ticker, count(distinct TransDate) as TradingDays " +
							"  from Company join PriceVolume using(Ticker) " +
							"  where Industry = ? " +
							"  and Ticker != ? " +
							"  group by Ticker having TradingDays >= 150 ");
	    pstmt.setString(1, Industry);
	    pstmt.setString(2, Ticker);;
	    rs = pstmt.executeQuery();
	    double indRet = 0;
	    double count = 0;
	    String Tick;
	    while(rs.next()){
		Tick = rs.getString(1);
		indRet +=getTickerReturnData(Tick, startDate, endDate);
		count++;
	    }
	    if(count != 0){indRet = (indRet/count) - 1;}
	    //indRet -= 1;
	    return indRet;
    }
    
    //Insert Data (and/or create table) into table called Performance
    static void insertData(String Industry, String Ticker, String StartDate, String EndDate, Double TickerReturn, Double IndustryReturn){
	try{
	PreparedStatement pstmt = conn2.prepareStatement("INSERT INTO Performance " +
                                                          "VALUES (?,?,?,?,?,?)");
	pstmt.setString(1, Industry);
	pstmt.setString(2, Ticker);
	pstmt.setString(3, StartDate);
	pstmt.setString(4, EndDate);
	pstmt.setDouble(5, TickerReturn);
	pstmt.setDouble(6, IndustryReturn);

	pstmt.executeUpdate();
	} catch (SQLException e){
            e.printStackTrace();
        }
    }
}
