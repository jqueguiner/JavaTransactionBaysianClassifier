import java.sql.*;

public class dataConnection {

	static String server = "localhost";
	static int port = 3306;
	static String database = "cgi_export";
	static String user = "root"; 
	static String password = "";
	static Statement statt;

	public dataConnection(String server,int port,String database,String user,String password) throws ClassNotFoundException, SQLException{
		
		dataConnection.server=server;
		dataConnection.port=port;
		dataConnection.database=database;
		dataConnection.user=user;
		dataConnection.password=password;
		
		statt = connect();
		
		System.out.println("Connexion established with : " + user + "@" + server + ":" + String.valueOf(port));
		return;
	}
	
	public ResultSet getAll(String query) throws SQLException {
		return statt.executeQuery(query);
    }
	
	public ResultSet getCountGroupedBy(String table, String whereColumn, String whereValue, String groupByColumn) throws SQLException {
		return statt.executeQuery("SELECT count(" + groupByColumn + ") as count FROM " + table + " WHERE " + whereColumn + "=" + whereValue + " GROUP BY " + groupByColumn + ";");
    }
	
	public void query(String queryStr){

		try {
			msg(queryStr);
			statt.execute(queryStr);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			msg(e.getMessage());
		}
		msg("query : " + queryStr);
    }
	
	public void truncate(String table) throws SQLException{
		/**statt.execute("TRUNCATE TABLE " + table + ";");**/
		msg("clean table " + table);
	}
	
	public static Statement connect() throws ClassNotFoundException, SQLException{
		
		final String driver = "com.mysql.jdbc.Driver";
		
		String connectionURL = null;
		Statement statement = null;
		
		Class.forName(driver);
		connectionURL = "jdbc:mysql://" + server + ":" + String.valueOf(port) + "/" + database;
		Connection connexion = DriverManager.getConnection(connectionURL,user,password);
		statement = connexion.createStatement();

		return statement;
	}
	
	private static void msg(final String msg){
		System.out.println(msg);
	}

}
