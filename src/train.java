import java.sql.ResultSet;
import java.sql.SQLException;


public class train {

	static dataManager db;
	static String server;
	static int port;
	static String database;
	static String user;
	static String password;
	
	static String dataTable;
	static String statTable;
	static String trainTable;
	static int trainSetSize;
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		server = "localhost";
		port = 3306;
		database = "cgi_export";
		user = "root";
		password = "";
		
		dataTable = "spend";
		statTable = "stats";
		trainTable = "trainset";
		trainSetSize = 1000;

		db = new dataManager(server, port, database, user, password, dataTable, statTable, trainTable, trainSetSize);
			
		initTrainDB();

	}
	
	public static void initTrainDB() throws SQLException{
		
		db.truncate(statTable);
		db.truncate(trainTable);
		
		db.createTrainTest();
		db.initCategories();
		db.initCategoriesERPs();
		db.initCategoriesGLDescriptions();
		db.initCategoriesVendors();

	}
	
	public static ResultSet getTrainDataSet() throws SQLException{
		return db.getAll("SELECT * FROM trainset;");
	}
	
}
