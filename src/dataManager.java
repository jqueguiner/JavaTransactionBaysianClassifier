import java.sql.*;

public class dataManager extends dataConnection{
	
	dataManager(String server, int port, String database, String user, String password, String dataTable, String statTable, String trainTable, int trainSetSize) throws ClassNotFoundException, SQLException{
		super(server,port,database,user,password);

		dataManager.server = server;
		dataManager.port = port;
		dataManager.database = database;
		dataManager.user = user;
		dataManager.password = password;
		
		dataManager.dataTable = dataTable;
		dataManager.statTable = password;
		dataManager.trainTable = password;

	}

	static Statement statt;
	
	static String server = "localhost";
	
	static int port = 3306;
	static String database = "cgi_export";
	static String user = "root"; 
	static String password = "";
	
	static String dataTable = "spend";
	static String statTable = "stats";
	static String trainTable = "trainset";
	
	static int trainSetSize = 1000;
	
	public static String getServer() {
		return server;
	}

	public static void setServer(String server) {
		dataManager.server = server;
	}

	public static int getPort() {
		return port;
	}

	public static void setPort(int port) {
		dataManager.port = port;
	}

	public static String getDatabase() {
		return database;
	}

	public static void setDatabase(String database) {
		dataManager.database = database;
	}

	public static String getUser() {
		return user;
	}

	public static void setUser(String user) {
		dataManager.user = user;
	}

	public static void setPassword(String password) {
		dataManager.password = password;
	}

	public static String getStatTable() {
		return statTable;
	}

	public static void setStatTable(String statTable) {
		dataManager.statTable = statTable;
	}

	public static String getTrainTable() {
		return trainTable;
	}

	public static void setTrainTable(String trainTable) {
		dataManager.trainTable = trainTable;
	}

	public static int getTrainSetSize() {
		return trainSetSize;
	}

	public static void setTrainSetSize(int trainSetSize) {
		dataManager.trainSetSize = trainSetSize;
	}

	
	public void initCategoriesERPs() throws SQLException{
		super.truncate("categorieserps");
		super.query("INSERT INTO categorieserps (SELECT trainset.Subcategory, trainset.ERPCommodityDescription as erp ,COUNT(trainset.Amount) as cnt, SUM(trainset.Amount),0,0 as amnt from trainset GROUP BY trainset.Subcategory, trainset.ERPCommodityDescription);");
		super.query("UPDATE categorieserps LEFT JOIN (SELECT trainset.ERPCommodityDescription, COUNT(trainset.Company) as cnt,SUM(trainset.Amount) as amnt FROM trainset GROUP BY trainset.ERPCommodityDescription) as ERPStats ON ERPStats.ERPCommodityDescription = categorieserps.dimension SET categorieserps.probabilityCnt = (categorieserps.cnt/ERPStats.cnt)*100000, categorieserps.probabilityAmnt = (categorieserps.amnt/ERPStats.amnt)*100;");
		msg("prepare ERP stats");
	}
	
	public void initCategoriesGLDescriptions() throws SQLException{
		super.truncate("categoriesgldescriptions");
		super.query("INSERT INTO categoriesgldescriptions (SELECT trainset.Subcategory, trainset.GLDescription as gldescription ,COUNT(trainset.Amount) as cnt, SUM(trainset.Amount) as amnt,0,0 from trainset GROUP BY trainset.Subcategory, trainset.GLDescription);");
		super.query("UPDATE categoriesgldescriptions LEFT JOIN (SELECT trainset.GLDescription, COUNT(trainset.Company) as cnt,SUM(trainset.Amount) as amnt FROM trainset GROUP BY trainset.GLDescription) as GLStats ON GLStats.GLDescription = categoriesgldescriptions.dimension SET categoriesgldescriptions.probabilityCnt = (categoriesgldescriptions.cnt/GLStats.cnt)*100000, categoriesgldescriptions.probabilityAmnt = (categoriesgldescriptions.amnt/GLStats.amnt)*100;");
		msg("prepare GL stats");
	}
	
	public void initCategoriesVendors() throws SQLException{
		super.truncate("categoriesvendors");
		super.query("INSERT INTO categoriesvendors (SELECT trainset.Subcategory, trainset.Vendor as vendor ,COUNT(trainset.Amount) as cnt, SUM(trainset.Amount) as amnt,0,0 from trainset GROUP BY trainset.Subcategory, trainset.Vendor);");
		super.query("UPDATE categoriesvendors LEFT JOIN (SELECT trainset.Vendor, COUNT(trainset.Company) as cnt,SUM(trainset.Amount) as amnt FROM trainset GROUP BY trainset.Vendor) as VendorStats ON VendorStats.Vendor = categoriesvendors.dimension SET categoriesvendors.probabilityCnt = (categoriesvendors.cnt/VendorStats.cnt)*100000, categoriesvendors.probabilityAmnt = (categoriesvendors.amnt/VendorStats.amnt)*100;");
		msg("prepare vendor stats");
	}
	
	public void initCategories() throws SQLException{
		super.truncate("categories");
		super.query("INSERT INTO categories (subcategory) (SELECT DISTINCT spend.Subcategory FROM spend);");
		super.query("UPDATE categories LEFT JOIN (SELECT trainset.Subcategory,count(trainset.Company) as cnt, sum(trainset.Amount) as amnt FROM trainset GROUP BY trainset.Subcategory) as trainSubCategoriesStats ON categories.subcategory=trainSubCategoriesStats.Subcategory SET categories.cnt=trainSubCategoriesStats.cnt, categories.amnt=trainSubCategoriesStats.amnt;");
		msg("prepare subcategories");
	}
	
	public void createTrainTest() throws SQLException{
		String createTrainSetQuery = "INSERT INTO trainset (SELECT * FROM " + dataTable + " ORDER BY Amount DESC LIMIT " + String.valueOf(trainSetSize) + ");";
		
		msg("create TrainSet Size = " + String.valueOf(trainSetSize));
	
		super.query(createTrainSetQuery);
	}
	
	private static void msg(final String msg){
		System.out.println(msg);
	}



	
}
