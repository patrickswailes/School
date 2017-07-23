import java.sql.*; //Import all SQL utilities
import java.util.*;//Import utilities
import java.io.*; // Imports the IO for filewrite


/**
 *
 * @author Patrick Swailes, James Lee
 */
public class JDBCProject {


	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) throws SQLException, IOException { //Main will through IO exceptions from file write opening and SQL from a few places
		int escape = 0;  //Use this variable to escape from the main loop
		int numRows = 0;  //  Use this variable to store the number of requested rows from user
		int tempRows = 0; // Use this to store the number of rows in the queried table
		int currentCount = 0; //Use this to keep track of how many records we have currently accepted from database
		int in = 0;  //Use this to store manu options from user
		boolean validTable = false; //This variable is used to flag to the java program that the table requested by user is valid or not
		boolean sampleBiggerThanTable = false; //This variable is used to notify the java program that the user's sample results are larger than total results.
		String tableName = "";  //This variable stores the name of the table the user has rekquested
		LinkedList <String> columnNames = new LinkedList<String>(); // This is a linked list that will hold/pop the column names of the requested table.  This allows
		// The program to take in any size table as linkedlists are dynamic in size
		List<Integer> availableRecordIndex = new ArrayList<Integer>(); // This arraylist holds Integer values of things that HAVE NOT been picked yet.  When the RNG
		//"decides" to pull a record, it will pull from the tempView with index from this array, it will 
		//then remove that index out of the array so it can't be picked twice.

		String newTable = "";  //This variable holds in the name of the new, user generated table.
		String query = "";		//This variable holds a SQL query as it is built from constants, user input, and column names from the DB
		String strIn = "";		//Variable to hold user input in the form of a string
		Scanner input = new Scanner(System.in); //Scanner used to read in user input
		Scanner queryIn = new Scanner(System.in); //Another scanner used for user input
		Random rand = new Random(System.currentTimeMillis()); //Random number generator creation
		int rando = rand.nextInt(2); //Returns a random number of 0 and 1.  This allows us to implement the requested algorithm by skipping those records that get a RNG of 0
		//and including those with a 1.  We then iterate through the index until the sample is complete.

		FileWriter fw = new FileWriter("Output.txt", true); //Creates a file writer for outputting to file

		//Setting up JDBC stuff
		String url = "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";  //variable to hold connection string
		Connection conn = DriverManager.getConnection(url); //Create a connection to database with provided URL
		Statement st = conn.createStatement(); //create and initialize a statement variable  
		ResultSet rs = null; //Creates a ResultSet variable to store information from queries/statements to the SQL database
		PreparedStatement s = conn.prepareStatement("set search_path to hw3;"); //I'll create temporary views when fetching random entries - can't access unless
		//search_path is set to hw3
		s.execute();  //changes the search_patch.
		try{
			s = conn.prepareStatement("DROP VIEW tempView"); //Used to drop any old tempView that was created in a previous program run
			s.execute(); //executes the SQL statement DROP VIEW tempView;
		}
		catch(SQLException pse){  //Local catch for this error

		}





		while(escape == 0){	//While loop that keeps user in our UI
			try{
				s = conn.prepareStatement("DROP VIEW tempView"); //Used to drop any old tempView that was created in a previous program run
				s.execute(); //executes the SQL statement DROP VIEW tempView;
			}
			catch(SQLException pse){  //Local catch for this error

			}
			System.out.println("Please select an option"); //Menu
			System.out.println("1) Enter Table Name");	
			System.out.println("2) Enter a SQL query");
			System.out.println("3) Exit");
			strIn = input.next();	//Takes in user input as a String
			try{
				in = Integer.parseInt(strIn);  //Try to turn the string into an int
			}    
			catch(NumberFormatException e){
				System.out.println("Please enter a valid number from 1 to 3");  //if not a number, tells the user this				
			}
			currentCount = 0; //re-initialize counter
			numRows = 0; //re-initialize counter
			tempRows = 0; //re-initialize counter
			switch (in) {  //Switch - case statement for menu
			case 1:   //Menu Option 1 - Enter Table Name
				System.out.println(); 
				System.out.println("Please enter the table name:  ");  //Asks user for name of table to get sample rows from
				strIn = input.next();  //holds the name as a string
				tableName = strIn; //holds the name is tableName;
				String testQuery = "select * from ";  //build a test query to make sure the table exists
				testQuery = testQuery + strIn + " limit 1"; //include name of table in test query		

				try{  //tries to see if table exists
					rs = st.executeQuery(testQuery);  //executes "select * from %TABLENAME% limit 1"
					ResultSetMetaData rsmd = rs.getMetaData(); //pulls the metadata of that table from the resultset                          
					int columnsNumber = rsmd.getColumnCount(); //use metadata to get the count of columns
					for (int i = 1; i <= columnsNumber; i++) {	//iterate through columns

						columnNames.add(rsmd.getColumnName(i).toString());  //converts the column names to strings and adds them to our linked list - columnNames

					}

					query = "CREATE VIEW tempView as SELECT ROW_NUMBER() OVER (ORDER BY ";  // Starts building a view to add row numbers to requested table
					query += columnNames.element() + ")"; //use this to add name of first column in the "ORDER BY" clause
					query += " as row, "; // adds the row numbers to a row named 'row'
					while(columnNames.isEmpty() == false){ //Iterate through our linked list of names
						query += columnNames.pop(); //Pop them into the query
						if(columnNames.isEmpty() == false){ //If not the last entry in columnNames
							query += ", ";  //Add a comma, as there will be another entry coming
						}
						else{
							query += " ";  //No comma - last entry
						}

					}

					query+= "FROM " + tableName; //Query the appropriate table

					try{
						st.execute(query); //Tries to create the view
					}
					catch(SQLException se){  //if this catch ever happens - we are pretty much done, it shouldn't happen.
						System.out.println("View Creation FAILED");	
						se.printStackTrace();  //Give some idea what just happened - again, this catch should never get hit.  It means automated query creation failed.
					}

					try{
						s = conn.prepareStatement("SELECT COUNT(row) FROM tempView"); //prepare a query to the database to count the number of entries in the DB
						//We need to know this so we can create the local index and so that we can see if
						//the user requested rows is larger than the total available rows
						rs = s.executeQuery();	// executes command
						while(rs.next()){ //iterate through result set
							tempRows = rs.getInt(1); //stores number of rows in table as tempRows							
						}	
					}
					catch (SQLException se){
						se.printStackTrace();
						System.out.println("Couldn't get row numbers");  //Should never happen, these are all derived queries from earlier information that was checked
					}													//earlier in the program


					System.out.println();	//Second Menu
					System.out.println("How would you like to manipulate the table?");
					System.out.println("1) Return Sample Rows");
					System.out.println("2) Reset the seed of the internal RNG");
					System.out.println("3) Go back");
					strIn = input.next(); //Reads in the user input for the second input into in
					try{
						in = Integer.parseInt(strIn);  //Try to turn the string into an int
					}    
					catch(NumberFormatException e){
						System.out.println("Please enter a valid number from 1 to 3");  //if not a number, tells the user this				
					}

					switch (in){	//Switch - Case for second menu
					case 1:	//User selected "Return Sample Rows"
						System.out.println();
						System.out.println("How many rows?");

						strIn = input.next();  //read in user input for number of rows

						try{
							in = Integer.parseInt(strIn);  //Try to turn the string into an int
							numRows = in;
						}    
						catch(NumberFormatException e){
							System.out.println("Please enter a valid number");  //if not a number, tells the user this
							break;
						}

						System.out.println();
						System.out.println("Where would you like the output to go?");
						System.out.println("1) Standard Out");  //sends to console
						System.out.println("2) Output.txt");	//sends to ./Output.txt
						System.out.println("3) New Table in DB");
						System.out.println("4) Go back");		//back to previous menu
						in = input.nextInt();	//reads in user input as an int
						switch (in){ //switch case to see if user wants output to go to SOUT or file
						case 1: //SOUT
							System.out.println("Outputting to standard out:");  //Lets user know where output is heading
							if(numRows > tempRows){  //checks to see if requested rows were larger than total rows
								System.out.println("You requested more rows than there are available in the sameple, returning all rows: ");
								numRows = tempRows; //This will make it so if user requests more rows than available, it will return all of them
							}
							for (int i = 1; i <= tempRows; i++){ 
								availableRecordIndex.add(i);	//Creates the index.  Adds a number for each requested row which we will use to join on the column 'row'
								//in our tempView view.
							}	
							while(availableRecordIndex.isEmpty() == false && currentCount < numRows){  //tests to see that index isn't empty and that we haven't returned more rows
								//than requested
								for(int i = 1; i <= availableRecordIndex.size(); i++){ //iterate through our index sequentially
									if(availableRecordIndex.get(i-1) != null && currentCount < numRows){ //makes sure it isn't a null entry and that we haven't returned too many rows
										if(rando > 0 && currentCount < numRows){ //So rando will be either 1 or 0 - which is how the sampling algorithm is supposed to work										
											//So we check each record (or in this case just the index) and if we haven't taken it and it isn't
											// a rando=0 (skip) record we take it.  If it does get a 0 - we skip it for now.

											currentCount++;		//Increases count of records as we just found one																					
											query = "Select * FROM TEMPVIEW WHERE row = ";  //Starts to build query to find the record in the DB that we identified in index
											query+= availableRecordIndex.get(i-1); //Pulls index out and uses it in query											
											rs = st.executeQuery(query); //store result of query in a resultset so we can display it
											availableRecordIndex.remove(i-1); //removes the record that we found from the index.
											while (rs.next()) {  //iterates through the result set
												for (int ii = 1; ii <= columnsNumber; ii++) { //For loop that will create a readable output

													if (ii > 1) System.out.print(",  "); //If not before first record, add a comma for readability
													if(rsmd.getColumnType(ii) == -7 ){	//Check to see if incoming data is boolean
														Boolean yn = rs.getBoolean(ii); //store in a yes/no variable
														System.out.print(yn + " " + rsmd.getColumnName(ii)); //prints out the boolean
													}
													else{
														System.out.print(rs.getString(ii+1) + " " + rsmd.getColumnName(ii)); //used to print off all other values
													}


												}
												System.out.println(""); //puts a space between columns
											}
										}
									}
									rando = rand.nextInt(2);  //Get next random number from the generator after we have process the record
								}
							}
							s = conn.prepareStatement("DROP VIEW tempView"); //drops the view
							s.execute(); 
							System.out.println("count " + currentCount); //Outputs the count of records at the end of the output
							break; 
						case 2: //outputting to Output.txt

							System.out.println("Outputting to Output.txt");  //tells user where the output is going
							if(numRows > tempRows){ //checks to see if requested rows were larger than total rows
								System.out.println("You requested more rows than there are available in the sameple, returning all rows: ");
								numRows = tempRows;  //This will make it so if user requests more rows than available, it will return all of them

							}
							for (int i = 1; i <= tempRows; i++){
								availableRecordIndex.add(i);									
							}
							//All comments are the same as the above except if noted
							while(availableRecordIndex.isEmpty() == false && currentCount < numRows){
								for(int i = 1; i <= availableRecordIndex.size(); i++){
									if(availableRecordIndex.get(i-1) != null && currentCount < numRows){
										if(rando > 0 && currentCount < numRows){										
											currentCount++;

											query = "Select * FROM TEMPVIEW WHERE row = ";
											query+= availableRecordIndex.get(i-1);
											rs = st.executeQuery(query);
											availableRecordIndex.remove(i-1);
											while (rs.next()) {
												for (int ii = 1; ii <= columnsNumber; ii++) {
													if (ii > 1) fw.write(",  "); //file write instead of SOUT
													if(rsmd.getColumnType(ii) == -7 ){	//Check to see if incoming data is boolean
														Boolean yn = rs.getBoolean(ii); //store in a yes/no variable
														fw.write(yn + " " + rsmd.getColumnName(ii)); //prints out the boolean
													}
													else{
														fw.write(rs.getString(ii+1) + " " + rsmd.getColumnName(ii)); //used to print off all other values
													}													
												}

												fw.write("" + '\n'); //Need to write newline manually
											}
										}
									}
									rando = rand.nextInt(2);  //generates next random number
								}
							}
							s = conn.prepareStatement("DROP VIEW tempView"); //drops our temp view so we can recreate if needed
							s.execute();
							try{
								fw.write("count " + currentCount + '\n');

							}
							catch(IOException e){
								System.out.println("Error in File");
							}
							break;
						case 3: //New table option
							System.out.println("What would you like to name the table?");
							strIn = input.next();  //stores new table name into our user input string variable
							newTable = strIn;  //stores it in newTable so we can build our query later
							query = "Select * INTO "; //building query
							query += strIn + " FROM " + tableName +" limit 1"; //end of build							
							st.execute(query);  //runs the query that will create the table with one entry in it (used to perserve column types)
							for (int i = 1; i <= columnsNumber; i++) {	//iterate through columns

								columnNames.add(rsmd.getColumnName(i).toString());  //converts the column names to strings and adds them to our linked list - columnNames

							}
							query = "DELETE FROM " + newTable;
							st.execute(query);  //Deletes the one record we copied over

							if(numRows > tempRows){  //checks to see if requested rows were larger than total rows
								System.out.println("You requested more rows than there are available in the sameple, returning all rows: ");
								numRows = tempRows; //This will make it so if user requests more rows than available, it will return all of them
							}
							for (int i = 1; i <= tempRows; i++){ 
								availableRecordIndex.add(i);	//Creates the index.  Adds a number for each requested row which we will use to join on the column 'row'
								//in our tempView view.
							}	
							while(availableRecordIndex.isEmpty() == false && currentCount < numRows){  //tests to see that index isn't empty and that we haven't returned more rows
								//than requested
								for(int i = 1; i <= availableRecordIndex.size(); i++){ //iterate through our index sequentially
									if(availableRecordIndex.get(i-1) != null && currentCount < numRows){ //makes sure it isn't a null entry and that we haven't returned too many rows
										if(rando > 0 && currentCount < numRows){ //So rando will be either 1 or 0 - which is how the sampling algorithm is supposed to work										
											//So we check each record (or in this case just the index) and if we haven't taken it and it isn't
											// a rando=0 (skip) record we take it.  If it does get a 0 - we skip it for now.

											currentCount++;		//Increases count of records as we just found one																					
											query = "Select * FROM TEMPVIEW WHERE row = ";  //Starts to build query to find the record in the DB that we identified in index
											query+= availableRecordIndex.get(i-1); //Pulls index out and uses it in query											
											rs = st.executeQuery(query); //store result of query in a resultset so we can manipulate it
											availableRecordIndex.remove(i-1); //removes the record that we found from the index.
											while(columnNames.isEmpty()==false){
												columnNames.pop();
											}
											for (int a = 1; a <= columnsNumber; a++) {	//iterate through columns

												columnNames.add(rsmd.getColumnName(a).toString());  //converts the column names to strings and adds them to our linked list - columnNames

											}
											query = "INSERT INTO " + newTable + " (";
											while(columnNames.isEmpty() == false){ //Iterate through our linked list of names
												query += columnNames.pop(); //Pop them into the query
												if(columnNames.isEmpty() == false){ //If not the last entry in columnNames
													query += ", ";  //Add a comma, as there will be another entry coming
												}
												else{
													query += " )";  //No comma - last entry
												}

											}
											query += " VALUES ( ";
											while(rs.next()){
												for (int b = 1; b <= columnsNumber; b++){
													if(rsmd.getColumnType(b) == -7 ){	//Check to see if incoming data is boolean
														Boolean yn = rs.getBoolean(b); //store in a yes/no variable
														if(yn){
															query+= "True";  //Need to output in this format - booleans don't convert to strings naturally
														}
														else{
															query+= "False";
														}
													}
													else{
														query+= "'" + rs.getString(b+1) +"'"; //the single quotes help cast the dates
													}
													if(b< columnsNumber){
														query+=", "; //means there are still more columns left, so keep adding commas
													}

												}
												query+=")"; //close the query
											}
											try{
												st.execute(query); //Execute the insert statement
											}
											catch(SQLException se){  
												System.out.println("Table Creation FAILED");	
												se.printStackTrace();  
											}

										}
									}
									rando = rand.nextInt(2);  //Get next random number from the generator after we have process the record
								}
							}
							s = conn.prepareStatement("DROP VIEW tempView"); //drops the view
							s.execute(); 
							System.out.println("count " + currentCount); //Outputs the count of records at the end of the output
							break;

						case 4: //Go Back option
							break;
						default:
							s = conn.prepareStatement("DROP VIEW tempView");
							s.execute();
							break;
						}                   
						break;




					case 2:
						System.out.println();
						rand.setSeed(System.currentTimeMillis());  //Resets the seed
						System.out.println("Random Number Generator Reset");
						System.out.println();

						break;

					case 3:
						s = conn.prepareStatement("DROP VIEW tempView");
						s.execute();
						break;        
					}

				}
				catch(SQLException se){					
					se.printStackTrace();
					System.out.println("Could not find table");
				}
				break;
			case 2: //Accepts a manual query
				System.out.println();
				System.out.println("Please enter your SELECT query on one line, no semicolon: ");
				query = queryIn.nextLine(); //reads in the input as a string

				try{
					rs = st.executeQuery(query); //tries to execute the user query
					ResultSetMetaData rsmd = rs.getMetaData(); //used to get column count and names                          
					int columnsNumber = rsmd.getColumnCount(); //stores number of columns in a local variable
					while (rs.next()) { //iterates through the resultset
						for (int i = 1; i <= columnsNumber; i++) { //Iterates through a result line
							if (i > 1) System.out.print(",  "); //if first column, no leading comma
							String columnValue = rs.getString(i); //output the value
							System.out.print(columnValue + " " + rsmd.getColumnName(i)); //output the column name
						}
						System.out.println(""); //used for formatting the results.
					}
				}
				catch(SQLException se){
					System.out.println("Invalid query: ");
					se.printStackTrace();  //This could get hit pretty frequently, so I print the full stack trace for the user
				}
				break;
			case 3:
				System.out.println();
				System.out.println("Good Bye!");
				escape = 1;
				try{
					rs.close();
				}
				catch(NullPointerException e){

				}
				st.close();
				conn.close();
				break;
			default:

				System.out.println("Invalid Input");
				System.out.println("\n \n \n");
				try{
					s = conn.prepareStatement("DROP VIEW tempView");
					s.execute();
				}
				catch(SQLException se){

				}
				break;
			}			
		}
		fw.close(); //closes the file
	}

}
