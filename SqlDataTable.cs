/*-------------------------------------------------------
 *              Created by Drew Copenhaver
 *              
 *-------------------------------------------------------
 *               Last Updated: 2013-07-17
 *------------------------------------------------------- 
 * 
 * It became clear that I was reusing certain code far too
 * often. This class is designed to take in a connection
 * string, a SQL-formatted query, and a list of parameters
 * for said query so it can run the query and return a
 * data table object so it can be used instead of having 
 * these statements over and over and over again in my
 * code.
 * 
 * -------------------------------------------------------
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;


    public class SqlDataTable : IDisposable
    {
        private string constr;
        private string query;
        private List<Tuple<string, string>> queryparams;
        private DataTable table;
        private Boolean tablecreated;

        /// <summary>
        /// Empty constructor. Initializes an object of type SqlDataTable. Before use, a connection string and query string must be set. The list of parameters is not required unless the SQL query has @variables in it.
        /// </summary>
        public SqlDataTable()
        {
            constr = "";
            query = "";
            queryparams = new List<Tuple<string, string>>();
            table = new DataTable();
            tablecreated = false;
        }

        /// <summary>
        /// Constructor for the SqlDataTable which sets the connection string, SQL query, and list of query parameters. A list of parameters is not requried.
        /// </summary>
        /// <param name="connection">Connection String</param>
        /// <param name="question">SQL Qurery</param>
        /// <param name="parameters">List of Query Parameters which Helps Mitigate the Potential for SQL Injection</param>
        public SqlDataTable(string connection, string question, List<Tuple<string, string>> parameters)
        {
            constr = connection;
            query = question;
            queryparams = parameters;
            table = new DataTable();
            tablecreated = false;
        }

        /// <summary>
        /// Sets the connection string value to be used for the SQL query. Expected in the same format that SqlConnections use, like "Server=ServerNumber\\SQLEXPRESS; Database=GenericDatabase; Integrated Security=True"
        /// </summary>
        public string ConnectionString
        {
            get
            {
                return constr;
            }

            set
            {
                constr = value;
            }
        }

        /// <summary>
        /// The SQL formatted query. At the moment, this expects to use T-SQL, but there is no reason functionality can't be expanded to properly use MySQL and other formats.
        /// </summary>
        public string Query
        {
            get
            {
                return query;
            }

            set
            {
                query = value;
            }
        }

        /// <summary>
        /// Accesses the data table. This property cannot be set except by calling CreateTable().
        /// </summary>
        public DataTable Table
        {
            get
            {
                return table;
            }
        }

        /// <summary>
        /// This value cannot be manually changed, it only gets set during CreateTable(). It does not display whether or not there is actually data in the datatable, only whether or not the query has been run.
        /// </summary>
        public Boolean TableCreated
        {
            get
            {
                return tablecreated;
            }
        }

        /// <summary>
        /// A method allowing the caller to determine whether or not there is any data in the table.
        /// </summary>
        /// <returns>True if there is at least one row.</returns>
        public Boolean HasData()
        {
            if (table.Rows.Count > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Allows the calling program to determine how many rows a table has.
        /// </summary>
        public int Rows
        {
            get
            {
                return table.Rows.Count;
            }
        }

        /// <summary>
        /// Allows the user to reset the constr, query, and query parameters to an initial value of zero. The table is cleared and the TableCreated property is set to true.
        /// </summary>
        public void Clear()
        {
            constr = "";
            query = "";
            queryparams.Clear();
            table.Clear();
            tablecreated = false;
        }

        /// <summary>
        /// Returns an object of type DataTable using the previously provided connection string and query as well as possible parameters.
        /// </summary>
        /// <returns>A SQL Table modeled by a Datatable.</returns>
        public DataTable CreateTable()
        {
            if ((constr == "") || (query == ""))
            {
                throw new System.ArgumentNullException();
            }

            SqlConnection queryConnection = new SqlConnection(constr);

            using (queryConnection)
            {
                SqlCommand command = new SqlCommand(query, queryConnection);

                string paramname = "";
                string paramvalue = "";

                //For every parameter provided by the calling program, add it to the SqlCommand
                foreach (Tuple<string, string> singleparam in queryparams)
                {
                    paramname = singleparam.Item1;
                    paramvalue = singleparam.Item2;

                    command.Parameters.AddWithValue(paramname, paramvalue);
                }

                queryConnection.Open();

                SqlDataAdapter adapter;
                int retries = 0;

                //Some additional logic to handle timeout errors, give it three chances before throwing an error
                while (true)
                {
                    try
                    {
                        adapter = new SqlDataAdapter(command);
                        adapter.Fill(table);
                        queryConnection.Close();
                        adapter.Dispose();
                        break;
                    }
                    catch
                    {
                        retries++;

                        if (retries > 3)
                        {
                            throw;
                        }
                    }
                }
            }

            tablecreated = true;

            return table;
        }

        /// <summary>
        /// Returns an object of type datatable. Unlike the initial version, this allows a user to provide the connection string, query, and parameters now instead of initially.
        /// </summary>
        /// <param name="connection">Connection String</param>
        /// <param name="queryText">SQL Query</param>
        /// <param name="queryPs">List of Query Parameters which Helps Mitigate the Potential for SQL Injection</param>
        /// <returns>A SQL Table modeled by a Datatable</returns>
        public DataTable CreateTable(string connection, string queryText, List<Tuple<string, string>> queryPs)
        {
            constr = connection;
            query = queryText;
            queryparams = queryPs;

            if ((constr == "") || (query == ""))
            {
                throw new System.ArgumentNullException();
            }

            SqlConnection queryConnection = new SqlConnection(constr);

            using (queryConnection)
            {
                SqlCommand command = new SqlCommand(query, queryConnection);

                string paramname = "";
                string paramvalue = "";

                //For every parameter provided by the calling program, add it to the SqlCommand
                foreach (Tuple<string, string> singleparam in queryparams)
                {
                    paramname = singleparam.Item1;
                    paramvalue = singleparam.Item2;

                    command.Parameters.AddWithValue(paramname, paramvalue);
                }

                queryConnection.Open();

                SqlDataAdapter adapter;
                int retries = 0;

                while (true)
                {
                    try
                    {
                        adapter = new SqlDataAdapter(command);
                        adapter.Fill(table);
                        queryConnection.Close();
                        adapter.Dispose();
                        break;
                    }
                    catch
                    {
                        retries++;

                        if (retries > 3)
                        {
                            throw;
                        }
                    }
                }
            }

            tablecreated = true;

            return table;
        }

        /// <summary>
        /// A list of parameters which expects all primary strings to be unique and in the format "@VariableName" and all secondary strings to be of any type, but converted to a string for the purpose of the query being run.
        /// </summary>
        public List<Tuple<string, string>> QueryParameters
        {
            get
            {
                return queryparams;
            }

            set
            {
                queryparams = value;
            }
        }

        /// <summary>
        /// Allows the user to override the original SqlDataTable object with a datatable of their own.
        /// </summary>
        /// <param name="d">An object of type DataTable with any data the user wishes.</param>
        public void AssignTable(DataTable d)
        {
            table = new DataTable();

            table = d;

            tablecreated = true;
        }

        /// <summary>
        /// Allows the program to get a value from a specific piece of the datatable object with a single method call.
        /// </summary>
        /// <param name="rowId">The row number</param>
        /// <param name="columnId">The column number which the field uses</param>
        /// <returns>A string representing the value within the column. The original data type from SQL may not be preserved. It may not be possible to cast back.</returns>
        public string GetValueAt(int rowId, int columnId)
        {
            string cellvalue = null;

            //If there are rows and (since it's 0 indexed) the rowId is within the table and the columnId is within the table
            if ((table.Rows.Count > 0) && (table.Rows.Count > rowId) && (table.Columns.Count > columnId))
            {
                DataRow row = table.Rows[rowId];

                cellvalue = row[columnId].ToString();
            }

            return cellvalue;
        }

        /// <summary>
        /// Add a pair of values as a Tuple(string, string)
        /// </summary>
        /// <param name="singleparam"></param>
        public void AddParameter(Tuple<string, string> singleparam)
        {
            queryparams.Add(singleparam);
        }

        /// <summary>
        /// Add an additional parameter to the list of parameters for the SQL query. Allows the user to simply set the parameters without a Tuple or list of Tuples.
        /// </summary>
        /// <param name="paramname">A value, expected in the format "@VariableName"</param>
        /// <param name="paramvalue">A string representing the number to be parameterized by the SQL query.</param>
        public void AddParameter(string paramname, string paramvalue)
        {
            Tuple<string, string> paramToAdd = new Tuple<string, string>(paramname, paramvalue);
            queryparams.Add(paramToAdd);
        }

        /// <summary>
        /// Allows you to clear the list of parameters in a query in the event you want to re-run things or improperly add a query.
        /// </summary>
        public void ClearParameters()
        {
            queryparams.Clear();
        }

        /// <summary>
        /// Allows the user a choice of how to clear and dispose of the object, but also allows it to be used inside a using statement.
        /// </summary>
        public void Dispose()
        {
            //Clear for good measure
            constr = "";
            query = "";
            queryparams.Clear();
            tablecreated = false;
            table.Dispose();

            // Per Microsoft's MSDN article:
            //"This object will be cleaned up by the Dispose method.
            // Therefore, you need to call GC.SupressFinalize to
            // take this object off the finalization queue
            // and prevent finalization code for this object
            // from executing a second time."
            GC.SuppressFinalize(this);
        }

        //Destructor
        ~SqlDataTable()
        {
            //Clear for good measure
            constr = "";
            query = "";
            queryparams.Clear();
            tablecreated = false;
            table.Dispose();
        }
    }
