using System;
using System.Data;
using System.Data.Common;

namespace DataAccess
{
    public class DataAccessor
    {
        public enum ConnectionType
        {
            SqlServer = 1,
            MySql = 2
        }

        public int TimeoutSeconds = 900;
        public ConnectionType Connection;
        public string Provider;
        public string Error = "";
        
        public DataAccessor(ConnectionType cType)
        {
            Connection = cType;
            switch (cType)
            {
                case ConnectionType.SqlServer: SetProvider("sqlserver"); break;
                case ConnectionType.MySql: SetProvider("mysql");  break;
            }
        }

        public DataAccessor()
        {
            var dbType = "sqlserver".ToLower(); // default to SQL server
            SetProvider(dbType);
        }

        private void SetProvider(string dbType)
        {
            switch (dbType)
            {
                case "sqlserver": Connection = ConnectionType.SqlServer; Provider = "System.Data.SqlClient"; break;
                case "mysql": Connection = ConnectionType.MySql; Provider = "MySql.Data.MySqlClient"; break;
                default: Connection = ConnectionType.SqlServer; Provider = "System.Data.SqlClient"; break;
            }
        }

        public DbConnection GetConnection(string connString)
        {
            var factory = DbProviderFactories.GetFactory(Provider);
            var conn = factory.CreateConnection();
            if (null != conn)
            {
                conn.ConnectionString = connString;   
            }
            return conn;
        }

        public DbDataReader ExecuteQueryToReader(DbConnection conn, string procName, params string[] parameters)
        {
            DbDataReader rdr;

            using (var cmd = CreateCommand(conn, procName, CommandType.StoredProcedure))
            {
                AddParameters(cmd, parameters);
                try
                {
                    rdr = cmd.ExecuteReader();
                }
                catch (Exception ex)
                {
                    Error = "Error: " + ex.Message + " -- " + ex.StackTrace;
                    return null;
                }
            }

            return rdr;
        }

        public DbDataReader ExecuteQueryToReader(DbConnection conn, string sql)
        {
            DbDataReader rdr;

            using (var cmd = CreateCommand(conn, sql, CommandType.Text))
            {
                try
                {
                    rdr = cmd.ExecuteReader();
                }
                catch (Exception ex)
                {
                    Error = "Error: " + ex.Message + " -- " + ex.StackTrace;
                    return null;
                }
            }

            return rdr;
        }

        public string ExecuteNonQuery(DbConnection conn, string procName, params string[] parameters)
        {
            string affected;
            using (var cmd = CreateCommand(conn, procName, CommandType.StoredProcedure))
            {
                AddParameters(cmd, parameters);
                try
                {
                    affected = cmd.ExecuteNonQuery().ToString();
                }
                catch (Exception ex)
                {
                    affected = "Error: " + ex.Message + " -- " + ex.StackTrace;
                }
            }
            return affected;
        }

        public string ExecuteNonQuery(DbConnection conn, string sql)
        {
            string affected;
            using (var cmd = CreateCommand(conn, sql, CommandType.Text))
            {
                try
                {
                    affected = cmd.ExecuteNonQuery().ToString();
                }
                catch (Exception ex)
                {
                    affected = "Error: " + ex.Message + " -- " + ex.StackTrace;
                }
            }
            return affected;
        }

        private DbCommand CreateCommand(DbConnection conn, string commandText, CommandType cType)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = commandText;
            cmd.CommandType = cType;
            cmd.CommandTimeout = TimeoutSeconds;
            return cmd;
        }

        private void AddParameters(DbCommand cmd, params string[] parameters)
        {
            if (parameters.Length <= 1) return;
            for (var i = 0; i < parameters.Length; i = i + 2)
            {
                var p = cmd.CreateParameter();
                p.ParameterName = "@" + (Connection == ConnectionType.MySql ? "_" : "") + parameters[i];
                if (null == parameters[i + 1])
                {
                    p.Value = DBNull.Value;
                }
                else
                {
                    p.Value = parameters[i + 1];
                }
                cmd.Parameters.Add(p);
            }
        }
    }
}
