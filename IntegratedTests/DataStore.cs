using System;
using System.Collections.Generic;
using System.Text;

namespace IntegratedTests
{
    public class DataStore
    {
        public DataStore()
        {

        }

        public static string connecionString =>
            @"Data Source=MARTINL\MARTIN_LOCAL;Initial Catalog=TestingDb;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
    }
}
