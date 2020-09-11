using NUnit.Framework;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using Dapper;
using QueryHelper;

namespace IntegratedTests
{
    public class Tests
    {
        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private SearchModel _searchModel;

        [SetUp]
        public void Setup()
        {
            _connection = CreateConnection();
            _transaction = _connection.BeginTransaction();
            _searchModel = new SearchModel() { Name = "test" };
        }

        /// <summary>
        /// Generate new connection based on connection string.
        /// </summary>
        /// <returns>.</returns>
        private SqlConnection SqlConnection()
        {
            return new SqlConnection(DataStore.connecionString);
        }

        /// <summary>
        /// Open new connection and return it for use.
        /// </summary>
        /// <returns>.</returns>
        private IDbConnection CreateConnection()
        {
            var conn = SqlConnection();
            conn.Open();
            return conn;
        }

        [Test]
        public void Connection_Is_Opened()
        {
            Assert.IsNotNull(_connection);
        }

        [Test]
        public void Get_Test_User_Is_Not_Null()
        {
            var select = typeof(TestUser).Select<TestUser>();
            var users =  _connection.Query(select.Query, null, _transaction);
            Assert.IsNotNull(users);
        }

        [Test]
        public void SuccessFull_Insert_Statement_Returns_1()
        {
            var insert = typeof(TestUser).Insert(new TestUser() {Name = "Test"});
            var result = _connection.Execute(insert.Query, insert.Model, _transaction);
            Assert.IsNotNull(result);
            Assert.AreEqual(result, 1);
        }

        [Test]
        public void InsertOn_Entity_Is_SuccessFull()
        {
            var user = new TestUser(){Name = "test"};
            var st = user.Insert();
            Assert.IsNotNull(st);
            Assert.IsNotNull(st.Query);
            Assert.IsNotNull(st.Model);
            var result = _connection.Execute(st.Query, st.Model, _transaction);
            Assert.IsNotNull(result);
            Assert.AreEqual(result,1);
        }

        [Test]
        public void UpdateOnEntity_Returns_1()
        {

            var userStatement = new TestUser().Select();
            var user = _connection.QueryFirst<TestUser>(userStatement.Query, null, _transaction);          
            var statement = user.Update().Where(x => x.Id);
            Assert.NotNull(statement);
            Assert.IsNotNull(statement.Model);
            Assert.IsNotNull(statement.Query);
            Assert.That(statement.Query.Contains("UPDATE"));
            var result =  _connection.Execute(statement.Query, statement.Model, _transaction);
            Assert.IsNotNull(result);
            Assert.AreEqual(result,1);
        }

        [Test]
        public void WhereLike_Is_Not_Null()
        {
         
            var userSt = new TestUser().Select(_searchModel).WhereLike<SearchModel>(x => x.Name, x => x.Name);
            var result = _connection.Query <TestUser>(userSt.Query, userSt.Model, _transaction).ToList();
            Assert.IsNotNull(result);
        }

        [Test]
        public void WhereLike_On_Type_Is_notNull()
        {
            var st = typeof(TestUser).Select<TestUser>(_searchModel).WhereLike<SearchModel>(x => x.Name, x => x.Name);
            var res = _connection.Query(st.Query, st.Model, _transaction).ToList();
            Assert.IsNotNull(res);
        }


        [Test]
        public void Join_Is_Not_Null()
        {
            _searchModel.UserId = 4;
            var selectStatements = Dslinq.GetSelectValues(typeof(TestUser)).ToList()
                .Concat(Dslinq.GetSelectValues(typeof(TestRelationship)));
            var st = new TestUser()
                .Join<TestUser, TestRelationship>(selectStatements, x => x.Id, x => x.UserId, _searchModel)
                .Where<SearchModel>(x => x.UserId, x => x.UserId).And(x=>x.Name,x=>x.Name).Or(x=>x.Name,"T");
            var res = _connection.Query<JoinedClass>(st.Query, st.Model, _transaction);
            Assert.IsNotNull(res);
        }

        [TearDown]
        public void TearDown()
        {
            _transaction.Commit();
            _connection.Close();
        }
    }
}